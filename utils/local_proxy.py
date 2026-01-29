import asyncio
import ssl
import logging
import subprocess
import time
from pathlib import Path
from yarl import URL  # Ensure 'pip install yarl'

# --- CONFIGURATION ---
PORT = 28080
CA_CERT = "proxy_ca.crt"
CA_KEY = "proxy_ca.key"

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("DualProxy")

def ensure_ca():
    if not Path(CA_CERT).exists():
        logger.info("Generating new MITM CA certificates...")
        subprocess.run([
            "openssl", "req", "-x509", "-newkey", "rsa:2048",
            "-keyout", CA_KEY, "-out", CA_CERT,
            "-days", "365", "-nodes",
            "-subj", "/CN=Spark-Proxy-CA"
        ], check=True, capture_output=True)

def modify_request(request_data, host_label):
    try:
        # Detect separator
        sep = b'\r\n\r\n'
        if sep not in request_data:
            sep = b'\n\n'
            if sep not in request_data:
                # Could be just headers?
                sep = b''
        
        if sep:
            parts = request_data.split(sep, 1)
            header_bytes = parts[0]
            body_part = parts[1]
        else:
            header_bytes = request_data
            body_part = b""

        header_text = header_bytes.decode(errors='ignore')
        
        # Detect line ending
        line_sep = '\r\n' if '\r\n' in header_text else '\n'
        lines = header_text.split(line_sep)
        
        # Check presigned ONLY in the Request Line (first line)
        # The Authorization header itself contains "Signature=", so checking the whole block is a bug.
        first_line = lines[0] if lines else ""
        is_presigned = any(k in first_line for k in ["X-Amz-Signature=", "Signature="])
        
        new_lines = []
        for line in lines:
            lower_line = line.lower()
            if is_presigned and lower_line.startswith("authorization:"):
                logger.info(f"STRIP: Removed Auth header for {host_label}")
                continue
            if lower_line.startswith("connection:"):
                continue
            if lower_line.startswith("proxy-connection:"):
                continue
            if lower_line.strip() == "":
                continue
            new_lines.append(line)
            
        new_lines.append("Connection: close")
        
        new_header_text = line_sep.join(new_lines)
        # logger.debug(f"Modified Headers:\n{new_header_text}")
        
        return new_header_text.encode() + (sep if sep else b'\r\n\r\n') + body_part
    except Exception as e:
        logger.error(f"Error modifying request: {e}")
        return request_data

async def pipe_stream(reader, writer):
    """Pipes data from reader to writer until reader is closed."""
    try:
        while True:
            data = await reader.read(8192)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except Exception:
        pass

async def handle_standard_http(reader, writer, initial_data):
    start_time = time.time()
    method = "UNKNOWN"
    url_str = "UNKNOWN"
    status_code = 0
    dest_writer = None
    req_task = None
    
    try:
        request_text = initial_data.decode(errors='ignore')
        lines = request_text.split('\n') # Simple split for first line
        first_line = lines[0].strip()
        
        # Parse Method and URL
        parts = first_line.split(' ')
        if len(parts) >= 2:
            method = parts[0]
            raw_url = parts[1]
            url_str = raw_url
            parsed = URL(raw_url)
            host = parsed.host
            port = parsed.port
        else:
            return

        # Fallback: Parse from Host Header
        if not host:
            for line in lines:
                if line.lower().startswith("host:"):
                    host_val = line.split(':', 1)[1].strip()
                    if ':' in host_val:
                        host, port_str = host_val.split(':')
                        port = int(port_str)
                    else:
                        host = host_val
                        port = 80
                    break

        if not host:
            logger.error("Could not determine target host.")
            return

        payload = modify_request(initial_data, f"{host}:{port}")
        
        # Establish connection
        dest_reader, dest_writer = await asyncio.open_connection(host, port)
        dest_writer.write(payload)
        await dest_writer.drain()

        # Bidirectional streaming
        req_task = asyncio.create_task(pipe_stream(reader, dest_writer))
        
        # Read response
        first_chunk = True
        while True:
            chunk = await dest_reader.read(8192)
            if not chunk: break
            
            if first_chunk:
                try:
                    response_text = chunk.decode(errors='ignore')
                    response_lines = response_text.split('\n')
                    if len(response_lines) > 0:
                        status_line = response_lines[0].strip()
                        status_parts = status_line.split(' ')
                        if len(status_parts) >= 2:
                            status_code = int(status_parts[1])
                except:
                    pass
                first_chunk = False

            writer.write(chunk)
            await writer.drain()
            
    except Exception as e:
        logger.error(f"HTTP Error: {e}")
        status_code = 500
    finally:
        if req_task:
            req_task.cancel()
            try:
                await req_task
            except asyncio.CancelledError:
                pass
        
        if dest_writer:
            dest_writer.close()
            # await dest_writer.wait_closed() # Optional, but good practice
        
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
            
        latency = (time.time() - start_time) * 1000
        logger.info(f"REQ: {method} {url_str} -> {status_code} ({latency:.2f}ms)")

async def handle_client(reader, writer):
    req_task = None
    try:
        initial_data = await reader.read(8192)
        if not initial_data: return

        if initial_data.startswith(b"CONNECT"):
            # HTTPS logic
            first_line = initial_data.decode().split('\n')[0].strip()
            target = first_line.split(' ')[1]
            host, port = target.split(':')
            
            writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            await writer.drain()

            ssl_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            ssl_ctx.load_cert_chain(CA_CERT, CA_KEY)
            await writer.start_tls(ssl_ctx)

            inner_data = await reader.read(8192)
            payload = modify_request(inner_data, target)

            d_reader, d_writer = await asyncio.open_connection(host, int(port), ssl=True)
            d_writer.write(payload)
            await d_writer.drain()

            # Bidirectional streaming for HTTPS
            req_task = asyncio.create_task(pipe_stream(reader, d_writer))
            
            while True:
                chunk = await d_reader.read(8192)
                if not chunk: break
                writer.write(chunk)
                await writer.drain()
            
            d_writer.close()
        else:
            await handle_standard_http(reader, writer, initial_data)
    except Exception as e:
        logger.debug(f"Connection handled: {e}")
    finally:
        if req_task:
            req_task.cancel()
            try:
                await req_task
            except asyncio.CancelledError:
                pass
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

async def main():
    ensure_ca()
    server = await asyncio.start_server(handle_client, '0.0.0.0', PORT)
    logger.info(f"Proxy listening on port {PORT} (Supports custom ports)")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())