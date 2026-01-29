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
        # Split headers and body
        parts = request_data.split(b'\r\n\r\n', 1)
        header_part = parts[0].decode(errors='ignore')
        body_part = parts[1] if len(parts) > 1 else b""
        
        is_presigned = any(k in header_part for k in ["X-Amz-Signature=", "Signature="])
        
        lines = header_part.split('\r\n')
        new_lines = []
        
        for line in lines:
            lower_line = line.lower()
            # Strip Authorization if presigned
            if is_presigned and lower_line.startswith("authorization:"):
                logger.info(f"STRIP: Removed Auth header for {host_label}")
                continue
            # Remove existing Connection header
            if lower_line.startswith("connection:"):
                continue
            # Remove Proxy-Connection header (sometimes sent by clients)
            if lower_line.startswith("proxy-connection:"):
                continue
            new_lines.append(line)
            
        # Force Connection: close to prevent hanging on Keep-Alive
        new_lines.append("Connection: close")
        
        new_header_part = '\r\n'.join(new_lines).encode()
        return new_header_part + b'\r\n\r\n' + body_part
    except Exception as e:
        logger.error(f"Error modifying request: {e}")
        return request_data

async def handle_standard_http(reader, writer, initial_data):
    start_time = time.time()
    method = "UNKNOWN"
    url_str = "UNKNOWN"
    status_code = 0
    dest_writer = None
    
    try:
        request_text = initial_data.decode(errors='ignore')
        lines = request_text.split('\r\n')
        first_line = lines[0]
        
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

        # 2. Fallback: Parse from Host Header if URL was relative
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

        # logger.info(f"HTTP: Forwarding to {host}:{port}")
        
        payload = modify_request(initial_data, f"{host}:{port}")
        
        # 3. Establish connection to the non-standard port
        dest_reader, dest_writer = await asyncio.open_connection(host, port)
        dest_writer.write(payload)
        await dest_writer.drain()

        # Read response to get status code
        first_chunk = True
        while True:
            chunk = await dest_reader.read(8192)
            if not chunk: break
            
            if first_chunk:
                try:
                    response_text = chunk.decode(errors='ignore')
                    response_lines = response_text.split('\r\n')
                    if len(response_lines) > 0:
                        status_line = response_lines[0]
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
        if dest_writer:
            dest_writer.close()
        # writer.close() is handled by caller
        latency = (time.time() - start_time) * 1000
        logger.info(f"REQ: {method} {url_str} -> {status_code} ({latency:.2f}ms)")

async def handle_client(reader, writer):
    try:
        initial_data = await reader.read(8192)
        if not initial_data: return

        if initial_data.startswith(b"CONNECT"):
            # HTTPS logic (simplified)
            first_line = initial_data.decode().split('\r\n')[0]
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
        writer.close()

async def main():
    ensure_ca()
    server = await asyncio.start_server(handle_client, '0.0.0.0', PORT)
    logger.info(f"Proxy listening on port {PORT} (Supports custom ports)")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())

