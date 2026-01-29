import asyncio
import ssl
import logging
import subprocess
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

def strip_auth_if_presigned(request_text, host_label):
    is_presigned = any(k in request_text for k in ["X-Amz-Signature=", "Signature="])
    lines = request_text.split('\r\n')
    modified_lines = []
    for line in lines:
        if is_presigned and line.lower().startswith("authorization:"):
            logger.info(f"STRIP: Removed Auth header for {host_label}")
            continue
        modified_lines.append(line)
    return '\r\n'.join(modified_lines).encode()

async def handle_standard_http(reader, writer, initial_data):
    try:
        request_text = initial_data.decode(errors='ignore')
        lines = request_text.split('\r\n')
        first_line = lines[0]
        print(first_line)


        # 1. Parse URL from Request Line (e.g., GET http://host:18080/path HTTP/1.1)
        parts = first_line.split(' ')
        if len(parts) < 2: return
        raw_url = parts[1]
        parsed = URL(raw_url)
        
        host = parsed.host
        port = parsed.port

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

        logger.info(f"HTTP: Forwarding to {host}:{port}")
        
        payload = strip_auth_if_presigned(request_text, f"{host}:{port}")
        
        # 3. Establish connection to the non-standard port
        dest_reader, dest_writer = await asyncio.open_connection(host, port)
        dest_writer.write(payload)
        await dest_writer.drain()

        while True:
            chunk = await dest_reader.read(8192)
            if not chunk: break
            writer.write(chunk)
            await writer.drain()
    except Exception as e:
        logger.error(f"HTTP Error: {e}")
    finally:
        writer.close()

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
            payload = strip_auth_if_presigned(inner_data.decode(errors='ignore'), target)

            d_reader, d_writer = await asyncio.open_connection(host, int(port), ssl=True)
            d_writer.write(payload)
            await d_writer.drain()

            while True:
                chunk = await d_reader.read(8192)
                if not chunk: break
                writer.write(chunk)
                await writer.drain()
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

