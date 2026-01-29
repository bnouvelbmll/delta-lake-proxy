import asyncio
import logging
import time
import ssl
import subprocess
import os
from aiohttp import ClientSession

# --- CONFIGURATION ---
PROXY_PORT = 28080
LOG_LEVEL = logging.INFO
CA_CERT = "proxy_ca.crt"
CA_KEY = "proxy_ca.key"

# --- COLORS ---
class Colors:
    RESET = "\033[0m"
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN = "\033[96m"
    WHITE = "\033[97m"

logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("Proxy")

def ensure_ca():
    if not os.path.exists(CA_CERT) or not os.path.exists(CA_KEY):
        logger.info("Generating new MITM CA certificates...")
        # This requires openssl to be installed
        try:
            subprocess.run([
                "openssl", "req", "-x509", "-newkey", "rsa:2048",
                "-keyout", CA_KEY, "-out", CA_CERT,
                "-days", "365", "-nodes",
                "-subj", "/CN=Spark-Proxy-CA"
            ], check=True, capture_output=True)
        except Exception as e:
            logger.error(f"Failed to generate CA certs: {e}")

def log_request(method, url, status, duration_ms, is_connect=False):
    color = Colors.GREEN
    if status >= 400: color = Colors.YELLOW
    if status >= 500: color = Colors.RED
    
    method_color = Colors.MAGENTA if is_connect else Colors.BLUE
    
    logger.info(
        f"{method_color}{method:<7}{Colors.RESET} "
        f"{url:<50} "
        f"{color}{status}{Colors.RESET} "
        f"({duration_ms:.2f}ms)"
    )

async def ensure_headers(reader):
    """Reads from reader until double newline is found, ensuring full headers."""
    data = b""
    while True:
        if b'\r\n\r\n' in data or b'\n\n' in data:
            return data
        try:
            chunk = await reader.read(8192)
            if not chunk:
                return data
            data += chunk
        except Exception:
            return data

async def handle_connect(reader, writer, first_line):
    """Handles HTTPS CONNECT tunneling with MITM."""
    start_time = time.time()
    target = first_line.split(' ')[1]
    try:
        host, port = target.split(':')
        port = int(port)
    except ValueError:
        logger.error(f"Invalid CONNECT target: {target}")
        writer.close()
        return

    try:
        # 1. Send 200 Connection Established
        writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        await writer.drain()
        
        # 2. Upgrade to SSL (MITM)
        ssl_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_ctx.load_cert_chain(CA_CERT, CA_KEY)
        
        # This requires Python 3.11+
        new_reader = await writer.start_tls(ssl_ctx)
        
        # 3. Read the encrypted request (now decrypted)
        initial_data = await new_reader.read(8192)
        if not initial_data:
            return

        # 4. Handle as standard HTTP request, but upstream is HTTPS
        await handle_http(new_reader, writer, initial_data, scheme="https", target_host=host)
        
    except Exception as e:
        logger.error(f"MITM CONNECT error to {target}: {e}")
        writer.close()
    finally:
        duration = (time.time() - start_time) * 1000
        log_request("CONNECT", target, 200, duration, is_connect=True)
        # writer.close() is handled in handle_http usually, but if we crash here:
        if not writer.is_closing():
            writer.close()

async def handle_http(reader, writer, initial_data, scheme="http", target_host=None):
    """Handles standard HTTP requests using aiohttp for upstream fetching."""
    start_time = time.time()
    
    # 1. Parse Request
    header_bytes = initial_data
    while b'\r\n\r\n' not in header_bytes and b'\n\n' not in header_bytes:
        chunk = await reader.read(8192)
        if not chunk: break
        header_bytes += chunk
    
    if b'\r\n\r\n' in header_bytes:
        sep = b'\r\n\r\n'
    elif b'\n\n' in header_bytes:
        sep = b'\n\n'
    else:
        sep = b''
        
    if sep:
        head_part, body_part = header_bytes.split(sep, 1)
    else:
        head_part = header_bytes
        body_part = b""

    try:
        header_text = head_part.decode(errors='ignore')
        lines = header_text.splitlines()
        if not lines: return
        
        req_line = lines[0]
        method, url, _ = req_line.split(' ', 2)
        
        # Parse headers (Case-Insensitive Keys)
        headers = {}
        for line in lines[1:]:
            if ':' in line:
                k, v = line.split(':', 1)
                headers[k.strip().lower()] = v.strip()

        # 2. Identify Presigned & Strip Auth
        # Check both URL and headers for signs of presigning
        is_presigned = "Signature=" in url or "X-Amz-Signature=" in url
        
        if is_presigned:
            if 'authorization' in headers:
                del headers['authorization']
                # logger.info(f"{Colors.CYAN}Stripped Auth for presigned URL{Colors.RESET}")

        # 3. Prepare Upstream Request
        # Remove hop-by-hop headers
        for h in ['proxy-connection', 'connection', 'keep-alive', 'transfer-encoding', 'host']:
            if h in headers:
                del headers[h]

        # Construct Full URL
        if url.startswith('/'):
            # Relative path, need host
            host = headers.get('host', target_host)
            if not host:
                logger.error("No Host header and no target_host")
                return
            url = f"{scheme}://{host}{url}"
        else:
            # Absolute URL (e.g. http://host/path)
            # If scheme is different, we might need to adjust, but usually it matches
            pass

        async def request_body_stream():
            if body_part:
                yield body_part
            while True:
                chunk = await reader.read(8192)
                if not chunk: break
                yield chunk

        # 4. Execute Request via aiohttp
        async with ClientSession(auto_decompress=False) as session:
            async with session.request(
                method=method,
                url=url,
                headers=headers,
                data=request_body_stream() if method in ['PUT', 'POST'] else None,
                allow_redirects=False
            ) as resp:
                
                # 5. Send Response to Client
                reason = resp.reason if resp.reason else "OK"
                status_line = f"HTTP/1.1 {resp.status} {reason}\r\n"
                writer.write(status_line.encode())
                
                for k, v in resp.headers.items():
                    if k.lower() not in ['connection', 'transfer-encoding', 'content-encoding', 'keep-alive', 'proxy-connection']:
                        writer.write(f"{k}: {v}\r\n".encode())
                
                if 'Content-Encoding' in resp.headers:
                    writer.write(f"Content-Encoding: {resp.headers['Content-Encoding']}\r\n".encode())

                writer.write(b"Connection: close\r\n")
                writer.write(b"\r\n")
                await writer.drain()
                
                try:
                    async for chunk in resp.content.iter_chunked(8192):
                        writer.write(chunk)
                        await writer.drain()
                except Exception as e:
                    logger.error(f"Error streaming response body: {e}")
                    raise e
                
                duration = (time.time() - start_time) * 1000
                log_request(method, url, resp.status, duration)

    except Exception as e:
        logger.error(f"HTTP Proxy Error: {e}")
    finally:
        writer.close()

async def handle_client(reader, writer):
    try:
        initial_data = await reader.read(8192)
        if not initial_data:
            writer.close()
            return

        first_line_end = initial_data.find(b'\n')
        if first_line_end != -1:
            first_line = initial_data[:first_line_end].decode(errors='ignore').strip()
            if first_line.startswith('CONNECT'):
                await handle_connect(reader, writer, first_line)
            else:
                await handle_http(reader, writer, initial_data)
        else:
            writer.close()
            
    except Exception as e:
        logger.error(f"Client Error: {e}")
        writer.close()

async def main():
    ensure_ca()
    server = await asyncio.start_server(handle_client, '0.0.0.0', PROXY_PORT)
    print(f"{Colors.GREEN}Async Proxy Active on port {PROXY_PORT}{Colors.RESET}")
    print(f"{Colors.CYAN}Features: MITM SSL, Presigned Auth Stripping, 206/Binary Safe{Colors.RESET}")
    
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
