import asyncio
import logging
import time
import ssl
from urllib.parse import urlparse
from aiohttp import ClientSession, TCPConnector

# --- CONFIGURATION ---
PROXY_PORT = 28080
LOG_LEVEL = logging.INFO

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

def parse_headers(header_text):
    headers = {}
    lines = header_text.split('\n')
    for line in lines[1:]: # Skip request line
        if ':' in line:
            key, value = line.split(':', 1)
            headers[key.strip()] = value.strip()
    return headers

async def handle_connect(reader, writer, first_line):
    """Handles HTTPS CONNECT tunneling."""
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
        # Connect to upstream
        upstream_reader, upstream_writer = await asyncio.open_connection(host, port)
        
        # Send 200 Connection Established
        writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        await writer.drain()
        
        # Pipe data
        await asyncio.gather(
            pipe_stream(reader, upstream_writer),
            pipe_stream(upstream_reader, writer)
        )
        
        upstream_writer.close()
    except Exception as e:
        logger.error(f"CONNECT error to {target}: {e}")
    finally:
        duration = (time.time() - start_time) * 1000
        log_request("CONNECT", target, 200, duration, is_connect=True)
        writer.close()

async def handle_http(reader, writer, initial_data):
    """Handles standard HTTP requests using aiohttp for upstream fetching."""
    start_time = time.time()
    
    # 1. Parse Request
    # full_data = await ensure_headers(reader) # REMOVED: This was consuming data incorrectly
    
    # Correct logic: We have initial_data. We check if it has headers. If not, read more.
    header_bytes = initial_data
    while b'\r\n\r\n' not in header_bytes and b'\n\n' not in header_bytes:
        chunk = await reader.read(8192)
        if not chunk: break
        header_bytes += chunk
    
    # Split headers and body (if any)
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
                headers[k.strip().lower()] = v.strip() # Lowercase keys

        # 2. Identify Presigned & Strip Auth
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

        # If URL is just path, try to find Host
        if url.startswith('/'):
            if 'host' in headers:
                url = f"http://{headers['host']}{url}"
            else:
                # Fallback or error
                pass

        # Read remaining body if Content-Length indicates so
        # For simplicity in this proxy, we might just read what's available or rely on aiohttp to stream?
        # But we need to provide data to aiohttp.
        # If there is a body, we should read it.
        # For now, let's assume small bodies or just what we have. 
        # Ideally we should stream the body from `reader` to `aiohttp`.
        
        async def request_body_stream():
            if body_part:
                yield body_part
            while True:
                chunk = await reader.read(8192)
                if not chunk: break
                yield chunk

        # 4. Execute Request via aiohttp
        # auto_decompress=False is CRITICAL for 206 ranges and binary integrity
        async with ClientSession(auto_decompress=False) as session:
            async with session.request(
                method=method,
                url=url,
                headers=headers,
                data=request_body_stream() if method in ['PUT', 'POST'] else None,
                allow_redirects=False
            ) as resp:
                
                # 5. Send Response to Client
                # Status Line
                status_line = f"HTTP/1.1 {resp.status} {resp.reason}\r\n"
                writer.write(status_line.encode())
                
                # Headers
                # Filter hop-by-hop
                for k, v in resp.headers.items():
                    if k.lower() not in ['connection', 'transfer-encoding', 'content-encoding']:
                        writer.write(f"{k}: {v}\r\n".encode())
                
                # Explicitly handle Content-Encoding
                # Since auto_decompress=False, we forward the raw bytes.
                # We MUST forward the Content-Encoding header if present so the client knows to decompress.
                if 'Content-Encoding' in resp.headers:
                    writer.write(f"Content-Encoding: {resp.headers['Content-Encoding']}\r\n".encode())

                writer.write(b"\r\n")
                await writer.drain()
                
                # Body
                async for chunk in resp.content.iter_chunked(8192):
                    writer.write(chunk)
                    await writer.drain()
                
                duration = (time.time() - start_time) * 1000
                log_request(method, url, resp.status, duration)

    except Exception as e:
        logger.error(f"HTTP Proxy Error: {e}")
        try:
            writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
            await writer.drain()
        except:
            pass
    finally:
        writer.close()

async def handle_client(reader, writer):
    try:
        # Peek at the first chunk to determine protocol
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
            # Fallback or malformed
            writer.close()
            
    except Exception as e:
        logger.error(f"Client Error: {e}")
        writer.close()

async def main():
    server = await asyncio.start_server(handle_client, '0.0.0.0', PROXY_PORT)
    print(f"{Colors.GREEN}Async Proxy Active on port {PROXY_PORT}{Colors.RESET}")
    print(f"{Colors.CYAN}Features: CONNECT support, Presigned Auth Stripping, 206/Binary Safe{Colors.RESET}")
    
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass