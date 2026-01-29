import asyncio
import logging
from aiohttp import web, ClientSession, TCPConnector

# --- CONFIGURATION ---
ENABLE_LOGGING = True
PROXY_PORT = 28080
LOG_LEVEL = logging.INFO

if ENABLE_LOGGING:
    logging.basicConfig(
        level=LOG_LEVEL,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S'
    )
logger = logging.getLogger("ProxyLogger")

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

async def connect_handler(request: web.Request):
    """Handles the CONNECT method for HTTPS tunneling."""
    loop = asyncio.get_running_loop()
    host, port = request.path.split(':')
    port = int(port)

    if ENABLE_LOGGING:
        logger.info(f"CONNECT {host}:{port}")

    # Connect to upstream
    try:
        upstream_reader, upstream_writer = await asyncio.open_connection(host, port)
    except Exception as e:
        logger.error(f"Failed to connect to upstream {host}:{port}: {e}")
        return web.Response(status=502, text="Bad Gateway")

    # Send 200 Connection Established to the client
    # We use a StreamResponse to take control, but we need to be careful about headers.
    # CONNECT responses are very minimal.
    resp = web.StreamResponse(status=200, reason='Connection Established')
    await resp.prepare(request)

    # Hijack the client connection
    # In aiohttp, we can access the transport.
    # We need to be careful not to let aiohttp close it immediately or interfere.
    
    transport = request.transport
    
    # We can't easily get a reader/writer pair from the transport in aiohttp handler 
    # without some hacks, but we can read from the request content (which is the reader)
    # and write to the transport (which is the writer).
    
    # However, request.content is a StreamReader.
    
    # Create tasks to pipe data
    # Client -> Upstream
    client_reader = request.content
    
    # Upstream -> Client
    # We need to write to the client. `resp.write` writes to the client.
    
    async def upstream_to_client():
        try:
            while True:
                data = await upstream_reader.read(8192)
                if not data:
                    break
                await resp.write(data)
        except Exception:
            pass

    async def client_to_upstream():
        try:
            while True:
                # request.content.read() reads from the client
                data = await client_reader.read(8192)
                if not data:
                    break
                upstream_writer.write(data)
                await upstream_writer.drain()
        except Exception:
            pass

    # Run both pipes
    await asyncio.gather(upstream_to_client(), client_to_upstream())
    
    upstream_writer.close()
    return resp

async def proxy_handler(request: web.Request):
    if request.method == 'CONNECT':
        return await connect_handler(request)

    # 1. Identify query nature
    is_presigned = any(k in request.query for k in ['X-Amz-Signature', 'Signature'])
    
    if ENABLE_LOGGING:
        logger.info(f"REQ: {request.method} {request.url}")
        if is_presigned:
            logger.info("Detected Presigned URL - Authorization will be stripped.")

    # 2. Header manipulation
    headers = dict(request.headers)
    if is_presigned:
        headers.pop('Authorization', None)
    
    # Remove hop-by-hop headers
    headers.pop('Host', None)
    headers.pop('Connection', None)
    headers.pop('Proxy-Connection', None)
    headers.pop('Transfer-Encoding', None)
    headers.pop('Keep-Alive', None)

    # 3. Forward request
    try:
        # Disable auto_decompress to forward raw bytes (important for 206 and binary files)
        async with ClientSession(auto_decompress=False) as session:
            async with session.request(
                method=request.method,
                url=request.url,
                headers=headers,
                data=request.content if request.has_body else None,
                allow_redirects=False
            ) as resp:
                
                if ENABLE_LOGGING:
                    logger.info(f"RES: {resp.status} for {request.path}")

                # 4. Stream response back
                # Copy headers from upstream
                proxy_headers = dict(resp.headers)
                
                # Remove hop-by-hop headers from response
                proxy_headers.pop('Connection', None)
                proxy_headers.pop('Transfer-Encoding', None)
                proxy_headers.pop('Content-Encoding', None) # Let client handle decoding if we send raw bytes? 
                # Wait, if we disabled auto_decompress, we receive raw bytes (compressed or not).
                # We should forward Content-Encoding so client knows how to handle it.
                # But aiohttp might have stripped it? No, resp.headers has it.
                # However, if we use `auto_decompress=False`, `resp.content` yields raw body.
                # So we SHOULD forward Content-Encoding.
                
                # Re-add Content-Encoding if it was present in upstream response
                if 'Content-Encoding' in resp.headers:
                    proxy_headers['Content-Encoding'] = resp.headers['Content-Encoding']

                proxy_resp = web.StreamResponse(status=resp.status, headers=proxy_headers)
                await proxy_resp.prepare(request)
                
                async for chunk in resp.content.iter_chunked(8192):
                    await proxy_resp.write(chunk)
                
                return proxy_resp

    except Exception as e:
        if ENABLE_LOGGING:
            logger.error(f"Error forwarding request: {str(e)}")
        return web.Response(status=502, text=f"Proxy Error: {e}")

async def main():
    app = web.Application()
    # Catch-all route
    app.router.add_route('*', '/{tail:.*}', proxy_handler)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PROXY_PORT)
    print(f"Async Proxy Active on http://0.0.0.0:{PROXY_PORT}")
    await site.start()
    
    # Keep alive
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
