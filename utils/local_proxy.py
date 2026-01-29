import asyncio
from aiohttp import web, ClientSession, ClientResponse
from yarl import URL

async def proxy_handler(request: web.Request):
    # 1. Identify if this is a presigned URL (e.g., contains 'X-Amz-Signature')
    is_presigned = 'X-Amz-Signature' in request.query or 'Signature' in request.query
    
    # 2. Prepare headers: Copy original headers but strip 'Authorization' if presigned
    headers = dict(request.headers)
    if is_presigned:
        headers.pop('Authorization', None)
        print(f"Stripping Authorization for presigned URL: {request.path_qs}")

    # 3. Forward the request to the actual destination
    # Note: Spark might send absolute URLs to a proxy
    target_url = request.url
    
    async with ClientSession() as session:
        async with session.request(
            method=request.method,
            url=target_url,
            headers=headers,
            data=await request.read() if request.has_body else None,
            allow_redirects=False # Handling redirects manually if needed
        ) as resp:
            # 4. Stream the response back to the Spark client
            proxy_resp = web.StreamResponse(status=resp.status, headers=resp.headers)
            await proxy_resp.prepare(request)
            async for chunk in resp.content.iter_chunked(8192):
                await proxy_resp.write(chunk)
            return proxy_resp

async def main():
    app = web.Application()
    # Catch-all route to handle all proxy requests
    app.router.add_route('*', '/{tail:.*}', proxy_handler)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, 'localhost', 28080)
    print("Proxy server running on http://localhost:28080")
    await site.start()
    
    # Keep the server running
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
