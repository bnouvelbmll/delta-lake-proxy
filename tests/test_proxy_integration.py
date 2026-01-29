import asyncio
import aiohttp
import pytest
import subprocess
import time
import sys
import os
from aiohttp import web
import threading

# Configuration
PROXY_PORT = 28081
MOCK_SERVER_PORT = 28082
PROXY_SCRIPT = "utils/local_proxy.py"

# --- Mock Upstream Server ---
async def mock_handler(request):
    if request.path == "/simple":
        return web.Response(text="Simple Response")
    
    if request.path == "/presigned":
        # Check if Authorization header was stripped
        if "Authorization" in request.headers:
            return web.Response(status=400, text="Auth header present!")
        return web.Response(text="Presigned OK")
    
    if request.path == "/binary":
        # Return some binary data
        data = b'\x00\x01\x02\x03\xff\xfe'
        return web.Response(body=data, content_type='application/octet-stream')
    
    if request.path == "/range":
        # Simple range handling mock
        range_header = request.headers.get("Range")
        full_content = b"0123456789"
        if range_header:
            try:
                # bytes=0-4
                unit, ranges = range_header.split('=')
                start, end = ranges.split('-')
                start = int(start)
                end = int(end)
                chunk = full_content[start:end+1]
                resp = web.Response(body=chunk, status=206)
                resp.headers['Content-Range'] = f"bytes {start}-{end}/{len(full_content)}"
                return resp
            except:
                return web.Response(status=400)
        return web.Response(body=full_content)

    return web.Response(status=404)

def run_mock_server(loop):
    asyncio.set_event_loop(loop)
    app = web.Application()
    app.router.add_get('/simple', mock_handler)
    app.router.add_get('/presigned', mock_handler)
    app.router.add_get('/binary', mock_handler)
    app.router.add_get('/range', mock_handler)
    runner = web.AppRunner(app)
    loop.run_until_complete(runner.setup())
    site = web.TCPSite(runner, 'localhost', MOCK_SERVER_PORT)
    loop.run_until_complete(site.start())
    loop.run_forever()

@pytest.fixture(scope="module")
def proxy_server():
    with open(PROXY_SCRIPT, 'r') as f:
        content = f.read()
    
    test_script = "utils/local_proxy_test.py"
    with open(test_script, 'w') as f:
        f.write(content.replace("PROXY_PORT = 28080", f"PROXY_PORT = {PROXY_PORT}"))
        
    proc = subprocess.Popen([sys.executable, test_script], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(2) # Wait for startup
    
    yield proc
    
    proc.terminate()
    proc.wait()
    if os.path.exists(test_script):
        os.remove(test_script)

@pytest.fixture(scope="module")
def mock_upstream():
    loop = asyncio.new_event_loop()
    t = threading.Thread(target=run_mock_server, args=(loop,), daemon=True)
    t.start()
    time.sleep(1) # Wait for startup
    yield
    # No clean shutdown for daemon thread in this simple test setup

# --- Tests ---

def test_simple_proxy(proxy_server, mock_upstream):
    async def run():
        async with aiohttp.ClientSession() as session:
            proxy_url = f"http://localhost:{PROXY_PORT}"
            target_url = f"http://localhost:{MOCK_SERVER_PORT}/simple"
            
            async with session.get(target_url, proxy=proxy_url) as resp:
                assert resp.status == 200
                text = await resp.text()
                assert text == "Simple Response"
    asyncio.run(run())

def test_presigned_auth_strip(proxy_server, mock_upstream):
    async def run():
        async with aiohttp.ClientSession() as session:
            proxy_url = f"http://localhost:{PROXY_PORT}"
            target_url = f"http://localhost:{MOCK_SERVER_PORT}/presigned?Signature=123"
            
            headers = {"Authorization": "Bearer secret"}
            async with session.get(target_url, proxy=proxy_url, headers=headers) as resp:
                assert resp.status == 200
                text = await resp.text()
                assert text == "Presigned OK"
    asyncio.run(run())

def test_binary_content(proxy_server, mock_upstream):
    async def run():
        async with aiohttp.ClientSession() as session:
            proxy_url = f"http://localhost:{PROXY_PORT}"
            target_url = f"http://localhost:{MOCK_SERVER_PORT}/binary"
            
            async with session.get(target_url, proxy=proxy_url) as resp:
                assert resp.status == 200
                data = await resp.read()
                assert data == b'\x00\x01\x02\x03\xff\xfe'
    asyncio.run(run())

def test_range_request(proxy_server, mock_upstream):
    async def run():
        async with aiohttp.ClientSession() as session:
            proxy_url = f"http://localhost:{PROXY_PORT}"
            target_url = f"http://localhost:{MOCK_SERVER_PORT}/range"
            
            headers = {"Range": "bytes=0-4"}
            async with session.get(target_url, proxy=proxy_url, headers=headers) as resp:
                assert resp.status == 206
                data = await resp.read()
                assert data == b"01234"
                assert resp.headers['Content-Range'] == "bytes 0-4/10"
    asyncio.run(run())

def test_connect_tunnel(proxy_server, mock_upstream):
    async def run():
        reader, writer = await asyncio.open_connection('localhost', PROXY_PORT)
        
        connect_req = f"CONNECT localhost:{MOCK_SERVER_PORT} HTTP/1.1\r\nHost: localhost:{MOCK_SERVER_PORT}\r\n\r\n"
        writer.write(connect_req.encode())
        await writer.drain()
        
        # Read response
        line = await reader.readline()
        assert b"200 Connection Established" in line
        
        # Consume empty line
        while True:
            line = await reader.readline()
            if line == b'\r\n': break
        
        # Now we are tunneled. Send a raw HTTP request through the tunnel.
        req = "GET /simple HTTP/1.1\r\nHost: localhost\r\n\r\n"
        writer.write(req.encode())
        await writer.drain()
        
        # Read response from upstream via tunnel
        response = await reader.read(1024)
        assert b"Simple Response" in response
        
        writer.close()
        await writer.wait_closed()
    asyncio.run(run())