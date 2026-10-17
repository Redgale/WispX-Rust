"""HTTP origin that answers every request with a close-delimited body (no Content-Length).

GET /?n=BYTES -> 200 with BYTES bytes of 'x', then the connection is closed.
"""
import asyncio, sys, urllib.parse

async def handle(reader, writer):
    try:
        head = await reader.readuntil(b"\r\n\r\n")
        path = head.split(b" ", 2)[1].decode()
        n = int(urllib.parse.parse_qs(urllib.parse.urlparse(path).query).get("n", ["16384"])[0])
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n")
        writer.write(b"x" * n)
        await writer.drain()
    except Exception:
        pass
    finally:
        writer.close()

async def main(port, tls_port):
    import ssl, os
    here = os.environ.get("CERT_DIR", os.path.dirname(os.path.abspath(__file__)))
    ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ctx.load_cert_chain(os.path.join(here, "cert.pem"), os.path.join(here, "key.pem"))
    plain = await asyncio.start_server(handle, "127.0.0.1", port, backlog=1024)
    tls = await asyncio.start_server(handle, "127.0.0.1", tls_port, ssl=ctx, backlog=1024)
    async with plain, tls:
        await asyncio.gather(plain.serve_forever(), tls.serve_forever())

asyncio.run(main(int(sys.argv[1]), int(sys.argv[2])))
