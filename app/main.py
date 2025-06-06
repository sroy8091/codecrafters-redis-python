import asyncio

async def respond(reader, writer):
    try:
        while True:
            await reader.read(1024)
            writer.write(b"+PONG\r\n")
            await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()

async def main():
    server = await asyncio.start_server(respond, "localhost", 6379)
    addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
    print(f'Serving on {addrs}')

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
