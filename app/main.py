import asyncio

from app.commands import handle_input


async def respond(reader, writer):
    try:
        while True:
            data = await reader.read(1024)
            print(f"Received: {data}")
            writer.write(handle_input(data))
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
