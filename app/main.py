import asyncio
import argparse

from app.commands import handle_input
from app.memory import set_memory_file


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
    # add args
    parser = argparse.ArgumentParser(description='Redis server implementation')
    parser.add_argument('--dir', type=str, help='the path to the directory where the RDB file is stored')
    parser.add_argument('--dbfilename', type=str, help='the name of the RDB file')
    args = parser.parse_args()
    set_memory_file(args)

    asyncio.run(main())
