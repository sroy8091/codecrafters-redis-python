import asyncio
import argparse

from app.memory import RedisStore
from app.server import ServerHandler

async def main(storage):
    handler = ServerHandler(storage)
    server = await asyncio.start_server(handler.respond, "localhost", 6379)
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
    storage = RedisStore(args.dir, args.dbfilename)

    asyncio.run(main(storage))
