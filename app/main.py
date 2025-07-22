import asyncio
import argparse

from app.data.memory import RedisStore
from app.data.metadata import ServerMetadata
from app.server.handshake import handshake
from app.server.handler import ServerHandler
# No direct import from app.commands, but update if needed in future.


async def main(storage, port):
    handler = ServerHandler(storage)
    server = await asyncio.start_server(handler.respond, "localhost", port)
    addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
    print(f'Serving on {addrs}')

    async with server:
        asyncio.create_task(handshake(storage.metadata, port, storage))
        await server.serve_forever()


if __name__ == "__main__":
    # add args
    parser = argparse.ArgumentParser(description='Redis server implementation')
    parser.add_argument('--dir', type=str, help='the path to the directory where the RDB file is stored')
    parser.add_argument('--dbfilename', type=str, help='the name of the RDB file')
    parser.add_argument('--port', type=int, default=6379, help='the port to run the server on')
    parser.add_argument('--host', type=str, default='localhost', help='the host to run the server on')
    parser.add_argument('--replicaof', type=str, default=None, help='the host and port to replicate from')

    args = parser.parse_args()
    metadata = ServerMetadata(args.replicaof, "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", 0)
    storage = RedisStore(args.dir, args.dbfilename, metadata)

    asyncio.run(main(storage, args.port))
