import asyncio

from app.data.metadata import ServerMetadata
from app.resp.RESPCodec import RESPEncoder


async def handshake(metadata: ServerMetadata):
    if metadata.role != "slave":
        return

    host, port = metadata.replicaof.split(" ")
    try:
        reader, writer = await asyncio.open_connection(host, port)
        encoder = RESPEncoder()
        # Send PING command as RESP array: *1\r\n$4\r\nPING\r\n
        writer.write(encoder.encode(["PING"]))
        await writer.drain()
        response = await reader.read(1024)
        print(f"Replication setup: {response}")
    except Exception as e:
        print("Unable to connect to master")