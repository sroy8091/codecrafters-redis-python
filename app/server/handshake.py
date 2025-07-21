import asyncio

from app.data.metadata import ServerMetadata
from app.resp.RESPCodec import RESPEncoder


async def handshake(metadata: ServerMetadata, port: int):
    if metadata.role != "slave":
        return

    host, master_port = metadata.replicaof.split(" ")
    try:
        reader, writer = await asyncio.open_connection(host, int(master_port))
        encoder = RESPEncoder()
        await send_command(reader, writer, encoder, ["PING"])
        await send_command(reader, writer, encoder, ["replconf", "listening-port", str(port)])
        await send_command(reader, writer, encoder, ["REPLCONF", "capa", "psync2"])
        await send_command(reader, writer, encoder, ["PSYNC", "?", "-1"])
    except Exception as e:
        print("Unable to connect to master")


async def send_command(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, encoder: RESPEncoder, command: list[str]):
    writer.write(encoder.encode(command))
    await writer.drain()
    response = await reader.read(1024)
    return response