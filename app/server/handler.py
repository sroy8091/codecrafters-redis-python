from app.action import RedisAction
import types


class ServerHandler:
    def __init__(self, storage):
        self.storage = storage
        self.action = RedisAction(self.storage)

    async def respond(self, reader, writer):
        try:
            while True:
                data = await reader.read(1024)
                print(f"Received: {data}")

                if not data:
                    print("Client disconnected")
                    break

                result = self.action.handle_input(data)
                # Handle async generator (streaming)
                if isinstance(result, types.AsyncGeneratorType):
                    async for chunk in result:
                        writer.write(chunk)
                        await writer.drain()
                else:
                    writer.write(result)
                    await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()