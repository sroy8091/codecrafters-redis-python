from app.commands import RedisAction


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

                writer.write(self.action.handle_input(data))
                await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()