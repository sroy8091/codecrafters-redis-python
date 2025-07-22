from app.action import RedisAction
import types


class ServerHandler:
    def __init__(self, storage):
        self.storage = storage
        self.action = RedisAction(self.storage)

    async def respond(self, reader, writer):
        is_replica_connection = False
        try:
            while True:
                data = await reader.read(1024)
                print(f"[{self.storage.metadata.role}] Received: {data}")

                if not data:
                    print(f"[{self.storage.metadata.role}] Client disconnected")
                    break

                try:
                    result = await self.action.handle_input(data, writer)
                    # Handle async generator (streaming)
                    if isinstance(result, types.AsyncGeneratorType):
                        async for chunk in result:
                            print(f"[{self.storage.metadata.role}] Sending chunk from async generator (possibly PSYNC/RDB)")
                            writer.write(chunk)
                            await writer.drain()
                        print(f"[{self.storage.metadata.role}] Finished sending all chunks (PSYNC/RDB or streaming command)")
                        
                        # Check if this was a PSYNC command - if so, keep connection open
                        if b'PSYNC' in data:
                            is_replica_connection = True
                            print(f"[{self.storage.metadata.role}] PSYNC completed - keeping connection open for replica")
                    else:
                        writer.write(result)
                        await writer.drain()
                        print(f"[{self.storage.metadata.role}] Sent response for command")
                except Exception as e:
                    print(f"[{self.storage.metadata.role}] Exception while handling command: {e}")
                    import traceback
                    traceback.print_exc()
        finally:
            if not is_replica_connection:
                print(f"[{self.storage.metadata.role}] Closing writer and waiting for it to close...")
                writer.close()
                await writer.wait_closed()
                print(f"[{self.storage.metadata.role}] Writer closed.")
            else:
                print(f"[{self.storage.metadata.role}] Keeping replica connection open")
                # Remove from replica_writers list if connection fails
                if writer in self.storage.metadata.replica_writers:
                    self.storage.metadata.replica_writers.remove(writer)