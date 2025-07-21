from enum import Enum
import types
from app.resp.RESPCodec import RESPDecoder, RESPEncoder
import os

class SimpleCommand(Enum):
    PING = "PING"
    ECHO = "ECHO"
    SET = "SET"
    GET = "GET"
    CONFIG = "CONFIG"
    KEYS = "KEYS"
    INFO = "INFO"

class StreamingCommand(Enum):
    PSYNC = "PSYNC"
    MONITOR = "MONITOR"
    # Add more streaming/multi-response commands as needed

class RedisAction:
    handlers = {}

    @classmethod
    def command(cls, name):
        def decorator(func):
            cls.handlers[name.upper()] = func
            return func
        return decorator

    def __init__(self, storage):
        self.storage = storage

    def handle_input(self, data: bytes):
        decoder = RESPDecoder()
        encoder = RESPEncoder()
        ins = decoder.decode(data)
        print(f"decoded data {ins} {type(ins)}")
        if ins is None:
            return encoder.encode("ERR: Invalid input")
        if "PING" in ins:
            command, args = "PING", None
        else:
            command, *args = ins
        handler = self.handlers.get(command.upper())
        if handler:
            result = handler(self, args)
            if isinstance(result, types.AsyncGeneratorType) or isinstance(result, types.GeneratorType):
                return result
            else:
                return encoder.encode(result)
        else:
            return encoder.encode("ERR: Unknown command")

@RedisAction.command("PING")
def handle_ping(self, args):
    return "PONG"

@RedisAction.command("ECHO")
def handle_echo(self, args):
    return args[0]

@RedisAction.command("SET")
def handle_set(self, data: list):
    if len(data) > 2:
        expiry = int(data[-1]) if data[-2] == "px" else int(data[-1])
        data = data[:2]
        print(f"Storing.. {data[0]}, {data[1]} with expiry {expiry}")
        self.storage.store(data[0], data[1], expiry)
    else:
        print(f"Storing.. {data[0]}, {data[1]}")
        self.storage.store(data[0], data[1], -1)
    return "OK"

@RedisAction.command("GET")
def handle_get(self, data):
    return self.storage.fetch(data[0])

@RedisAction.command("CONFIG")
def handle_config(self, data):
    if data[-1] == "dir":
        return self.storage.get_dir()
    else:
        return self.storage.get_dbfielname()

@RedisAction.command("KEYS")
def handle_keys(self, data):
    return self.storage.fetch_all_keys()

@RedisAction.command("INFO")
def handle_info(self, data):
    return self.storage.get_metadata_str()

@RedisAction.command("REPLCONF")
def handle_replconf(self, data):
    return "OK"

@RedisAction.command("PSYNC")
async def handle_psync(self, data):
    encoder = RESPEncoder()
    # 1. Send FULLRESYNC
    yield encoder.encode(f"FULLRESYNC {self.storage.metadata.master_replid} 0")
    # 2. Send RDB as RESP bulk string
    rdb_path = self.storage.rdb_path
    if rdb_path and os.path.exists(rdb_path):
        with open(rdb_path, "rb") as f:
            rdb_data = f.read()
        if len(rdb_data) > 0:
            header = f"${len(rdb_data)}\r\n".encode()
            yield header + rdb_data + b"\r\n"
            return
    # If no RDB file or empty, send minimal valid RDB
    minimal_rdb = b"REDIS0011\xff"
    header = f"${len(minimal_rdb)}\r\n".encode()
    yield header + minimal_rdb
    