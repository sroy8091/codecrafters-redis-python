from enum import Enum

from app.RESPCodec import RESPDecoder, RESPEncoder

class Action(Enum):
    PING = "PING"
    ECHO = "ECHO"
    SET = "SET"
    GET = "GET"
    CONFIG = "CONFIG"
    KEYS = "KEYS"
    INFO = "INFO"


class RedisAction:
    def __init__(self, storage):
        self.resolver = {
            "PING": lambda x: "PONG",
            "ECHO": lambda x: x[0],
            "SET": self.set_memory,
            "GET": self.get_memory,
            "CONFIG": self.get_config,
            "KEYS": self.get_keys,
            "INFO": self.get_info,
        }
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
        return encoder.encode(self.resolver[command](args))

    def set_memory(self, data: list):
        if len(data) > 2:
            expiry = int(data[-1]) if data[-2] == "px" else int(data[-1])
            data = data[:2]
            print(f"Storing.. {data[0]}, {data[1]} with expiry {expiry}")
            self.storage.store(data[0], data[1], expiry)
        else:
            print(f"Storing.. {data[0]}, {data[1]}")
            self.storage.store(data[0], data[1], -1)
        return "OK"

    def get_memory(self, data):
        return self.storage.fetch(data[0])

    def get_config(self, data):
        if data[-1] == "dir":
            return self.storage.get_dir()
        else:
            return self.storage.get_dbfielname()

    def get_keys(self, data):
        return self.storage.fetch_all_keys()

    def get_info(self, data):
        return "role:master"