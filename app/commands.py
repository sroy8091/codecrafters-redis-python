from enum import Enum

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