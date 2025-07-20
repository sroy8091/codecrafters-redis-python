import json
import os
import struct
from typing import Any

from app.data.expiry import check_expiry, get_current_time
from app.data.metadata import ServerMetadata

class RedisStore:
    def __init__(self, __dir: str, dbfilename: str, metadata: ServerMetadata):
        self.memory = dict()
        self.dir = __dir
        self.dbfilename = dbfilename
        self.rdb_path = None
        self.metadata = metadata

        if self.dir is not None and self.dbfilename is not None:
            self.rdb_path = os.path.join(self.dir, self.dbfilename)
            if os.path.exists(self.rdb_path):
                self._load_rdb(self.rdb_path)
        print("memory stored in ", self.memory)

    def _load_rdb(self, path):
        with open(path, "rb") as f:
            data = f.read()
        pos = 0
        # Check header
        if data[:9] != b'REDIS0011':
            print("Invalid RDB header")
            return
        pos = 9
        expiry = -1
        while pos < len(data):
            b1 = data[pos]
            if b1 == 0xFE:  # DB selector
                pos += 2  # skip FE and db number (assume 1 byte db index)
            elif b1 == 0xFB:  # Hash table size info
                pos += 1
                _, n = _decode_size(data, pos)
                pos += n
                _, n2 = _decode_size(data, pos)
                pos += n2
            elif b1 == 0xFC:  # Expiry in ms
                pos += 1
                expiry = struct.unpack('<Q', data[pos:pos+8])[0]
                pos += 8
            elif b1 == 0xFD:  # Expiry in s
                pos += 1
                expiry = struct.unpack('<I', data[pos:pos+4])[0] * 1000
                pos += 4
            elif b1 == 0xFF:  # End of file
                break
            elif b1 == 0x00:  # String type
                pos += 1
                key, n = _decode_string(data, pos)
                pos += n
                value, n = _decode_string(data, pos)
                pos += n
                self.memory[key] = (value, expiry)
                expiry = -1  # reset expiry for next key
            else:
                # Unknown type, skip
                pos += 1

    def get_dir(self):
        return ["dir", self.dir]

    def get_dbfilename(self):
        return ["dbfilename", self.dbfilename]

    def get_metadata(self) -> str:
        return self.metadata.to_str()
    
    def store(self, key: str, value: Any, expiry: int):
        if isinstance(self.memory, dict):
            self.memory[key] = (value, get_current_time() + expiry if expiry > 0 else -1)
        else:
            with open(self.memory, "w") as f:
                f.write(json.dumps({key: (value, get_current_time() + expiry)}))

    def fetch(self, key):
        if isinstance(self.memory, dict):
            return check_expiry(self.memory.get(key))
        else:
            with open(self.memory, "r") as f:
                # TODO we are loading the whole RDB file for each get, that's not good
                pair = json.loads(f.read())
                print(f"Fetched from {self.memory} and got {pair}")
            print(f"Found value {pair.get(key)} for key {key}")
            return check_expiry(pair.get(key))

    def fetch_all_keys(self):
        if isinstance(self.memory, dict):
            return list(self.memory.keys())
        else:
            with open(self.memory, "r") as f:
                # TODO we are loading the whole RDB file for each get, that's not good
                pair = json.loads(f.read())
                print(f"Fetched from {self.memory} and got {pair}")
            return list(pair.keys())

def _decode_size(data, pos):
    b = data[pos]
    type_ = (b & 0xC0) >> 6
    if type_ == 0:
        return b & 0x3F, 1
    elif type_ == 1:
        val = ((b & 0x3F) << 8) | data[pos+1]
        return val, 2
    elif type_ == 2:
        val = int.from_bytes(data[pos+1:pos+5], 'big')
        return val, 5
    else:
        # Not handling special string encodings for this stage
        return 0, 1

def _decode_string(data, pos):
    strlen, n = _decode_size(data, pos)
    pos += n
    s = data[pos:pos+strlen].decode('utf-8')
    return s, n + strlen