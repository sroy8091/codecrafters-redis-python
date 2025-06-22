import json
from typing import Any

from app.expiry import check_expiry, get_current_time


class RedisStore:
    def __init__(self, __dir: str, dbfilename: str):
        self.memory = dict()
        self.dir = __dir
        self.dbfilename = dbfilename
        if self.dir is not None and self.dbfilename is not None:
            self.memory = self.dir + "/" + self.dbfilename
        print("memory stored in ", self.memory)

    def get_dir(self):
        return ["dir", self.dir]

    def get_dbfilename(self):
        return ["dbfilename", self.dbfilename]
    
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