import json
import time

from app.RESPCodec import RESPDecoder, RESPEncoder
from app import memory


def handle_input(data):
    decoder = RESPDecoder()
    encoder = RESPEncoder()
    ins = decoder.decode(data)
    print(f"decoded data {ins} {type(ins)}")
    if "PING" in ins:
        command, args = "PING", None
    else:
        command, *args = ins
    return encoder.encode(resolver[command](args))

def set_memory(data):
    if len(data) > 2:
        expiry = data[-1] if data[-2] == "px" else data[-1] * 1000
        data = data[:2]
        json_str = json.dumps({data[0]: (data[1], time.time() * 1000 + int(expiry))})
    else:
        json_str = json.dumps({data[0]: (data[1], -1)})

    with open(memory.MEMORY_FILE, 'w') as f:
        print("Writing data...", json_str)
        f.write(json_str)
    return "OK"

def get_memory(data):
    with open(memory.MEMORY_FILE, 'r') as f:
        ram_memory = json.loads(f.read())
        print(f"Fetched from {memory.MEMORY_FILE} and got {ram_memory}")
    print(f"Found value {ram_memory.get(data[0])} for key {data[0]}")
    return check_expiry(ram_memory.get(data[0]))

def check_expiry(data):
    print("Checking expiry for...", data)
    if data[1] == -1:
        return data[0]
    elif data[1] > time.time() * 1000:
        return data[0]
    else:
        return None

def get_config(data):
    return get_memory([data[-1]])

resolver = {
    "PING": lambda x : "PONG",
    "ECHO": lambda x : x[0],
    "SET": set_memory,
    "GET": get_memory,
    "CONFIG": get_config
}