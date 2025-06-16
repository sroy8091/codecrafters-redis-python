import json

from app.RESPCodec import RESPDecoder, RESPEncoder


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
    json_str = json.dumps({data[0]: data[1]})
    with open('memory.json', 'w') as f:
        f.write(json_str)
    return "OK"

def get_memory(data):
    with open('memory.json', 'r') as f:
        memory = json.loads(f.read())
    return memory[data[0]]

resolver = {
    "PING": lambda x : "PONG",
    "ECHO": lambda x : x[0],
    "SET": set_memory,
    "GET": get_memory,
}