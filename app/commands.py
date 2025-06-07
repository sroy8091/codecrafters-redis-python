from app.RESPCodec import RESPDecoder, RESPEncoder


def handle_input(data):
    decoder = RESPDecoder()
    encoder = RESPEncoder()
    ins = decoder.decode(data)
    print(f"decoded data {ins} {type(ins)}")
    if "PING" in ins:
        command, args = "PING", None
    else:
        command, args = ins
    return encoder.encode(resolver[command](args))


resolver = {
    "PING": lambda x : "PONG",
    "ECHO": lambda x : x
}