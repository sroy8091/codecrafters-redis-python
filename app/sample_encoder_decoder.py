from RESPCodec import RESPDecoder, RESPEncoder

def encode(s):
    encoder = RESPEncoder()
    print(f"Encoded data --- {encoder.encode(s)}")

def decode(s):
    decoder = RESPDecoder()
    print(f"Decode data --- {decoder.decode(s)}")

decode(b'*2\r\n$3\r\ndir\r\n$16\r\n/tmp/redis-files\r\n')