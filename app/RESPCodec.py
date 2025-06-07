from codecs import IncrementalEncoder, IncrementalDecoder
from typing import Any, Optional


class RESPEncoder(IncrementalEncoder):
    def encode(self, input: Any, final: bool = False) -> bytes:
        if input is None:
            return b'$-1\r\n'

        if isinstance(input, str):
            encoded = input.encode('utf-8')
            return f"${len(encoded)}\r\n{input}\r\n".encode('utf-8')

        if isinstance(input, bytes):
            return f"${len(input)}\r\n".encode('utf-8') + input + b'\r\n'

        if isinstance(input, int):
            return f":{input}\r\n".encode('utf-8')

        if isinstance(input, list):
            parts = [f"*{len(input)}\r\n".encode('utf-8')]
            for item in input:
                parts.append(self.encode(item))
            return b''.join(parts)

        if isinstance(input, Error):
            return f"-{str(input)}\r\n".encode('utf-8')

        if isinstance(input, SimpleString):
            return f"+{str(input)}\r\n".encode('utf-8')

        raise ValueError(f"Unsupported type for RESP encoding: {type(input)}")


class RESPDecoder(IncrementalDecoder):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.buffer = b''

    def decode(self, input: bytes, final: bool = False) -> Optional[Any]:
        self.buffer += input

        if not self.buffer:
            return None

        try:
            result, consumed = self._decode_one(self.buffer)
            self.buffer = self.buffer[consumed:]
            return result
        except (IndexError, ValueError):
            # Not enough data yet
            if final:
                # If this is the final chunk, raise the error
                raise
            return None

    def _decode_one(self, data: bytes) -> tuple[Any, int]:
        if not data:
            return None, 0

        data_type = chr(data[0])
        pos = 1

        if data_type == '+':  # Simple String
            end = data.find(b'\r\n', pos)
            if end == -1:
                raise ValueError("Incomplete simple string")
            return SimpleString(data[pos:end].decode('utf-8')), end + 2

        elif data_type == '-':  # Error
            end = data.find(b'\r\n', pos)
            if end == -1:
                raise ValueError("Incomplete error")
            return Error(data[pos:end].decode('utf-8')), end + 2

        elif data_type == ':':  # Integer
            end = data.find(b'\r\n', pos)
            if end == -1:
                raise ValueError("Incomplete integer")
            return int(data[pos:end]), end + 2

        elif data_type == '$':  # Bulk String
            end = data.find(b'\r\n', pos)
            if end == -1:
                raise ValueError("Incomplete bulk string length")
            length = int(data[pos:end])
            if length == -1:  # Null
                return None, end + 2

            str_start = end + 2
            str_end = str_start + length
            if len(data) < str_end + 2:
                raise ValueError("Incomplete bulk string data")
            if data[str_end:str_end + 2] != b'\r\n':
                raise ValueError("Missing CRLF after bulk string")
            return data[str_start:str_end].decode('utf-8'), str_end + 2

        elif data_type == '*':  # Array
            end = data.find(b'\r\n', pos)
            if end == -1:
                raise ValueError("Incomplete array length")
            count = int(data[pos:end])
            if count == -1:  # Null array
                return None, end + 2

            pos = end + 2
            result = []
            for _ in range(count):
                value, consumed = self._decode_one(data[pos:])
                result.append(value)
                pos += consumed
            return result, pos

        raise ValueError(f"Invalid RESP data type: {data_type}")

    def reset(self):
        self.buffer = b''


class Error(Exception):
    """Represents a RESP Error."""
    pass


class SimpleString:
    """Represents a RESP Simple String."""

    def __init__(self, value: str):
        self.value = value

    def __str__(self):
        return self.value