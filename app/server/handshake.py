import asyncio

from app.data.metadata import ServerMetadata
from app.resp.RESPCodec import RESPEncoder, RESPDecoder
from app.action import RedisAction


async def handshake(metadata: ServerMetadata, port: int, storage=None):
    """Complete handshake with master and handle command propagation"""
    if metadata.role != "slave":
        return

    host, master_port = metadata.replicaof.split(" ")
    try:
        reader, writer = await asyncio.open_connection(host, int(master_port))
        
        # Phase 1: Complete handshake sequence
        print(f"[{metadata.role}] Starting handshake with master {host}:{master_port}")
        await perform_handshake_sequence(reader, writer, port, metadata.role)
        
        # Phase 2: Handle FULLRESYNC + RDB transfer
        print(f"[{metadata.role}] Handshake complete, receiving RDB...")
        remaining_commands = await handle_fullresync_and_rdb(reader, metadata.role)
        
        # Phase 3: Command propagation loop
        print(f"[{metadata.role}] Starting command propagation...")
        await handle_command_propagation(reader, writer, storage, metadata.role, remaining_commands)
        
    except Exception as e:
        print(f"Unable to connect to master: {e}")
    finally:
        if 'writer' in locals():
            writer.close()
            await writer.wait_closed()


async def perform_handshake_sequence(reader, writer, port, role):
    """Phase 1: Send PING, REPLCONF twice, and PSYNC commands"""
    encoder = RESPEncoder()
    
    # Step 1: PING
    print(f"[{role}] Sending PING...")
    await send_command(reader, writer, encoder, ["PING"])
    
    # Step 2: REPLCONF listening-port
    print(f"[{role}] Sending REPLCONF listening-port {port}...")
    await send_command(reader, writer, encoder, ["REPLCONF", "listening-port", str(port)])
    
    # Step 3: REPLCONF capa
    print(f"[{role}] Sending REPLCONF capa psync2...")
    await send_command(reader, writer, encoder, ["REPLCONF", "capa", "psync2"])
    
    # Step 4: PSYNC (no response handling here, done in next phase)
    print(f"[{role}] Sending PSYNC...")
    writer.write(encoder.encode(["PSYNC", "?", "-1"]))
    await writer.drain()


async def handle_fullresync_and_rdb(reader, role):
    """Phase 2: Handle FULLRESYNC response and RDB transfer"""
    # Read FULLRESYNC response + RDB data (might contain commands too)
    data = await reader.read(1024)
    print(f"[{role}] FULLRESYNC response: {data}")
    
    pos = 0
    
    # Parse FULLRESYNC message (either +FULLRESYNC or $SIZE\r\nFULLRESYNC format)
    if data.startswith(b'+'):
        # Simple string format: +FULLRESYNC ...\r\n
        crlf_pos = data.find(b'\r\n')
        if crlf_pos == -1:
            raise Exception("Invalid FULLRESYNC simple string format")
        
        fullresync_msg = data[1:crlf_pos].decode('utf-8')
        print(f"[{role}] FULLRESYNC message: {fullresync_msg}")
        pos = crlf_pos + 2
        
    elif data.startswith(b'$'):
        # Bulk string format: $53\r\nFULLRESYNC ...\r\n
        crlf_pos = data.find(b'\r\n')
        if crlf_pos == -1:
            raise Exception("Invalid FULLRESYNC bulk string format")
        
        fullresync_size = int(data[1:crlf_pos])
        pos = crlf_pos + 2
        fullresync_msg = data[pos:pos + fullresync_size].decode('utf-8')
        print(f"[{role}] FULLRESYNC message: {fullresync_msg}")
        pos += fullresync_size + 2  # +2 for trailing \r\n
    else:
        raise Exception(f"Unexpected FULLRESYNC format: {data[:10]}")
    
    # Parse RDB bulk string - it might be in the same packet or the next one
    if pos >= len(data):
        # RDB data is in the next packet, read it
        print(f"[{role}] RDB data not in FULLRESYNC packet, reading next packet...")
        rdb_data = await reader.read(1024)
        if not rdb_data:
            raise Exception("Connection closed while waiting for RDB data")
        
        if rdb_data[0:1] != b'$':
            raise Exception(f"Expected RDB bulk string, got: {rdb_data[:10]}")
        
        rdb_header_end = rdb_data.find(b'\r\n')
        if rdb_header_end == -1:
            raise Exception("Invalid RDB bulk string format")
            
        rdb_size = int(rdb_data[1:rdb_header_end])
        rdb_content_start = rdb_header_end + 2
        
        print(f"[{role}] RDB size: {rdb_size} bytes")
        
        # Extract RDB data
        if rdb_size > 0:
            actual_rdb_data = rdb_data[rdb_content_start:rdb_content_start + rdb_size]
            print(f"[{role}] RDB transfer complete ({len(actual_rdb_data)} bytes received)")
        else:
            print(f"[{role}] Empty RDB received - no data to transfer")
        
        # Return any remaining commands that came after RDB
        remaining_pos = rdb_content_start + rdb_size
        remaining_commands = rdb_data[remaining_pos:] if remaining_pos < len(rdb_data) else b''
        
    elif data[pos:pos+1] != b'$':
        raise Exception(f"Expected RDB bulk string after FULLRESYNC, got: {data[pos:pos+10]}")
    else:
        # RDB data is in the same packet
        rdb_header_end = data.find(b'\r\n', pos)
        if rdb_header_end == -1:
            raise Exception("Invalid RDB bulk string format")
            
        rdb_size = int(data[pos+1:rdb_header_end])
        pos = rdb_header_end + 2
        
        print(f"[{role}] RDB size: {rdb_size} bytes")
        
        # Extract RDB data
        if rdb_size > 0:
            rdb_data = data[pos:pos + rdb_size]
            pos += rdb_size
            print(f"[{role}] RDB transfer complete ({rdb_size} bytes received)")
        else:
            print(f"[{role}] Empty RDB received - no data to transfer")
        
        # Return any remaining commands that came with RDB
        remaining_commands = data[pos:] if pos < len(data) else b''
    
    if remaining_commands:
        print(f"[{role}] Found commands after RDB: {remaining_commands}")
    
    return remaining_commands


async def handle_command_propagation(reader, writer, storage, role, initial_commands=b''):
    """Phase 3: Handle ongoing command propagation from master"""
    decoder = RESPDecoder()
    action = RedisAction(storage) if storage else None
    
    # Process any commands that came with the RDB
    command_buffer = initial_commands
    
    while True:
        try:
            # Get data to process
            if command_buffer:
                data = command_buffer
                command_buffer = b''  # Clear buffer after use
            else:
                data = await reader.read(1024)
                if not data:
                    print(f"[{role}] Master disconnected")
                    break
            
            print(f"[{role}] Received command from master: {data}")
            
            # Process all commands in the data (bulk or single)
            await process_commands_from_stream(decoder, action, data, role, writer)
            
        except Exception as e:
            print(f"[{role}] Error in command propagation: {e}")
            import traceback
            traceback.print_exc()
            break


async def process_commands_from_stream(decoder, action, data, role, writer=None):
    """Process all RESP commands from a data stream (handles bulk commands)"""
    if not action:
        return
    
    try:
        # Feed data to decoder and process first command
        first_command = decoder.decode(data)
        if first_command:
            await execute_command(action, first_command, role, writer)
        
        # Process all remaining commands from decoder buffer
        while True:
            command = decoder.decode(b'')  # Process from internal buffer
            if command is None:
                break
            await execute_command(action, command, role, writer)
            
    except Exception as e:
        print(f"[{role}] Error processing commands: {e}")
        import traceback
        traceback.print_exc()


async def execute_command(action, command, role, writer=None):
    """Execute a single command on the replica"""
    print(f"[{role}] Parsed command: {command}")
    
    # Convert command back to RESP format for action.handle_input
    encoder = RESPEncoder()
    command_bytes = encoder.encode(command)
    
    # Execute the command and get response
    result = await action.handle_input(command_bytes, writer=writer)
    
    # Send any response back to master (RedisAction decides what needs responses)
    if result and writer:
        writer.write(result)
        await writer.drain()
        print(f"[{role}] Sent response back to master")
    
    print(f"[{role}] Command executed successfully")


async def send_command(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, encoder: RESPEncoder, command: list[str]):
    """Send a command and wait for response during handshake"""
    writer.write(encoder.encode(command))
    await writer.drain()
    response = await reader.read(1024)
    print(f"[slave] Handshake response: {response}")
    return response