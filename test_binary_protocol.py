#!/usr/bin/env python3
"""
Quick test to verify binary protocol is working
"""
import socket
import struct
import time

def test_binary_protocol():
    """Test basic binary protocol connectivity"""
    try:
        # Connect to binary protocol server
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5.0)
        sock.connect(('127.0.0.1', 4030))
        
        print("✅ Connected to binary protocol server on port 4030")
        
        # Send PING command (0xFF)
        # Format: MAGIC (4 bytes) + command (1 byte) + length (4 bytes) + payload + CRC (4 bytes)
        magic = 0x4B59524F  # "KYRO" 
        command = 0xFF  # PING
        payload_len = 0  # PING has no payload
        crc = 0x12345678  # Dummy CRC for test
        
        # Send header
        header = struct.pack('<IBI', magic, command, payload_len)
        # Since payload_len = 0, no payload bytes
        # Send CRC
        crc_bytes = struct.pack('<I', crc)
        
        frame = header + crc_bytes
        sock.send(frame)
        print("✅ Sent PING command to binary protocol server")
        
        # Try to read response
        response = sock.recv(1024)
        if response:
            print(f"✅ Received response from binary protocol server: {len(response)} bytes")
            # Parse response
            if len(response) >= 8:
                resp_magic, resp_len = struct.unpack('<II', response[:8])
                print(f"  Response MAGIC: 0x{resp_magic:08X}")
                print(f"  Response length: {resp_len}")
        else:
            print("⚠️ No response received from binary protocol server")
            
        sock.close()
        return True
        
    except ConnectionRefusedError:
        print("❌ Connection refused - binary protocol server not running on port 4030")
        return False
    except Exception as e:
        print(f"❌ Error testing binary protocol: {e}")
        return False

if __name__ == "__main__":
    print("🧪 Testing Binary Protocol Server...")
    test_binary_protocol()
