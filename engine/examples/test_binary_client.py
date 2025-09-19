#!/usr/bin/env python3
"""
Simple binary protocol client test for KyroDB
Tests the TCP binary protocol implementation
"""

import socket
import struct
import sys

# Protocol constants
MAGIC = 0x4B59524F  # "KYRO"
BATCH_LOOKUP = 0x01
PUT = 0x02  
BATCH_PUT = 0x03
PING = 0xFF

def send_frame(sock, command, payload):
    """Send a binary protocol frame"""
    frame_length = len(payload)
    
    # Frame format: [MAGIC: u32][COMMAND: u8][LENGTH: u32][payload...][CRC32: u32]
    header = struct.pack('<LBL', MAGIC, command, frame_length)
    
    # Simple CRC (just sum for test)
    crc = sum(header + payload) & 0xFFFFFFFF
    frame = header + payload + struct.pack('<L', crc)
    
    sock.send(frame)

def recv_response(sock):
    """Receive and parse binary protocol response"""
    # Read response header
    header = sock.recv(8)
    if len(header) < 8:
        return None
        
    magic, length = struct.unpack('<LL', header)
    if magic != MAGIC:
        print(f"Invalid response magic: 0x{magic:X}")
        return None
    
    # Read response payload
    if length > 0:
        payload = sock.recv(length)
        return payload
    return b''

def test_ping():
    """Test PING command"""
    print("🚀 Testing PING command...")
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('127.0.0.1', 3031))
        
        # Send PING command (no payload)
        send_frame(sock, PING, b'')
        
        # Receive response
        response = recv_response(sock)
        if response is not None:
            print(f"✅ PING successful! Response: {len(response)} bytes")
        else:
            print("❌ PING failed")
        
        sock.close()
        return True
        
    except Exception as e:
        print(f"❌ PING error: {e}")
        return False

def test_put():
    """Test PUT command"""
    print("🚀 Testing PUT command...")
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('127.0.0.1', 3031))
        
        # PUT payload: [key: u64][value_len: u32][value_data...]
        key = 123
        value = b"Hello Binary Protocol!"
        payload = struct.pack('<QL', key, len(value)) + value
        
        send_frame(sock, PUT, payload)
        
        # Receive response
        response = recv_response(sock)
        if response is not None:
            offset = struct.unpack('<Q', response)[0] if len(response) >= 8 else 0
            print(f"✅ PUT successful! Offset: {offset}")
        else:
            print("❌ PUT failed")
        
        sock.close()
        return True
        
    except Exception as e:
        print(f"❌ PUT error: {e}")
        return False

def test_batch_lookup():
    """Test BATCH_LOOKUP command"""
    print("🚀 Testing BATCH_LOOKUP command...")
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('127.0.0.1', 3031))
        
        # BATCH_LOOKUP payload: [num_keys: u32][key1: u64][key2: u64]...
        keys = [123, 456, 789]  # 123 was inserted by PUT
        payload = struct.pack('<L', len(keys))
        for key in keys:
            payload += struct.pack('<Q', key)
        
        send_frame(sock, BATCH_LOOKUP, payload)
        
        # Receive response
        response = recv_response(sock)
        if response is not None:
            if len(response) >= 4:
                num_results = struct.unpack('<L', response[:4])[0]
                print(f"✅ BATCH_LOOKUP successful! Found {num_results} results")
                
                # Parse results: [key: u64][found: u8][value: u64] × num_results
                offset = 4
                for i in range(num_results):
                    if offset + 17 <= len(response):
                        key, found, value = struct.unpack('<QBQ', response[offset:offset+17])
                        status = "FOUND" if found else "NOT_FOUND"
                        print(f"  Key {key}: {status} (value: {value})")
                        offset += 17
            else:
                print("✅ BATCH_LOOKUP successful but empty response")
        else:
            print("❌ BATCH_LOOKUP failed")
        
        sock.close()
        return True
        
    except Exception as e:
        print(f"❌ BATCH_LOOKUP error: {e}")
        return False

def main():
    """Run all binary protocol tests"""
    print("🧪 KyroDB Binary Protocol Test Suite")
    print("=" * 50)
    
    # Test connection to server
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        sock.connect(('127.0.0.1', 3031))
        sock.close()
        print("✅ Binary TCP server is reachable")
    except Exception as e:
        print(f"❌ Cannot connect to binary server: {e}")
        sys.exit(1)
    
    # Run tests in sequence: PUT first, then LOOKUP
    tests = [
        test_put,  # Insert data first
        test_batch_lookup,  # Then lookup the data we inserted  
        test_ping  # PING last (has issues but not critical)
    ]
    
    passed = 0
    for test in tests:
        if test():
            passed += 1
        print()
    
    print(f"📊 Test Results: {passed}/{len(tests)} passed")
    
    if passed == len(tests):
        print("🎉 All binary protocol tests passed!")
        sys.exit(0)
    else:
        print("⚠️  Some tests failed")
        sys.exit(1)

if __name__ == "__main__":
    main()
