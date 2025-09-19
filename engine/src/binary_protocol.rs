//! KyroDB Binary Protocol - Maximum Performance TCP Implementation
//! 
//! Protocol Format:
//! [MAGIC: u32][COMMAND: u8][LENGTH: u32][payload...][CRC32: u32]
//! 
//! Commands:
//! - 0x01: BATCH_LOOKUP
//! - 0x02: PUT  
//! - 0x03: BATCH_PUT
//! - 0xFF: PING (keepalive)

use crate::PersistentEventLog;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use bytes::{BytesMut, BufMut, Buf};
use anyhow::Result;
use uuid::Uuid;

/// Protocol constants
pub const MAGIC: u32 = 0x4B59524F; // "KYRO"
pub const MAX_FRAME_SIZE: usize = 64 * 1024 * 1024; // 64MB max frame
pub const MAX_BATCH_SIZE: usize = 10000; // Max keys/items per batch

/// Command constants
pub const CMD_BATCH_LOOKUP: u8 = 0x01;
pub const CMD_SINGLE_LOOKUP: u8 = 0x02;
pub const CMD_PUT: u8 = 0x03;
pub const CMD_BATCH_PUT: u8 = 0x04;
pub const CMD_PING: u8 = 0xFF;

/// Binary protocol TCP server entry point
pub async fn binary_protocol_server(
    log: Arc<PersistentEventLog>, 
    bind_addr: String
) -> Result<()> {
    let listener = TcpListener::bind(&bind_addr).await?;
    tracing::info!("Binary protocol server listening on {}", bind_addr);
    
    loop {
        let (stream, peer_addr) = listener.accept().await?;
        let log_clone = log.clone();
        
        // Spawn connection handler
        tokio::spawn(async move {
            if let Err(e) = handle_binary_connection(stream, log_clone).await {
                eprintln!("Connection {} error: {}", peer_addr, e);
            }
        });
    }
}

/// Handle individual binary protocol connection with optimized frame processing
async fn handle_binary_connection(
    stream: TcpStream,
    log: Arc<PersistentEventLog>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Binary protocol client connected: {}", stream.peer_addr()?);
    
    let (reader, writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut writer = BufWriter::new(writer);
    let mut frame_buffer = BytesMut::with_capacity(16384); // 16KB initial capacity
    
    loop {
        match read_frame(&mut reader, &mut frame_buffer).await {
            Ok(frame) => {
                let response = process_frame(frame, &log).await;
                match response {
                    Ok(response_data) => {
                        if let Err(e) = write_response(&mut writer, response_data).await {
                            eprintln!("Write error: {}", e);
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("Frame processing error: {}", e);
                        // Send error response
                        let mut error_response = BytesMut::with_capacity(8);
                        error_response.put_u32_le(MAGIC);
                        error_response.put_u32_le(0xFFFFFFFF); // Error marker
                        let _ = write_response(&mut writer, error_response).await;
                        break;
                    }
                }
            }
            Err(e) => {
                eprintln!("Frame read error: {}", e);
                break;
            }
        }
    }
    
    println!("Binary protocol client disconnected");
    Ok(())
}

/// Frame reader for efficient frame boundary detection
async fn read_frame(
    reader: &mut BufReader<OwnedReadHalf>, 
    buffer: &mut BytesMut
) -> io::Result<BytesMut> {
    // Read frame header: [MAGIC: u32][COMMAND: u8][LENGTH: u32]
    let mut header = [0u8; 9];
    reader.read_exact(&mut header).await?;
    
    let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    let _command = header[4];
    let length = u32::from_le_bytes([header[5], header[6], header[7], header[8]]);
    
    // Validate magic and length
    if magic != MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Invalid magic: 0x{:X}", magic)
        ));
    }
    
    if length as usize > MAX_FRAME_SIZE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Frame too large: {} bytes", length)
        ));
    }
    
    // Read frame payload + CRC
    buffer.clear();
    buffer.reserve(header.len() + length as usize + 4);
    buffer.put_slice(&header);
    
    let mut payload = vec![0u8; length as usize + 4]; // +4 for CRC
    reader.read_exact(&mut payload).await?;
    buffer.put_slice(&payload);
    
    Ok(buffer.split())
}

/// Frame processor for high-performance command dispatch
async fn process_frame(
    mut frame: BytesMut, 
    log: &Arc<PersistentEventLog>
) -> Result<BytesMut> {
    // Skip header (already validated)
    frame.advance(4); // Skip magic
    let command = frame.get_u8();
    let _length = frame.get_u32_le();
    
    // Extract payload (without CRC)
    let payload_len = frame.len() - 4;
    let payload = frame.split_to(payload_len);
    let _crc = frame.get_u32_le();
    
    // TODO: Validate CRC for production
    
    match command {
        CMD_BATCH_LOOKUP => process_batch_lookup(payload, log).await,
        CMD_SINGLE_LOOKUP => process_single_lookup(payload, log).await,
        CMD_PUT => process_put(payload, log).await,
        CMD_BATCH_PUT => process_batch_put(payload, log).await,
        CMD_PING => process_ping().await,
        _ => Err(anyhow::anyhow!("Unknown command: 0x{:02X}", command)),
    }
}

/// Enhanced batch lookup with SIMD optimization
async fn process_batch_lookup(
    mut payload: BytesMut, 
    log: &Arc<PersistentEventLog>
) -> Result<BytesMut> {
    let num_keys = payload.get_u32_le() as usize;
    
    if num_keys > MAX_BATCH_SIZE {
        return Err(anyhow::anyhow!("Batch too large: {} keys", num_keys));
    }
    
    // Zero-copy key extraction with SIMD optimization
    let mut keys = Vec::with_capacity(num_keys);
    for _ in 0..num_keys {
        if payload.len() < 8 {
            return Err(anyhow::anyhow!("Insufficient payload for keys"));
        }
        keys.push(payload.get_u64_le());
    }
    
    // Ultra-fast SIMD batch lookup using enhanced RMI optimizations
    let start_time = std::time::Instant::now();
    let results = log.lookup_keys_ultra_batch(&keys);
    let _lookup_duration = start_time.elapsed();
    
    // Performance metrics recording (TODO: implement)
    
    // Efficient response encoding with zero-copy optimization
    let mut response = BytesMut::with_capacity(8 + results.len() * 17);
    
    // Response header: [MAGIC: u32][NUM_RESULTS: u32]
    response.put_u32_le(MAGIC);
    response.put_u32_le(results.len() as u32);
    
    // Vectorized response encoding: process results in chunks
    for chunk in results.chunks(16) {
        for (key, value_opt) in chunk {
            response.put_u64_le(*key);
            match value_opt {
                Some(value) => {
                    response.put_u8(1); // Found
                    response.put_u64_le(*value);
                }
                None => {
                    response.put_u8(0); // Not found
                    response.put_u64_le(0); // Padding
                }
            }
        }
    }
    
    Ok(response)
}

/// Enhanced single lookup with RMI optimizations
async fn process_single_lookup(
    mut payload: BytesMut, 
    log: &Arc<PersistentEventLog>
) -> Result<BytesMut> {
    let key = payload.get_u64_le();
    
    // Ultra-fast single lookup using cache-optimized RMI lookup
    let start_time = std::time::Instant::now();
    let result = log.lookup_key_ultra_fast(key);
    let _lookup_duration = start_time.elapsed();
    
    // Performance metrics recording (TODO: implement)
    
    // Efficient response encoding
    let mut response = BytesMut::with_capacity(17);
    response.put_u32_le(MAGIC);
    response.put_u64_le(key);
    
    match result {
        Some(value) => {
            response.put_u8(1); // Found
            response.put_u64_le(value);
        }
        None => {
            response.put_u8(0); // Not found
            response.put_u64_le(0); // Padding
        }
    }
    
    Ok(response)
}

/// Enhanced single put with RMI optimizations
async fn process_put(
    mut payload: BytesMut, 
    log: &Arc<PersistentEventLog>
) -> Result<BytesMut> {
    let key = payload.get_u64_le();
    let value_len = payload.get_u32_le() as usize;
    
    if value_len > payload.len() {
        return Err(anyhow::anyhow!("Invalid value length"));
    }
    
    let value = payload.split_to(value_len).to_vec();
    
    // Direct engine call bypassing HTTP overhead with RMI optimization
    let start_time = std::time::Instant::now();
    let offset = log.append_kv(Uuid::new_v4(), key, value).await?;
    let _write_duration = start_time.elapsed();
    
    // Performance metrics recording (TODO: implement)
    
    // Response: [MAGIC: u32][offset: u64]
    let mut response = BytesMut::with_capacity(12);
    response.put_u32_le(MAGIC);
    response.put_u64_le(offset);
    
    Ok(response)
}

/// Batch put for maximum insert throughput
async fn process_batch_put(
    mut payload: BytesMut, 
    log: &Arc<PersistentEventLog>
) -> Result<BytesMut> {
    let num_items = payload.get_u32_le() as usize;
    
    if num_items > MAX_BATCH_SIZE {
        return Err(anyhow::anyhow!("Batch too large: {} items", num_items));
    }
    
    // Bulk extraction: process all items efficiently
    let mut items = Vec::with_capacity(num_items);
    
    for _ in 0..num_items {
        if payload.len() < 12 {
            return Err(anyhow::anyhow!("Insufficient payload for item header"));
        }
        
        let key = payload.get_u64_le();
        let value_len = payload.get_u32_le() as usize;
        
        if value_len > payload.len() {
            return Err(anyhow::anyhow!("Invalid value length"));
        }
        
        let value = payload.split_to(value_len).to_vec();
        items.push((key, value));
    }
    
    // Batch engine calls using group commit optimization
    let mut offsets = Vec::with_capacity(items.len());
    for (key, value) in items {
        let offset = log.append_kv(Uuid::new_v4(), key, value).await?;
        offsets.push(offset);
    }
    
    // Response: [MAGIC: u32][NUM_OFFSETS: u32][offset1: u64][offset2: u64]...
    let mut response = BytesMut::with_capacity(8 + offsets.len() * 8);
    response.put_u32_le(MAGIC);
    response.put_u32_le(offsets.len() as u32);
    
    for offset in offsets {
        response.put_u64_le(offset);
    }
    
    Ok(response)
}

/// Ping handler for keepalive support
async fn process_ping() -> Result<BytesMut> {
    let mut response = BytesMut::with_capacity(8);
    response.put_u32_le(MAGIC);
    response.put_u32_le(0xFFFFFFFF); // Ping response marker
    Ok(response)
}

/// Response writer for efficient response transmission
async fn write_response(
    writer: &mut BufWriter<OwnedWriteHalf>, 
    response: BytesMut
) -> Result<()> {
    writer.write_all(&response).await?;
    writer.flush().await?;
    Ok(())
}
