use file_protocol::{FileProtocol, FileProtocolConfig, ProtocolError, State};
use cubeos_service::Config;
use log::{error, info, warn};
use std::collections::HashMap;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use udp_rs::{UdpStream};
use hal_stream::Stream;
use cbor_protocol::Protocol;
use failure::bail;
use std::net::UdpSocket;

#[derive(Debug,Clone)]
pub struct FileService {
    config: FileProtocolConfig,
    source_ip: String,
    source_port: u16,
    target_ip: String,
    target_port: u16,
}
impl FileService {
    pub fn new(config: &Config) -> Self {
        // Get the storage directory prefix that we'll be using for our
        // temporary/intermediate storage location
        let prefix = match config.get("storage_dir") {
            Some(val) => val.as_str().map(|str| str.to_owned()),
            None => None,
        };

        // Get the chunk size to be used for transfers
        let transfer_chunk_size = match config.get("transfer_chunk_size") {
            Some(val) => val.as_integer().unwrap_or(1024),
            None => 1024,
        };

        // Get the chunk size to be used for hashing
        let hash_chunk_size = match config.get("hash_chunk_size") {
            Some(val) => val.as_integer().unwrap_or(transfer_chunk_size * 2),
            None => transfer_chunk_size * 2,
        } as usize;

        let transfer_chunk_size = transfer_chunk_size as usize;

        let hold_count = match config.get("hold_count") {
            Some(val) => val.as_integer().unwrap_or(5),
            None => 5,
        } as u16;

        // Get the inter chunk delay value
        let inter_chunk_delay = config
            .get("inter_chunk_delay")
            .and_then(|i| i.as_integer())
            .unwrap_or(1) as u64;

        // Get the max chunk transmission value
        let max_chunks_transmit = config
            .get("max_chunks_transmit")
            .and_then(|chunks| chunks.as_integer())
            .map(|chunks| chunks as u32);

        // Get the source ip
        let source_ip = config
            .get("source_ip")
            .unwrap()
            .as_str()
            .map(|ip| ip.to_owned())
            .unwrap_or_else(|| "0.0.0.0".to_owned());

        // Get the source port
        let source_port = config
            .get("source_port")
            .and_then(|port| port.as_integer())
            .map(|port| port as u16)
            .unwrap_or(0);

        // Get the target ip
        let target_ip = config
            .get("target_ip")
            .unwrap()
            .as_str()
            .map(|ip| ip.to_owned())
            .unwrap();

        // Get the target port
        let target_port = config
            .get("target_port")
            .and_then(|port| port.as_integer())
            .map(|port| port as u16)
            .unwrap_or(0);

        FileService {
            config: FileProtocolConfig::new(
                prefix,
                transfer_chunk_size,
                hold_count,
                inter_chunk_delay,
                max_chunks_transmit,
                hash_chunk_size,
            ),
            source_ip,
            source_port,
            target_ip,
            target_port,
        }
    }
    pub fn download(&self, source_path: String, target_path: String) -> Result<(), failure::Error> {        
        let f_protocol: FileProtocol<UdpStream> = FileProtocol::new(
            UdpStream::new(format!("{}:{}",self.source_ip,self.source_port),format!("{}:{}",self.target_ip,self.target_port)),
            self.config.clone(),
        );
        download(f_protocol, &source_path, &target_path)
    }
    pub fn upload(&self, source_path: String, target_path: String) -> Result<(), failure::Error> {
        let f_protocol: FileProtocol<UdpStream> = FileProtocol::new(
            UdpStream::new(format!("{}:{}",self.source_ip,self.source_port),format!("{}:{}",self.target_ip,self.target_port)),
            self.config.clone(),
        );
        upload(f_protocol, &source_path, &target_path)
    }
    pub fn cleanup(&self, hash: Option<String>) -> Result<(), failure::Error> {
        let f_protocol: FileProtocol<UdpStream> = FileProtocol::new(
            UdpStream::new(format!("{}:{}",self.source_ip,self.source_port),format!("{}:{}",self.target_ip,self.target_port)),
            self.config.clone(),
        );

        cleanup(f_protocol, hash)
    }
}

pub fn upload<T: Stream + std::fmt::Debug>(f_protocol: FileProtocol<T>, source_path: &str, target_path: &str) -> Result<(), failure::Error> 
where std::io::Error: From<<T as Stream>::StreamError>
{
    println!("Uploading local: {} to remote: {}", source_path, target_path);
    info!("Uploading local: {} to remote: {}", source_path, target_path);

    // Copy file to upload to temp storage. Calculate the hash and chunk info
    let (hash, num_chunks, mode) = f_protocol.initialize_file(&source_path)?;

    // Generate channel id for transaction
    let channel = f_protocol.generate_channel()?;

    // Tell our destination the hash and number of chunks to expect
    f_protocol.send_metadata(channel, &hash, num_chunks)?;

    // Send export command for file
    f_protocol.send_export(channel, &hash, &target_path, mode)?;

    // Start the engine to send the file data chunks
    f_protocol.message_engine(
        |d| f_protocol.recv(Some(d)),
        Duration::from_secs(2),
        &State::Transmitting,
    )?;
    Ok(())
}

pub fn download<T: Stream + std::fmt::Debug>(f_protocol: FileProtocol<T>, source_path: &str, target_path: &str) -> Result<(), failure::Error> 
where std::io::Error: From<<T as Stream>::StreamError>
{
    println!("Downloading remote: {} to local: {}", source_path, target_path);
    info!(
        "Downloading remote: {} to local: {}",
        source_path, target_path
    );

    // Generate channel id for transaction
    let channel = f_protocol.generate_channel()?;
    
    // set the default chunk number as 9999, WIP
    // let num_chunks = 9999;

    // Send our file request to the remote addr and verify that it's
    // going to be able to send it
    f_protocol.send_import(channel, source_path)?;

    // Wait for the request reply.
    // Note/TODO: We don't use a timeout here because we don't know how long it will
    // take the server to prepare the file we've requested.
    // Larger files (> 100MB) can take over a minute to process.
    let reply = match f_protocol.recv(None) {
        Ok(message) => message,
        Err(error) => bail!("Failed to import file: {}", error),
    };

    let (num_chunks, recv_message) = f_protocol.get_import_size(reply).unwrap();

    let state = f_protocol.process_message(
        recv_message,
        &State::StartReceive {
            path: target_path.to_string(),
        },
    )?;

    f_protocol.message_engine(
        |d| f_protocol.recv(Some(d)),
        Duration::from_secs(2),
        &state,
    )?;
    Ok(())
}

pub fn cleanup<T: Stream + std::fmt::Debug>(f_protocol: FileProtocol<T>, hash: Option<String>) -> Result<(), failure::Error> 
where std::io::Error: From<<T as Stream>::StreamError>
{
    match &hash {
        Some(s) => info!("Requesting remote cleanup of temp storage for hash {}", s),
        None => info!("Requesting remote cleanup of all temp storage"),
    }

    // Generate channel ID for transaction
    let channel = f_protocol.generate_channel()?;

    // Send our cleanup request to the remote addr and verify that it's
    // going to be able to send it
    f_protocol.send_cleanup(channel, hash)?;

    Ok(())
}