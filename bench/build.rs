fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=../engine/proto/kyrodb.proto");
    
    #[cfg(feature = "grpc")]
    {
        println!("Building gRPC protobuf...");
        
        // Check if proto file exists
        let proto_path = "../engine/proto/kyrodb.proto";
        if !std::path::Path::new(proto_path).exists() {
            panic!("Proto file not found at: {}", proto_path);
        }
        
        // Use the vendored protoc
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap());
        
        println!("Compiling protobuf from: {}", proto_path);
        
        // Use the default tonic build which writes to OUT_DIR
        tonic_build::configure()
            .build_server(false) // We only need the client
            .build_client(true)
            .compile(&[proto_path], &["../engine/proto"])?;
        
        println!("Protobuf compilation successful!");
    }
    
    #[cfg(not(feature = "grpc"))]
    {
        println!("gRPC feature not enabled, skipping protobuf compilation");
    }
    
    Ok(())
}
