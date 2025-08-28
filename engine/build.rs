use std::process::Command;

fn main() {
    // Best-effort Git commit (works in git checkouts; falls back to "unknown" in archives/CI)
    let commit = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|o| {
            if o.status.success() {
                Some(String::from_utf8_lossy(&o.stdout).trim().to_string())
            } else {
                None
            }
        })
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=GIT_COMMIT_HASH={}", commit);

    // Collect enabled Cargo features from env vars like CARGO_FEATURE_FOO=1
    let mut features: Vec<String> = std::env::vars()
        .filter_map(|(k, v)| {
            if k.starts_with("CARGO_FEATURE_") && v == "1" {
                // Normalize: CARGO_FEATURE_SOME_FEATURE -> "some-feature"
                Some(
                    k["CARGO_FEATURE_".len()..]
                        .to_ascii_lowercase()
                        .replace('_', "-"),
                )
            } else {
                None
            }
        })
        .collect();
    features.sort();
    let feature_list = features.join(",");
    println!("cargo:rustc-env=CARGO_FEATURES={}", feature_list);

    // Rebuild when HEAD or refs change (best-effort; harmless if repo absent)
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/refs/heads");

    // Build gRPC code only when grpc feature is enabled
    let grpc_enabled = std::env::var("CARGO_FEATURE_GRPC").ok().as_deref() == Some("1");
    if grpc_enabled {
        // Watch the proto file relative to this crate (engine/)
        println!("cargo:rerun-if-changed=proto/kyrodb.proto");
        // Use vendored protoc to avoid system dependencies
        let protoc_path = protoc_bin_vendored::protoc_bin_path().expect("protoc not found");
        std::env::set_var("PROTOC", protoc_path);
        tonic_build::configure()
            .build_server(true)
            .build_client(true)
            .compile(&["proto/kyrodb.proto"], &["proto"]) // fixed paths
            .expect("failed to build gRPC code");
    }
}
