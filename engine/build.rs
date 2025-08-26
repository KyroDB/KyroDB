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
}
