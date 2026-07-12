//! Enforce the top-level VERSION file as the single authoritative version
//! source. Cargo requires the manifest `version` to be a literal, so it cannot
//! read VERSION directly; this guard fails the build if the two drift apart.
//! Runtime version strings are sourced from VERSION via include_str! in the
//! binary — this only keeps the cargo metadata honest.

use std::path::Path;

fn main() {
    let version_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../VERSION");
    println!("cargo:rerun-if-changed={}", version_path.display());
    let file_version = std::fs::read_to_string(&version_path)
        .expect("read VERSION")
        .trim()
        .to_string();
    let manifest_version = std::env::var("CARGO_PKG_VERSION").unwrap();
    assert!(
        file_version == manifest_version,
        "VERSION file ({file_version}) disagrees with Cargo.toml version ({manifest_version}); \
         update [workspace.package] version in Cargo.toml to match VERSION"
    );
}
