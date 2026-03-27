# 0008 Remote Root Filesystem Pressure

## Summary

- The HannsDB reference host has free capacity on `/data`, but the root filesystem `/` is already full
- Default Rust and Python tool paths write into `/root`, which immediately blocks source builds and system-level package installs

## Evidence

- `df -h` on 2026-03-27 reported:
  - `/dev/vda1` mounted on `/`: `40G used / 0 available / 100%`
  - `/dev/vdb` mounted on `/data`: `20G used / 167G available`
- `cargo build --features ffi --lib --release` in `/data/work/knowhere-rs` failed with:
  - `database or disk is full`
  - `No space left on device (os error 28)`
- The same host shows:
  - `/root/.cargo`: `203M`
  - `/data/work/knowhere-rs`: `7.0M`
  - `/data/work/milvus-rs-integ`: `127M`

## Impact

- Default `cargo` registry/cache writes to `/root/.cargo`, not `/data`
- Default temporary files may also hit `/tmp` on `/`
- Stage-1 remote builds will fail unless caches, temporary files, and Python virtual environments are redirected to `/data/work`

## Stage 1 Policy

- Do not delete unrelated remote data as a first response
- Prefer redirecting build state into `/data/work/milvus-rs-integ`:
  - `CARGO_HOME`
  - `CARGO_TARGET_DIR`
  - `TMPDIR`
  - Python venv path for `conan`
