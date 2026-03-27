# 0007 Remote Prerequisites

## Summary

- The authoritative x86 host at `/data/work` is suitable for stage-1 build work, but it is missing several Milvus prerequisites out of the box
- Confirmed missing tools on `knowhere-x86-hk-proxy` as of 2026-03-27:
  - `conan`
  - `go`
  - `docker`
  - `docker-compose`

## Impact

- Missing `conan` blocks Milvus `internal/core` dependency resolution and `make build-cpp`
- Missing `go` blocks building the Milvus server binary from source
- Missing `docker` / `docker compose` blocks the planned standalone dependency lane for `etcd` and `minio`

## Evidence

- Remote tool probe returned paths for `make`, `gcc`, `g++`, `cmake`, and `python3`
- The same probe returned empty results for `conan`, `go`, `docker`, and `docker-compose`
- Remote host details confirmed during probe:
  - `Linux ecs-knowledgebase-473c 6.8.0-87-generic x86_64`
  - `Python 3.12.3`
  - `gcc 13.3.0`
  - `cmake 3.28.3`

## Stage 1 Policy

- Record missing remote prerequisites before provisioning them
- Prefer the minimum remote provisioning needed for the stage-1 lane:
  - first `conan` for `internal/core`
  - then `go` for source-built `milvus`
  - then `docker compose` only when moving from build verification to standalone smoke
