# Milvus macOS Build Profile Report

## Build Environment
- **OS**: macOS 15 (Sequoia)
- **Architecture**: arm64 (Apple Silicon)
- **Compiler**: LLVM/Clang 15
- **Build System**: Ninja
- **Build Type**: Release

## Build Time Summary

| Phase | Time | Notes |
|-------|------|-------|
| Conan Dependencies | ~3 min | Already cached |
| CMake Configuration | ~2 min | Download third-party repos |
| Tantivy (Rust) | ~2 min 17s | Compiled separately |
| C++ Core (901 targets) | ~8 min | Parallel build with Ninja |
| Go Build | ~1 min | Final binary linking |
| **Total** | **~13 min 51s** | |

## ccache Statistics
```
Cacheable calls:   10288 / 10641 (96.68%)
  Hits:             1494 / 10288 (14.52%)
    Direct:         1030 /  1494 (68.94%)
    Preprocessed:    464 /  1494 (31.06%)
  Misses:           8794 / 10288 (85.48%)
Cache size:         1.2 GB / 20.0 GB (6.13%)
```

## Build Artifacts
- `bin/milvus` - 207 MB (main binary)
- `internal/core/output/lib/` - Shared libraries

## Optimization Opportunities

### 1. Improve ccache Hit Rate (Current: 14.52%)
The low hit rate indicates this is a fresh build. Subsequent builds should see 80%+ hit rates.

**Recommendation**: Pre-populate ccache in CI environments.

### 2. Tantivy Build Integration
Currently tantivy must be built separately. Could be integrated into the main CMake build.

**Recommendation**: Fix CMake dependency ordering for tantivy_binding target.

### 3. Parallel Build Settings
Current build uses all available CPU cores via Ninja.

**Recommendation**: No changes needed - Ninja already optimizes parallelism.

### 4. Conan Package Caching
Third-party packages are cached in ~/.conan.

**Recommendation**: Use Conan remote cache for CI.

## Incremental Build Performance

After initial build, incremental builds should be much faster:
- Single file change: ~10-30 seconds
- Go code only: ~30-60 seconds
- Full C++ rebuild (cached): ~2-3 minutes

## Docker Not Available

Docker was not installed on this machine, so Linux platform tests could not be run locally.
To test on Linux:
1. Install Docker Desktop for Mac
2. Run: `cd build-test && ./scripts/run-all-tests.sh`
