# Milvus Build Test Suite

This directory contains scripts and Docker configurations to test Milvus compilation across multiple platforms.

## Supported Platforms

| Platform | Architecture | Docker | Native |
|----------|--------------|--------|--------|
| Ubuntu 20.04 | amd64, arm64 | Yes | - |
| Ubuntu 22.04 | amd64, arm64 | Yes | - |
| Ubuntu 24.04 | amd64, arm64 | Yes | - |
| Rocky Linux 8 | amd64, arm64 | Yes | - |
| Rocky Linux 9 | amd64, arm64 | Yes | - |
| Amazon Linux 2023 | amd64, arm64 | Yes | - |
| macOS 12-15 | Intel, Apple Silicon | - | Yes |

## Quick Start

### Test All Linux Platforms (Docker)

```bash
cd build-test
chmod +x scripts/*.sh

# Run all tests sequentially
./scripts/run-all-tests.sh

# Run all tests in parallel (needs more resources)
./scripts/run-all-tests.sh --parallel

# Run specific platforms only
./scripts/run-all-tests.sh --platforms ubuntu2204,rocky9
```

### Test Single Platform (Docker)

```bash
# Test Ubuntu 22.04 (amd64)
./scripts/test-single-docker.sh ubuntu2204

# Test Rocky 9 on ARM64 (for Apple Silicon)
./scripts/test-single-docker.sh rocky9 --arm64
```

### Test macOS (Native)

Run this on your Mac mini or local Mac:

```bash
# Normal build
./scripts/test-macos.sh

# Clean build (removes previous build artifacts)
./scripts/test-macos.sh --clean
```

## Directory Structure

```
build-test/
├── README.md                    # This file
├── docker-compose.yml           # Docker Compose for all platforms
├── docker/
│   ├── Dockerfile.ubuntu2004    # Ubuntu 20.04
│   ├── Dockerfile.ubuntu2204    # Ubuntu 22.04
│   ├── Dockerfile.ubuntu2404    # Ubuntu 24.04
│   ├── Dockerfile.rocky8        # Rocky Linux 8
│   ├── Dockerfile.rocky9        # Rocky Linux 9
│   └── Dockerfile.amazonlinux2023  # Amazon Linux 2023
├── scripts/
│   ├── run-all-tests.sh         # Run all Docker tests
│   ├── test-single-docker.sh    # Test single Docker platform
│   └── test-macos.sh            # Test macOS native build
└── results/                     # Build logs and results
    ├── ubuntu2204.log
    ├── ubuntu2204.exit
    ├── macos14-arm64.log
    └── ...
```

## Results

After running tests, results are saved in `results/`:

- `<platform>.log` - Full build output
- `<platform>.exit` - Exit code (0 = success)

## Using Docker Compose Directly

```bash
cd build-test

# Build and test all platforms
docker compose up --build

# Build specific platform
docker compose up --build ubuntu2204

# Build ARM64 variants
docker compose up --build ubuntu2204-arm64 rocky9-arm64
```

## Requirements

- **Docker**: For Linux platform testing
- **Docker Compose**: v2.x recommended
- **macOS**: For native macOS testing (Mac mini or local Mac)
- **Disk Space**: ~50GB per platform for full build

## CI Integration

These tests can be integrated into GitHub Actions:

```yaml
jobs:
  build-test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        platform: [ubuntu2004, ubuntu2204, ubuntu2404, rocky8, rocky9]
    steps:
      - uses: actions/checkout@v4
      - name: Build and test
        run: |
          cd build-test
          ./scripts/test-single-docker.sh ${{ matrix.platform }}

  macos-test:
    runs-on: macos-14  # or macos-13, macos-12
    steps:
      - uses: actions/checkout@v4
      - name: Build and test
        run: |
          cd build-test
          ./scripts/test-macos.sh
```

## Troubleshooting

### Docker build fails with "no space left on device"

```bash
docker system prune -a
```

### ARM64 builds fail on Intel Mac

ARM64 builds require either:
- Apple Silicon Mac with Docker Desktop
- Linux with QEMU binfmt support

### macOS build fails with "llvm not found"

```bash
brew install llvm@17
```
