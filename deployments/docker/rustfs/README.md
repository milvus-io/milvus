# Milvus with RustFS

[RustFS](https://github.com/rustfs/rustfs) is a high-performance S3-compatible object storage system written in Rust. It delivers **2.3x better performance than MinIO for small objects (4KB)**, making it an excellent choice for Milvus vector database workloads.

## Features

- **S3 Compatible**: Full S3 API compatibility, drop-in replacement for MinIO
- **High Performance**: 2.3x faster than MinIO for 4KB objects
- **Optimized Concurrency**: Enhanced lock optimization and priority scheduling
- **Production Ready**: Built with Rust for memory safety and reliability

## Quick Start

### Standalone Mode

```bash
cd deployments/docker/rustfs
docker-compose -f standalone.yml up -d
```

### Cluster Mode

```bash
cd deployments/docker/rustfs
docker-compose -f cluster.yml up -d
```

## Access

### RustFS Console

- **URL**: http://localhost:9001
- **Default Credentials**: `rustfsadmin` / `rustfsadmin`

### Milvus

- **URL**: http://localhost:19530
- **Health Check**: http://localhost:9091/healthz

## Configuration

### Docker Compose Environment Variables

The Docker Compose files pre-configure the following environment variables:

| Variable | Value | Description |
|----------|-------|-------------|
| `RUSTFS_ADDRESS` | `:9000` | S3 API listen address |
| `RUSTFS_CONSOLE_ADDRESS` | `:9001` | Web console listen address |
| `RUSTFS_ACCESS_KEY` | `rustfsadmin` | S3 access key |
| `RUSTFS_SECRET_KEY` | `rustfsadmin` | S3 secret key |
| `MINIO_ADDRESS` | `rustfs:9000` | Milvus connects to RustFS |
| `MINIO_ACCESS_KEY_ID` | `rustfsadmin` | Milvus access key |
| `MINIO_SECRET_ACCESS_KEY` | `rustfsadmin` | Milvus secret key |

### Milvus Configuration

To use RustFS with Milvus, configure `minio` section in `configs/milvus.yaml`:

```yaml
minio:
  address: localhost:9000  # or rustfs:9000 in Docker
  port: 9000
  accessKeyID: rustfsadmin
  secretAccessKey: rustfsadmin
  cloudProvider: rustfs  # Use RustFS optimized implementation
```

## Performance Tuning

### Standalone Mode

Default performance settings for standalone deployments:

```yaml
RUSTFS_OBJECT_GET_TIMEOUT=30
RUSTFS_OBJECT_MAX_CONCURRENT_DISK_READS=64
RUSTFS_OBJECT_DUPLEX_BUFFER_SIZE=4194304  # 4MB
RUSTFS_RUNTIME_WORKER_THREADS=16
```

### Cluster Mode

Enhanced settings for cluster deployments:

```yaml
RUSTFS_OBJECT_MAX_CONCURRENT_DISK_READS=128
RUSTFS_OBJECT_DUPLEX_BUFFER_SIZE=8388608  # 8MB
```

## Storage Volumes

Data is persisted in Docker volumes:

- **Standalone**: `./volumes/rustfs:/data`
- **Cluster**: `${DOCKER_VOLUME_DIRECTORY:-.}/volumes/rustfs:/data`

The volume path inside the container uses the pattern: `/data/volume{1..4}`

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Docker Network                           │
│  ┌──────────────┐  ┌──────────────┐  ┌─────────────────────┐  │
│  │    etcd      │  │   RustFS     │  │      Milvus         │  │
│  │   :2379      │  │   :9000      │  │     :19530          │  │
│  │              │  │   :9001      │  │  (Proxy/Nodes)      │  │
│  └──────────────┘  └──────────────┘  └─────────────────────┘  │
│                           │                                     │
│                           │ S3 Protocol                         │
│                           ▼                                     │
│                   ┌─────────────────┐                          │
│                   │ RustFS Storage  │                          │
│                   │  /data/volume*  │                          │
│                   └─────────────────┘                          │
└─────────────────────────────────────────────────────────────────┘
```

## Logs

- **Milvus logs**: `/tmp/milvus_rustfs.log` (when using start script)
- **Docker logs**: `docker-compose logs -f rustfs` or `docker-compose logs -f standalone`

## Troubleshooting

### Check RustFS Health

```bash
curl http://localhost:9000/health
```

### Check Milvus Health

```bash
curl http://localhost:9091/healthz
```

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f rustfs
docker-compose logs -f standalone
```

## References

- [RustFS GitHub](https://github.com/rustfs/rustfs)
- [RustFS Documentation](https://rustfs.com/docs)
- [Milvus Documentation](https://milvus.io/docs)
- [Milvus Configuration](https://milvus.io/docs/configure)

## License

This integration follows the same license as Milvus (Apache 2.0).
