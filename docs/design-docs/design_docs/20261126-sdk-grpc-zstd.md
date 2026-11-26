# MEP: Opt-in gRPC message compression for Milvus SDKs

- **Created:** 2026-11-26
- **Author(s):** @xiaofan-luan
- **Status:** Draft
- **Component:** SDK

## Summary

Add a per-connection compression option to the Milvus SDKs. Python and C++ expose gzip from their standard gRPC runtimes. Java, Node.js and Rust support zstd. The Go SDK already supports zstd through its public dial options and a registered codec. Callers who omit the option get the existing transport defaults.

## Motivation

A batch insert can send a large protobuf request. A query can return a large response. Applications with compressible payloads can trade CPU for fewer bytes on either leg, but the public SDKs expose different levels of control over gRPC message encoding. The Milvus proxy already registers a zstd message codec. This proposal gives applications a way to use it when their endpoint supports it. The benefit depends on payload size and content. Compression can enlarge a small message. [Proxy codec registration](https://github.com/milvus-io/milvus/blob/a2dc3f1b033b4f13d57a6e81bc9749d279c928ec/internal/util/grpcclient/compression.go#L45-L55)

## Public interfaces

| SDK | Connection option | Implementation |
| --- | --- | --- |
| Python | Pass `grpc_compression="gzip"` to `MilvusClient` or `AsyncMilvusClient`. | Select gzip on the grpcio channel and include the compression setting in the connection pool key. |
| Java V2 | `ConnectConfig.builder().enableZstdCompression(true)` | Register a channel-local gRPC codec backed by zstd-jni. A channel interceptor applies the call option to both synchronous and asynchronous stubs. Database switches and global primary changes copy the setting. |
| Node.js | `new MilvusClient({ address, grpcCompression: 'zstd' })` | Use Node’s native zstd functions in the grpc-js compression filter for this client’s channels. The same option accepts `'identity'` and `'gzip'`. |
| Rust V2 | `ClientV2::new(&ConnectConfig::new().uri(uri).zstd_compression(true))` | Enable tonic’s zstd feature and apply its send and accept settings to the generated client when the connection is built or rebuilt. |
| C++ | Call `connection.WithCompression(milvus::GrpcCompression::GZIP)` before `client->Connect(connection)`. | Set the default gRPC channel compression algorithm from the connection parameters. |
| Go v3 (existing) | Pass `grpc.WithDefaultCallOptions(grpc.UseCompressor(zstd.Name))` in `ClientConfig.DialOptions`. | Import `github.com/mostynb/go-grpc-compression/nonclobbering/zstd` to register the codec at initialization. Selection applies to the connection. [Go SDK dial options](https://github.com/milvus-io/milvus/blob/9bcbf1f13351a505c65a0302f6196b13c522dc98/client/milvusclient/client.go#L98-L114) |

## Design details

The client sends the selected request algorithm in `grpc-encoding` and advertises the response algorithms it accepts in `grpc-accept-encoding`. The server chooses the response encoding. The per-message flag in the five-byte gRPC envelope determines whether that message needs decoding. For example, a runtime can send an empty request without compressing it. HTTP body compression for REST endpoints has a separate configuration path. [gRPC compression specification](https://github.com/grpc/grpc/blob/master/doc/compression.md)

Store the option with the connection configuration and carry it through pooling, reconnection and failover. Honor the transport’s message-size limits when decoding. The Java and Node.js adapters also cap the zstd history window at 8 MiB. The endpoint must support the selected request algorithm. If it rejects the encoding, surface the RPC error to the caller.

## Compatibility and packaging

- **Python and C++.** Both use the gzip codec shipped in upstream gRPC. Python users install the standard grpcio wheel. C++ builds use the existing gRPC dependency and Conan recipe. The SDK compression option does not support DEFLATE. Milvus v3.0.6 standalone rejects gRPC requests encoded with deflate during `Connect` with `UNIMPLEMENTED`.
- **Java.** The core SDK declares zstd-jni as a runtime dependency. Verify its native binaries on the platforms shipped to users and exercise the published SDK layout, which relocates gRPC classes.
- **Node.js.** The option requires native zstd on Node 22.15+, 23.8+ or 24+. Older supported runtimes can use the default transport. The adapter uses a private filter in `@grpc/grpc-js` and pins version 1.14.6. Revalidate the filter and framing before upgrading that dependency.
- **Rust.** Move from tonic 0.13 to the 0.14.5 tonic and tonic-prost runtime and generator family for bounded decompression while retaining Rust 1.86 support. Applications sharing generated types with the SDK need compatible prost 0.14 and tonic types.

## Test plan

Exercise the public connection option against an independent gRPC peer. Record the request and response encoding, inspect logical and compressed message lengths, and check the decoded RPC result. A local Go test server has exercised `Connect` and `GetVersion` across SDKs. It returns a known version string so a client must successfully decode the response. This verifies interoperability on loopback.

Cover default connections alongside opted-in connections, reconnects, sync and async paths where available, decoded-size limits, and runtime capability errors. Before release, exercise the published artifacts against supported Milvus versions and gateways with representative insert and query payloads. Measure bytes, CPU and latency on both the client and proxy before recommending compression for a workload.
