Zstandard library : usage examples
==================================

- [Simple compression](simple_compression.c) :
  Compress a single file.
  Introduces usage of : `ZSTD_compress()`

- [Simple decompression](simple_decompression.c) :
  Decompress a single file.
  Only compatible with simple compression.
  Result remains in memory.
  Introduces usage of : `ZSTD_decompress()`

- [Multiple simple compression](multiple_simple_compression.c) :
  Compress multiple files (in simple mode) in a single command line.
  Demonstrates memory preservation technique that
  minimizes malloc()/free() calls by re-using existing resources.
  Introduces usage of : `ZSTD_compressCCtx()`

- [Streaming memory usage](streaming_memory_usage.c) :
  Provides amount of memory used by streaming context.
  Introduces usage of : `ZSTD_sizeof_CStream()`

- [Streaming compression](streaming_compression.c) :
  Compress a single file.
  Introduces usage of : `ZSTD_compressStream()`

- [Multiple Streaming compression](multiple_streaming_compression.c) :
  Compress multiple files (in streaming mode) in a single command line.
  Introduces memory usage preservation technique,
  reducing impact of malloc()/free() and memset() by re-using existing resources.

- [Streaming decompression](streaming_decompression.c) :
  Decompress a single file compressed by zstd.
  Compatible with both simple and streaming compression.
  Result is sent to stdout.
  Introduces usage of : `ZSTD_decompressStream()`

- [Dictionary compression](dictionary_compression.c) :
  Compress multiple files using the same dictionary.
  Introduces usage of : `ZSTD_createCDict()` and `ZSTD_compress_usingCDict()`

- [Dictionary decompression](dictionary_decompression.c) :
  Decompress multiple files using the same dictionary.
  Result remains in memory.
  Introduces usage of : `ZSTD_createDDict()` and `ZSTD_decompress_usingDDict()`
