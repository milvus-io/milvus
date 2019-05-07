# Linux Kernel Patch

There are four pieces, the `xxhash` kernel module, the `zstd_compress` and `zstd_decompress` kernel modules, the BtrFS patch, and the SquashFS patch.
The patches are based off of the linux kernel master branch.

## xxHash kernel module

* The patch is located in `xxhash.diff`.
* The header is in `include/linux/xxhash.h`.
* The source is in `lib/xxhash.c`.
* `test/XXHashUserLandTest.cpp` contains tests for the patch in userland by mocking the kernel headers.
  I tested the tests by commenting a line of of each branch in `xxhash.c` one line at a time, and made sure the tests failed.
  It can be run with the following commands:
  ```
  cd test && make googletest && make XXHashUserLandTest && ./XXHashUserLandTest
  ```
* I also benchmarked the `xxhash` module against upstream xxHash, and made sure that they ran at the same speed.

## Zstd Kernel modules

* The (large) patch is located in `zstd.diff`, which depends on `xxhash.diff`.
* The header is in `include/linux/zstd.h`.
* It is split up into `zstd_compress` and `zstd_decompress`, which can be loaded independently.
* Source files are in `lib/zstd/`.
* `lib/Kconfig` and `lib/Makefile` need to be modified by applying `lib/Kconfig.diff` and `lib/Makefile.diff` respectively.
  These changes are also included in the `zstd.diff`.
* `test/UserlandTest.cpp` contains tests for the patch in userland by mocking the kernel headers.
  It can be run with the following commands:
  ```
  cd test && make googletest && make UserlandTest && ./UserlandTest
  ```

## BtrFS

* The patch is located in `btrfs.diff`.
* Additionally `fs/btrfs/zstd.c` is provided as a source for convenience.
* The patch seems to be working, it doesn't crash the kernel, and compresses at speeds and ratios that are expected.
  It could still use some more testing for fringe features, like printing options.

### Benchmarks

Benchmarks run on a Ubuntu 14.04 with 2 cores and 4 GiB of RAM.
The VM is running on a Macbook Pro with a 3.1 GHz Intel Core i7 processor,
16 GB of ram, and a SSD.
The kernel running was built from the master branch with the patch.

The compression benchmark is copying 10 copies of the
unzipped [silesia corpus](http://mattmahoney.net/dc/silesia.html) into a BtrFS
filesystem mounted with `-o compress-force={none, lzo, zlib, zstd}`.
The decompression benchmark is timing how long it takes to `tar` all 10 copies
into `/dev/null`.
The compression ratio is measured by comparing the output of `df` and `du`.
See `btrfs-benchmark.sh` for details.

| Algorithm | Compression ratio | Compression speed | Decompression speed |
|-----------|-------------------|-------------------|---------------------|
| None      | 0.99              | 504 MB/s          | 686 MB/s            |
| lzo       | 1.66              | 398 MB/s          | 442 MB/s            |
| zlib      | 2.58              | 65 MB/s           | 241 MB/s            |
| zstd 1    | 2.57              | 260 MB/s          | 383 MB/s            |
| zstd 3    | 2.71              | 174 MB/s          | 408 MB/s            |
| zstd 6    | 2.87              | 70 MB/s           | 398 MB/s            |
| zstd 9    | 2.92              | 43 MB/s           | 406 MB/s            |
| zstd 12   | 2.93              | 21 MB/s           | 408 MB/s            |
| zstd 15   | 3.01              | 11 MB/s           | 354 MB/s            |


## SquashFS

* The patch is located in `squashfs.diff`
* Additionally `fs/squashfs/zstd_wrapper.c` is provided as a source for convenience.
* The patch has been tested on the master branch of the kernel.

### Benchmarks

Benchmarks run on a Ubuntu 14.04 with 2 cores and 4 GiB of RAM.
The VM is running on a Macbook Pro with a 3.1 GHz Intel Core i7 processor,
16 GB of ram, and a SSD.
The kernel running was built from the master branch with the patch.

The compression benchmark is the file tree from the SquashFS archive found in the
Ubuntu 16.10 desktop image (ubuntu-16.10-desktop-amd64.iso).
The compression benchmark uses mksquashfs with the default block size (128 KB)
and various compression algorithms/compression levels.
`xz` and `zstd` are also benchmarked with 256 KB blocks.
The decompression benchmark is timing how long it takes to `tar` the file tree
into `/dev/null`.
See `squashfs-benchmark.sh` for details.

| Algorithm      | Compression ratio | Compression speed | Decompression speed |
|----------------|-------------------|-------------------|---------------------|
| gzip           | 2.92              |   15 MB/s         | 128 MB/s            |
| lzo            | 2.64              |  9.5 MB/s         | 217 MB/s            |
| lz4            | 2.12              |   94 MB/s         | 218 MB/s            |
| xz             | 3.43              |  5.5 MB/s         |  35 MB/s            |
| xz 256 KB      | 3.53              |  5.4 MB/s         |  40 MB/s            |
| zstd 1         | 2.71              |   96 MB/s         | 210 MB/s            |
| zstd 5         | 2.93              |   69 MB/s         | 198 MB/s            |
| zstd 10        | 3.01              |   41 MB/s         | 225 MB/s            |
| zstd 15        | 3.13              | 11.4 MB/s         | 224 MB/s            |
| zstd 16 256 KB | 3.24              |  8.1 MB/s         | 210 MB/s            |
