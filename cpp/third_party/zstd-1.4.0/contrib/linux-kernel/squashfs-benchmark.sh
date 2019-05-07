# !/bin/sh
set -e

# Benchmarks run on a Ubuntu 14.04 VM with 2 cores and 4 GiB of RAM.
# The VM is running on a Macbook Pro with a 3.1 GHz Intel Core i7 processor and
# 16 GB of RAM and an SSD.

# $BENCHMARK_DIR is generated with the following commands, from the Ubuntu image
# ubuntu-16.10-desktop-amd64.iso.
# > mkdir mnt
# > sudo mount -o loop ubuntu-16.10-desktop-amd64.iso mnt
# > cp mnt/casper/filesystem.squashfs .
# > sudo unsquashfs filesystem.squashfs

# $HOME is on a ext4 filesystem
BENCHMARK_DIR="$HOME/squashfs-root/"
BENCHMARK_FS="$HOME/filesystem.squashfs"

# Normalize the environment
sudo rm -f $BENCHMARK_FS 2> /dev/null > /dev/null || true
sudo umount /mnt/squashfs 2> /dev/null > /dev/null || true

# Run the benchmark
echo "Compression"
echo "sudo mksquashfs $BENCHMARK_DIR $BENCHMARK_FS $@"
time sudo mksquashfs $BENCHMARK_DIR $BENCHMARK_FS $@ 2> /dev/null > /dev/null

echo "Approximate compression ratio"
printf "%d / %d\n"                                                             \
  $(sudo du -sx --block-size=1 $BENCHMARK_DIR | cut -f1)                       \
  $(sudo du -sx --block-size=1 $BENCHMARK_FS  | cut -f1);

# Mount the filesystem
sudo mount -t squashfs $BENCHMARK_FS /mnt/squashfs

echo "Decompression"
time sudo tar -c /mnt/squashfs 2> /dev/null | wc -c > /dev/null

sudo umount /mnt/squashfs
