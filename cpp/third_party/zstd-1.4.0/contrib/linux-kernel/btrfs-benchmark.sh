# !/bin/sh
set -e

# Benchmarks run on a Ubuntu 14.04 VM with 2 cores and 4 GiB of RAM.
# The VM is running on a Macbook Pro with a 3.1 GHz Intel Core i7 processor and
# 16 GB of RAM and an SSD.

# silesia is a directory that can be downloaded from
# http://mattmahoney.net/dc/silesia.html
# ls -l silesia/
# total 203M
# -rwxr-xr-x 1 terrelln 9.8M Apr 12  2002 dickens
# -rwxr-xr-x 1 terrelln  49M May 31  2002 mozilla
# -rwxr-xr-x 1 terrelln 9.6M Mar 20  2003 mr
# -rwxr-xr-x 1 terrelln  32M Apr  2  2002 nci
# -rwxr-xr-x 1 terrelln 5.9M Jul  4  2002 ooffice
# -rwxr-xr-x 1 terrelln 9.7M Apr 11  2002 osdb
# -rwxr-xr-x 1 terrelln 6.4M Apr  2  2002 reymont
# -rwxr-xr-x 1 terrelln  21M Mar 25  2002 samba
# -rwxr-xr-x 1 terrelln 7.0M Mar 24  2002 sao
# -rwxr-xr-x 1 terrelln  40M Mar 25  2002 webster
# -rwxr-xr-x 1 terrelln 8.1M Apr  4  2002 x-ray
# -rwxr-xr-x 1 terrelln 5.1M Nov 30  2000 xml

# $HOME is on a ext4 filesystem
BENCHMARK_DIR="$HOME/silesia/"
N=10

# Normalize the environment
sudo umount /mnt/btrfs 2> /dev/null > /dev/null || true
sudo mount -t btrfs $@ /dev/sda3 /mnt/btrfs
sudo rm -rf /mnt/btrfs/*
sync
sudo umount /mnt/btrfs
sudo mount -t btrfs $@ /dev/sda3 /mnt/btrfs

# Run the benchmark
echo "Compression"
time sh -c "for i in \$(seq $N); do sudo cp -r $BENCHMARK_DIR /mnt/btrfs/\$i; done; sync"

echo "Approximate compression ratio"
printf "%d / %d\n"                                                             \
  $(df /mnt/btrfs --output=used -B 1 | tail -n 1)                              \
  $(sudo du /mnt/btrfs -b -d 0 | tr '\t' '\n' | head -n 1);

# Unmount and remount to avoid any caching
sudo umount /mnt/btrfs
sudo mount -t btrfs $@ /dev/sda3 /mnt/btrfs

echo "Decompression"
time sudo tar -c /mnt/btrfs 2> /dev/null | wc -c > /dev/null

sudo rm -rf /mnt/btrfs/*
sudo umount /mnt/btrfs

# Run for each of -o compress-force={none, lzo, zlib, zstd} 5 times and take the
# min time and ratio.
# Ran zstd with compression levels {1, 3, 6, 9, 12, 15}.
# Original size: 2119415342 B (using du /mnt/btrfs)

# none
# compress: 4.205 s
# decompress: 3.090 s
# ratio: 0.99

# lzo
# compress: 5.328 s
# decompress: 4.793 s
# ratio: 1.66

# zlib
# compress: 32.588 s
# decompress: 8.791 s
# ratio : 2.58

# zstd 1
# compress: 8.147 s
# decompress: 5.527 s
# ratio : 2.57

# zstd 3
# compress: 12.207 s
# decompress: 5.195 s
# ratio : 2.71

# zstd 6
# compress: 30.253 s
# decompress: 5.324 s
# ratio : 2.87

# zstd 9
# compress: 49.659 s
# decompress: 5.220 s
# ratio : 2.92

# zstd 12
# compress: 99.245 s
# decompress: 5.193 s
# ratio : 2.93

# zstd 15
# compress: 196.997 s
# decompress: 5.992 s
# ratio : 3.01
