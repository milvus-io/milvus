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
BENCHMARK_FILE="linux-4.11.6.tar"
BENCHMARK_DIR="$HOME/$BENCHMARK_FILE"

# Normalize the environment
sudo umount /mnt/btrfs 2> /dev/null > /dev/null || true
sudo mount -t btrfs $@ /dev/sda3 /mnt/btrfs
sudo rm -rf /mnt/btrfs/*
sync
sudo umount /mnt/btrfs
sudo mount -t btrfs $@ /dev/sda3 /mnt/btrfs

# Run the benchmark
echo "Copy"
time sh -c "sudo cp -r $BENCHMARK_DIR /mnt/btrfs/$BENCHMARK_FILE && sync"

echo "Approximate tarred compression ratio"
printf "%d / %d\n"                                                             \
  $(df /mnt/btrfs --output=used -B 1 | tail -n 1)                              \
  $(sudo du /mnt/btrfs -b -d 0 | tr '\t' '\n' | head -n 1);

# Unmount and remount to avoid any caching
sudo umount /mnt/btrfs
sudo mount -t btrfs $@ /dev/sda3 /mnt/btrfs

echo "Extract"
time sh -c "sudo tar -C /mnt/btrfs -xf /mnt/btrfs/$BENCHMARK_FILE && sync"

# Remove the tarball, leaving only the extracted data
sudo rm /mnt/btrfs/$BENCHMARK_FILE
# Unmount and remount to avoid any caching
sudo umount /mnt/btrfs
sudo mount -t btrfs $@ /dev/sda3 /mnt/btrfs

echo "Approximate extracted compression ratio"
printf "%d / %d\n"                                                             \
  $(df /mnt/btrfs --output=used -B 1 | tail -n 1)                              \
  $(sudo du /mnt/btrfs -b -d 0 | tr '\t' '\n' | head -n 1);

echo "Read"
time sudo tar -c /mnt/btrfs 2> /dev/null | wc -c > /dev/null

sudo rm -rf /mnt/btrfs/*
sudo umount /mnt/btrfs

# Run for each of -o compress-force={none, lzo, zlib, zstd} 5 times and take the
# min time and ratio.

# none
# copy: 0.981 s
# extract: 5.501 s
# read: 8.807 s
# tarball ratio: 0.97
# extracted ratio: 0.78

# lzo
# copy: 1.631 s
# extract: 8.458 s
# read: 8.585 s
# tarball ratio: 2.06
# extracted ratio: 1.38

# zlib
# copy: 7.750 s
# extract: 21.544 s
# read: 11.744 s
# tarball ratio : 3.40
# extracted ratio: 1.86

# zstd 1
# copy: 2.579 s
# extract: 11.479 s
# read: 9.389 s
# tarball ratio : 3.57
# extracted ratio: 1.85
