#!/bin/bash

#Download milvus source code
git clone https://github.com/milvus-io/milvus.git
cd milvus
# $1 is the branch name or commit number
git checkout $1
make
cd ..

#Prepare for milvus-deb
mkdir -p milvus-deb/milvus
mkdir milvus-deb/milvus/milvus-bin
mkdir milvus-deb/milvus/milvus-lib
## binary
cp milvus/bin/milvus milvus-deb/milvus/milvus-bin/
wget https://github.com/etcd-io/etcd/releases/download/v3.5.0/etcd-v3.5.0-linux-amd64.tar.gz && tar -xf etcd-v3.5.0-linux-amd64.tar.gz
cp etcd-v3.5.0-linux-amd64/etcd milvus-deb/milvus/milvus-bin/milvus-etcd
wget https://dl.min.io/server/minio/release/linux-amd64/archive/minio.RELEASE.2021-02-14T04-01-33Z -O milvus-deb/milvus/milvus-bin/milvus-minio
## lib
cp -d milvus/internal/core/output/lib/* milvus-deb/milvus/milvus-lib/
cp /usr/lib/x86_64-linux-gnu/libgfortran.so.4.0.0 milvus-deb/milvus/milvus-lib/libgfortran.so.4
cp /usr/lib/x86_64-linux-gnu/libgomp.so.1.0.0 milvus-deb/milvus/milvus-lib/libgomp.so.1
cp /usr/lib/x86_64-linux-gnu/libquadmath.so.0.0.0 milvus-deb/milvus/milvus-lib/libquadmath.so.0
cp /usr/lib/x86_64-linux-gnu/libtbb.so.2 milvus-deb/milvus/milvus-lib/libtbb.so.2
cp /usr/lib/libopenblas-r0.3.10.so milvus-deb/milvus/milvus-lib/libopenblas.so.0
## script
cp -r scripts milvus-deb/milvus/
## config
cp -r milvus/configs milvus-deb/milvus/

# set env
apt update
apt install gnupg pbuilder ubuntu-dev-tools apt-file dh-make build-essential -y
## $3 is name, $4 is email
bzr whoami "$3 $4"
export DEBFULLNAME="$3"
export DEBEMAIL="$4"


#Initial milvus package
cd milvus-deb
#$2 is Milvus version
tar zcf milvus-$2.tar.gz milvus
rm -rf milvus
bzr dh-make milvus $2 milvus-$2.tar.gz

##Modify debian files
sed -i '1s/unstable/bionic/' milvus/debian/changelog
sed -i "3s/(Closes: #nnnn)  <nnnn is the bug number of your ITP>/$2-1/" milvus/debian/changelog
sed -i '3,4d' milvus/debian/README.Debian
sed -i '3,5d' milvus/debian/README.source

cp ../debian/* milvus/debian/
rm -rf milvus/debian/*.ex milvus/debian/*.EX

# package milvus deb
cd milvus
bzr add debian/source/format
bzr commit -m "Initial commit of Debian packaging."
bzr builddeb -- -us -uc

#sign package and  upload to launcgpad
#bzr builddeb -S
#dput ppa:milvusdb/milvus milvus_$2-1_source.changes
