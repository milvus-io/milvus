#!/bin/bash

#Change config
rm -rf milvus
cp -r /milvus .
sed -i 's#embed: false#embed: true#' milvus/configs/milvus.yaml
sed -i 's#dir: default.etcd#data.dir: /var/lib/milvus/etcd#' milvus/configs/milvus.yaml
sed -i '/data.dir: \/var\/lib\/milvus\/etcd/a \  config:\n    path: /etc/milvus/configs/embedEtcd.yaml' milvus/configs/milvus.yaml
sed -i 's#storageType: remote#storageType: local#' milvus/configs/milvus.yaml
cat << EOF > milvus/configs/embedEtcd.yaml
listen-client-urls: http://0.0.0.0:2379
advertise-client-urls: http://0.0.0.0:2379
quota-backend-bytes: 4294967296
auto-compaction-mode: revision
auto-compaction-retention: '1000'
EOF


#Prepare for milvus-deb
rm -rf milvus-deb
mkdir -p milvus-deb/milvus
mkdir milvus-deb/milvus/milvus-bin
mkdir milvus-deb/milvus/milvus-lib
## binary
cp milvus/bin/milvus milvus-deb/milvus/milvus-bin/
## lib
cp -d milvus/lib/* milvus-deb/milvus/milvus-lib/
cp /usr/lib/x86_64-linux-gnu/libgfortran.so.5.0.0 milvus-deb/milvus/milvus-lib/libgfortran.so.4
cp /usr/lib/x86_64-linux-gnu/libgomp.so.1.0.0 milvus-deb/milvus/milvus-lib/libgomp.so.1
cp /usr/lib/x86_64-linux-gnu/libquadmath.so.0.0.0 milvus-deb/milvus/milvus-lib/libquadmath.so.0
cp /usr/lib/x86_64-linux-gnu/libopenblas.so.0  milvus-deb/milvus/milvus-lib/libopenblas.so.0
## script
cp -r scripts milvus-deb/milvus/
## config
cp -r milvus/configs milvus-deb/milvus/

# set env
apt update
apt install gnupg pbuilder ubuntu-dev-tools apt-file dh-make build-essential libopenblas-openmp-dev brz-debian -y
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
dpkg-buildpackage -us -uc -ui
