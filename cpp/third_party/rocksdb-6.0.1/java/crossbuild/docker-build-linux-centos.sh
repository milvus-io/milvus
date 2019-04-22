#!/usr/bin/env bash

set -e
#set -x

rm -rf /rocksdb-local
cp -r /rocksdb-host /rocksdb-local
cd /rocksdb-local

# Use scl devtoolset if available (i.e. CentOS <7)
if hash scl 2>/dev/null; then
	if scl --list | grep -q 'devtoolset-7'; then
		scl enable devtoolset-7 'make jclean clean'
		scl enable devtoolset-7 'PORTABLE=1 make -j6 rocksdbjavastatic'
	elif scl --list | grep -q 'devtoolset-2'; then
		scl enable devtoolset-2 'make jclean clean'
		scl enable devtoolset-2 'PORTABLE=1 make -j6 rocksdbjavastatic'
	else
		echo "Could not find devtoolset"
		exit 1;
	fi
else
	make jclean clean
        PORTABLE=1 make -j6 rocksdbjavastatic
fi

cp java/target/librocksdbjni-linux*.so java/target/rocksdbjni-*-linux*.jar /rocksdb-host/java/target

