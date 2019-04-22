#!/usr/bin/env bash
#
#

if [ "$#" -lt 2 ]; then
  echo "usage: $BASH_SOURCE <DB Path> <External SST Dir>"
  exit 1
fi

db_dir=$1
external_sst_dir=$2

for f in `find $external_sst_dir -name extern_sst*`
do
  echo == Ingesting external SST file $f to DB at $db_dir
  ./ldb --db=$db_dir --create_if_missing ingest_extern_sst $f
done
