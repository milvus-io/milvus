#!/usr/bin/env bash
#
#
#

if [ "$#" -lt 3 ]; then
  echo "usage: $BASH_SOURCE <input_data_path> <DB Path> <extern SST dir>"
  exit 1
fi

input_data_dir=$1
db_dir=$2
extern_sst_dir=$3
rm -rf $db_dir

set -e

n=0

for f in `find $input_data_dir -name sorted_data*`
do
  echo == Writing external SST file $f to $extern_sst_dir/extern_sst${n}
  ./ldb --db=$db_dir --create_if_missing write_extern_sst $extern_sst_dir/extern_sst${n} < $f
  let "n = n + 1"
done
