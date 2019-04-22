#!/usr/bin/env bash
# Script to report lite build binary size for latest RocksDB commits.
# Usage:
#   ./report_lite_binary_size [num_recent_commits]

num_recent_commits=${1:-10}

echo "Computing RocksDB lite build binary size for the most recent $num_recent_commits commits."

for ((i=0; i < num_recent_commits; i++))
do
  git checkout master~$i
  commit_hash=$(git show -s --format=%H)
  commit_time=$(git show -s --format=%ct)

  # It would be nice to check if scuba already have a record for the commit,
  # but sandcastle don't seems to have scuba CLI installed.

  make clean
  make OPT=-DROCKSDB_LITE static_lib

  if make OPT=-DROCKSDB_LITE static_lib
  then
    build_succeeded='true'
    strip librocksdb.a
    binary_size=$(stat -c %s librocksdb.a)
  else
    build_succeeded='false'
    binary_size=0
  fi

  current_time="\"time\": $(date +%s)"
  commit_hash="\"hash\": \"$commit_hash\""
  commit_time="\"commit_time\": $commit_time"
  build_succeeded="\"build_succeeded\": \"$build_succeeded\""
  binary_size="\"binary_size\": $binary_size"

  scribe_log="{\"int\":{$current_time, $commit_time, $binary_size}, \"normal\":{$commit_hash, $build_succeeded}}"
  echo "Logging to scribe: $scribe_log"
  scribe_cat perfpipe_rocksdb_lite_build "$scribe_log"
done
