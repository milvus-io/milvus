# shellcheck disable=SC2148
TESTDIR=`mktemp -d ${TMPDIR:-/tmp}/rocksdb-dump-test.XXXXX`
DUMPFILE="tools/sample-dump.dmp"

# Verify that the sample dump file is undumpable and then redumpable.
./rocksdb_undump --dump_location=$DUMPFILE --db_path=$TESTDIR/db
./rocksdb_dump --anonymous --db_path=$TESTDIR/db --dump_location=$TESTDIR/dump
cmp $DUMPFILE $TESTDIR/dump
