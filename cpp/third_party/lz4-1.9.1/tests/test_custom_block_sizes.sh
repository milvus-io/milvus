#/usr/bin/env sh
set -e

LZ4=../lz4
CHECKFRAME=./checkFrame
DATAGEN=./datagen

failures=""

TMPFILE=/tmp/test_custom_block_sizes.$$
TMPFILE1=/tmp/test_custom_block_sizes1.$$
TMPFILE2=/tmp/test_custom_block_sizes2.$$
$DATAGEN -g12345678 > $TMPFILE1
$DATAGEN -g12345678 > $TMPFILE2

echo Testing -B31
$LZ4 -f -B31 $TMPFILE1 && failures="31 (should fail) "

for blocksize in 32 65535 65536
do
  echo Testing -B$blocksize
  $LZ4 -f -B$blocksize $TMPFILE1
  $LZ4 -f -B$blocksize $TMPFILE2
  cat $TMPFILE1.lz4 $TMPFILE2.lz4 > $TMPFILE.lz4
  $CHECKFRAME -B$blocksize -b4 $TMPFILE.lz4 || failures="$failures $blocksize "
done

for blocksize in 65537 262143 262144
do
  echo Testing -B$blocksize
  $LZ4 -f -B$blocksize $TMPFILE1
  $LZ4 -f -B$blocksize $TMPFILE2
  cat $TMPFILE1.lz4 $TMPFILE2.lz4 > $TMPFILE.lz4
  $CHECKFRAME -B$blocksize -b5 $TMPFILE.lz4 || failures="$failures $blocksize "
done

for blocksize in 262145 1048575 1048576
do
  echo Testing -B$blocksize
  $LZ4 -f -B$blocksize $TMPFILE1
  $LZ4 -f -B$blocksize $TMPFILE2
  cat $TMPFILE1.lz4 $TMPFILE2.lz4 > $TMPFILE.lz4
  $CHECKFRAME -B$blocksize -b6 $TMPFILE.lz4 || failures="$failures $blocksize "
done

for blocksize in 1048577 4194303 4194304
do
  echo Testing -B$blocksize
  $LZ4 -f -B$blocksize $TMPFILE1
  $LZ4 -f -B$blocksize $TMPFILE2
  cat $TMPFILE1.lz4 $TMPFILE2.lz4 > $TMPFILE.lz4
  $CHECKFRAME -B$blocksize -b7 $TMPFILE.lz4 || failures="$failures $blocksize "
done

for blocksize in 4194305 10485760
do
  echo Testing -B$blocksize
  $LZ4 -f -B$blocksize $TMPFILE1
  $LZ4 -f -B$blocksize $TMPFILE2
  cat $TMPFILE1.lz4 $TMPFILE2.lz4 > $TMPFILE.lz4
  $CHECKFRAME -B4194304 -b7 $TMPFILE.lz4 || failures="$failures $blocksize "
done

rm $TMPFILE.lz4 $TMPFILE1 $TMPFILE1.lz4 $TMPFILE2 $TMPFILE2.lz4
if [ "$failures" == "" ]
then
  echo ---- All tests passed
  exit 0
else
  echo ---- The following tests had failures: $failures
  exit 1
fi
