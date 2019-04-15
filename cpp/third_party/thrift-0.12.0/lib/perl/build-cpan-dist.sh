#!/bin/bash
#
# This script is intended to be used after tagging the repository and updating
# the version files for a release.  It will create a CPAN archive.  Run this
# from inside a docker image like ubuntu-xenial.
#

set -e

rm -f MANIFEST
rm -rf Thrift-*

# setup cpan without a prompt
echo | cpan
cpan install HTTP::Date
cpan install CPAN
cpan install CPAN::Meta ExtUtils::MakeMaker JSON::PP

perl Makefile.PL
rm MYMETA.yml
make manifest
make dist

#
# We unpack the archive so we can add version metadata for CPAN
# so that it properly indexes Thrift and remove unnecessary files.
#

echo '-----------------------------------------------------------'
set -x

DISTFILE=$(ls Thrift*.gz)
NEWFILE=${DISTFILE/t-v/t-}
if [[ "$DISTFILE" != "$NEWFILE" ]]; then
    mv $DISTFILE $NEWFILE
    DISTFILE="$NEWFILE"
fi
tar xzf $DISTFILE
rm $DISTFILE
DISTDIR=$(ls -d Thrift*)
# cpan doesn't like "Thrift-v0.nn.0 as a directory name
# needs to be Thrift-0.nn.0
NEWDIR=${DISTDIR/t-v/t-}
if [[ "$DISTDIR" != "$NEWDIR" ]]; then
    mv $DISTDIR $NEWDIR
    DISTDIR="$NEWDIR"
fi
cd $DISTDIR
cp -p ../Makefile.PL .
perl ../tools/FixupDist.pl
cd ..
tar cvzf $DISTFILE $DISTDIR
rm -r $DISTDIR
