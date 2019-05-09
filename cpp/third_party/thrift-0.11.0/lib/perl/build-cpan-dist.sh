#!/bin/bash
#
# This script is intended to be used after tagging the repository and updating
# the version files for a release.  It will create a CPAN archive.

perl Makefile.PL
make
make manifest
make dist
