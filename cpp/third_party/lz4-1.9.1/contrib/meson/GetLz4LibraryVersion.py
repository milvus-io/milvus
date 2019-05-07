#!/usr/bin/env python3
# #############################################################################
# Copyright (c) 2018-present    lzutao <taolzu(at)gmail.com>
# All rights reserved.
#
# This source code is licensed under both the BSD-style license (found in the
# LICENSE file in the root directory of this source tree) and the GPLv2 (found
# in the COPYING file in the root directory of this source tree).
# #############################################################################
import re


def find_version_tuple(filepath):
  version_file_data = None
  with open(filepath) as fd:
    version_file_data = fd.read()

  patterns = r"""#\s*define\s+LZ4_VERSION_MAJOR\s+([0-9]+).*$
#\s*define\s+LZ4_VERSION_MINOR\s+([0-9]+).*$
#\s*define\s+LZ4_VERSION_RELEASE\s+([0-9]+).*$
"""
  regex = re.compile(patterns, re.MULTILINE)
  version_match = regex.search(version_file_data)
  if version_match:
    return version_match.groups()
  raise Exception("Unable to find version string.")


def main():
  import argparse
  parser = argparse.ArgumentParser(description='Print lz4 version from lib/lz4.h')
  parser.add_argument('file', help='path to lib/lz4.h')
  args = parser.parse_args()
  version_tuple = find_version_tuple(args.file)
  print('.'.join(version_tuple))


if __name__ == '__main__':
  main()
