#!/usr/bin/python

# amalgamate.py creates an amalgamation from a unity build.
# It can be run with either Python 2 or 3.
# An amalgamation consists of a header that includes the contents of all public
# headers and a source file that includes the contents of all source files and
# private headers.
#
# This script works by starting with the unity build file and recursively expanding
# #include directives. If the #include is found in a public include directory,
# that header is expanded into the amalgamation header.
#
# A particular header is only expanded once, so this script will
# break if there are multiple inclusions of the same header that are expected to
# expand differently. Similarly, this type of code causes issues:
#
# #ifdef FOO
#   #include "bar.h"
#   // code here
# #else
#   #include "bar.h"            // oops, doesn't get expanded
#   // different code here
# #endif
#
# The solution is to move the include out of the #ifdef.

from __future__ import print_function

import argparse
from os import path
import re
import sys

include_re = re.compile('^[ \t]*#include[ \t]+"(.*)"[ \t]*$')
included = set()
excluded = set()

def find_header(name, abs_path, include_paths):
    samedir = path.join(path.dirname(abs_path), name)
    if path.exists(samedir):
        return samedir
    for include_path in include_paths:
        include_path = path.join(include_path, name)
        if path.exists(include_path):
            return include_path
    return None

def expand_include(include_path, f, abs_path, source_out, header_out, include_paths, public_include_paths):
    if include_path in included:
        return False

    included.add(include_path)
    with open(include_path) as f:
        print('#line 1 "{}"'.format(include_path), file=source_out)
        process_file(f, include_path, source_out, header_out, include_paths, public_include_paths)
    return True

def process_file(f, abs_path, source_out, header_out, include_paths, public_include_paths):
    for (line, text) in enumerate(f):
        m = include_re.match(text)
        if m:
            filename = m.groups()[0]
            # first check private headers
            include_path = find_header(filename, abs_path, include_paths)
            if include_path:
                if include_path in excluded:
                    source_out.write(text)
                    expanded = False
                else:
                    expanded = expand_include(include_path, f, abs_path, source_out, header_out, include_paths, public_include_paths)
            else:
                # now try public headers
                include_path = find_header(filename, abs_path, public_include_paths)
                if include_path:
                    # found public header
                    expanded = False
                    if include_path in excluded:
                        source_out.write(text)
                    else:
                        expand_include(include_path, f, abs_path, header_out, None, public_include_paths, [])
                else:
                    sys.exit("unable to find {}, included in {} on line {}".format(filename, abs_path, line))

            if expanded:
                print('#line {} "{}"'.format(line+1, abs_path), file=source_out)
        elif text != "#pragma once\n":
            source_out.write(text)

def main():
    parser = argparse.ArgumentParser(description="Transform a unity build into an amalgamation")
    parser.add_argument("source", help="source file")
    parser.add_argument("-I", action="append", dest="include_paths", help="include paths for private headers")
    parser.add_argument("-i", action="append", dest="public_include_paths", help="include paths for public headers")
    parser.add_argument("-x", action="append", dest="excluded", help="excluded header files")
    parser.add_argument("-o", dest="source_out", help="output C++ file", required=True)
    parser.add_argument("-H", dest="header_out", help="output C++ header file", required=True)
    args = parser.parse_args()

    include_paths = list(map(path.abspath, args.include_paths or []))
    public_include_paths = list(map(path.abspath, args.public_include_paths or []))
    excluded.update(map(path.abspath, args.excluded or []))
    filename = args.source
    abs_path = path.abspath(filename)
    with open(filename) as f, open(args.source_out, 'w') as source_out, open(args.header_out, 'w') as header_out:
        print('#line 1 "{}"'.format(filename), file=source_out)
        print('#include "{}"'.format(header_out.name), file=source_out)
        process_file(f, abs_path, source_out, header_out, include_paths, public_include_paths)

if __name__ == "__main__":
    main()
