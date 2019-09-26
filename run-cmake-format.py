#!/usr/bin/env python3

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import hashlib
import pathlib
import subprocess
import sys


patterns = [
    'cpp/CMakeLists.txt',
    # Keep an explicit list of files to format as we don't want to reformat
    # files we imported from other location.
    'cpp/cmake/BuildUtils.cmake',
    'cpp/cmake/DefineOptions.cmake',
    'cpp/cmake/FindClangTools.cmake',
    'cpp/cmake/ThirdPartyPackages.cmake',
    'cpp/src/core/cmake/BuildUtilsCore.cmake',
    'cpp/src/core/cmake/DefineOptionsCore.cmake',
    'cpp/src/core/cmake/ThirdPartyPackagesCore.cmake
    'cpp/src/**/CMakeLists.txt',
    'cpp/unittest/**/CMakeLists.txt'
]

here = pathlib.Path(__file__).parent


def find_cmake_files():
    for pat in patterns:
        yield from here.glob(pat)


def run_cmake_format(paths):
    # cmake-format is fast enough that running in parallel doesn't seem
    # necessary
    # autosort is off because it breaks in cmake_format 5.1
    #   See: https://github.com/cheshirekow/cmake_format/issues/111
    cmd = ['cmake-format', '--in-place', '--autosort=false'] + paths
    try:
        subprocess.run(cmd, check=True)
    except FileNotFoundError:
        try:
            import cmake_format
        except ImportError:
            raise ImportError(
                "Please install cmake-format: `pip install cmake_format`")
        else:
            # Other error, re-raise
            raise


def check_cmake_format(paths):
    hashes = {}
    for p in paths:
        contents = p.read_bytes()
        hashes[p] = hashlib.sha256(contents).digest()

    run_cmake_format(paths)

    # Check contents didn't change
    changed = []
    for p in paths:
        contents = p.read_bytes()
        if hashes[p] != hashlib.sha256(contents).digest():
            changed.append(p)

    if changed:
        items = "\n".join("- %s" % p for p in sorted(changed))
        print("The following cmake files need re-formatting:\n%s" % (items,))
        print()
        print("Consider running `run-cmake-format.py`")
        sys.exit(1)


if __name__ == "__main__":
    paths = list(find_cmake_files())
    if "--check" in sys.argv:
        check_cmake_format(paths)
    else:
        run_cmake_format(paths)
