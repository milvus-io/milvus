#!/usr/bin/env python
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

from __future__ import print_function
import lintutils
from subprocess import PIPE, STDOUT
import argparse
import multiprocessing as mp
import sys
import platform
from functools import partial


# NOTE(wesm):
#
# * readability/casting is disabled as it aggressively warns about functions
#   with names like "int32", so "int32(x)", where int32 is a function name,
#   warns with
_filters = '''
-whitespace/comments
-readability/casting
-readability/todo
-readability/alt_tokens
-build/header_guard
-build/c++11
-runtime/references
-build/include_order
'''.split()


def _get_chunk_key(filenames):
    # lists are not hashable so key on the first filename in a chunk
    return filenames[0]


def _check_some_files(completed_processes, filenames):
    # cpplint outputs complaints in '/path:line_number: complaint' format,
    # so we can scan its output to get a list of files to fix
    result = completed_processes[_get_chunk_key(filenames)]
    return lintutils.stdout_pathcolonline(result, filenames)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Runs cpplint on all of the source files.")
    parser.add_argument("--cpplint_binary",
                        required=True,
                        help="Path to the cpplint binary")
    parser.add_argument("--exclude_globs",
                        help="Filename containing globs for files "
                        "that should be excluded from the checks")
    parser.add_argument("--source_dir",
                        required=True,
                        help="Root directory of the source code")
    parser.add_argument("--quiet", default=False,
                        action="store_true",
                        help="If specified, only print errors")
    arguments = parser.parse_args()

    exclude_globs = []
    if arguments.exclude_globs:
        for line in open(arguments.exclude_globs):
            exclude_globs.append(line.strip())

    linted_filenames = []
    for path in lintutils.get_sources(arguments.source_dir, exclude_globs):
        linted_filenames.append(str(path))

    cmd = [
        arguments.cpplint_binary,
        '--verbose=2',
        '--linelength=120',
        '--filter=' + ','.join(_filters)
    ]
    if (arguments.cpplint_binary.endswith('.py') and
            platform.system() == 'Windows'):
        # Windows doesn't support executable scripts; execute with
        # sys.executable
        cmd.insert(0, sys.executable)
    if arguments.quiet:
        cmd.append('--quiet')
    else:
        print("\n".join(map(lambda x: "Linting {}".format(x),
                            linted_filenames)))

    # lint files in chunks: each invocation of cpplint will process 16 files
    chunks = lintutils.chunk(linted_filenames, 16)
    cmds = [cmd + some for some in chunks]
    results = lintutils.run_parallel(cmds, stdout=PIPE, stderr=STDOUT)

    error = False
    # record completed processes (keyed by the first filename in the input
    # chunk) for lookup in _check_some_files
    completed_processes = {
        _get_chunk_key(filenames): result
        for filenames, result in zip(chunks, results)
    }
    checker = partial(_check_some_files, completed_processes)
    pool = mp.Pool()
    try:
        # scan the outputs of various cpplint invocations in parallel to
        # distill a list of problematic files
        for problem_files, stdout in pool.imap(checker, chunks):
            if problem_files:
                if isinstance(stdout, bytes):
                    stdout = stdout.decode('utf8')
                print(stdout, file=sys.stderr)
                error = True
    except Exception:
        error = True
        raise
    finally:
        pool.terminate()
        pool.join()

    sys.exit(1 if error else 0)

