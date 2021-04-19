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

import multiprocessing as mp
import os
from fnmatch import fnmatch
from subprocess import Popen


def chunk(seq, n):
    """
    divide a sequence into equal sized chunks
    (the last chunk may be smaller, but won't be empty)
    """
    chunks = []
    some = []
    for element in seq:
        if len(some) == n:
            chunks.append(some)
            some = []
        some.append(element)
    if len(some) > 0:
        chunks.append(some)
    return chunks


def dechunk(chunks):
    "flatten chunks into a single list"
    seq = []
    for chunk in chunks:
        seq.extend(chunk)
    return seq


def run_parallel(cmds, **kwargs):
    """
    Run each of cmds (with shared **kwargs) using subprocess.Popen
    then wait for all of them to complete.
    Runs batches of multiprocessing.cpu_count() * 2 from cmds
    returns a list of tuples containing each process'
    returncode, stdout, stderr
    """
    complete = []
    for cmds_batch in chunk(cmds, mp.cpu_count() * 2):
        procs_batch = [Popen(cmd, **kwargs) for cmd in cmds_batch]
        for proc in procs_batch:
            stdout, stderr = proc.communicate()
            complete.append((proc.returncode, stdout, stderr))
    return complete


_source_extensions = '''
.h
.cc
.cpp
'''.split()


def get_sources(source_dir, exclude_globs=[]):
    sources = []
    for directory, subdirs, basenames in os.walk(source_dir):
        for path in [os.path.join(directory, basename)
                     for basename in basenames]:
            # filter out non-source files
            if os.path.splitext(path)[1] not in _source_extensions:
                continue

            path = os.path.abspath(path)

            # filter out files that match the globs in the globs file
            if any([fnmatch(path, glob) for glob in exclude_globs]):
                continue

            sources.append(path)
    return sources


def stdout_pathcolonline(completed_process, filenames):
    """
    given a completed process which may have reported some files as problematic
    by printing the path name followed by ':' then a line number, examine
    stdout and return the set of actually reported file names
    """
    returncode, stdout, stderr = completed_process
    bfilenames = set()
    for filename in filenames:
        bfilenames.add(filename.encode('utf-8') + b':')
    problem_files = set()
    for line in stdout.splitlines():
        for filename in bfilenames:
            if line.startswith(filename):
                problem_files.add(filename.decode('utf-8'))
                bfilenames.remove(filename)
                break
    return problem_files, stdout

