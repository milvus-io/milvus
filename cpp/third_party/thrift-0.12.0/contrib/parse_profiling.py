#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""
This script can be used to make the output from
apache::thrift::profile_print_info() more human-readable.

It translates each executable file name and address into the corresponding
source file name, line number, and function name.  By default, it also
demangles C++ symbol names.
"""

import optparse
import os
import re
import subprocess
import sys


class AddressInfo(object):
    """
    A class to store information about a particular address in an object file.
    """
    def __init__(self, obj_file, address):
        self.objectFile = obj_file
        self.address = address
        self.sourceFile = None
        self.sourceLine = None
        self.function = None


g_addrs_by_filename = {}


def get_address(filename, address):
    """
    Retrieve an AddressInfo object for the specified object file and address.

    Keeps a global list of AddressInfo objects.  Two calls to get_address()
    with the same filename and address will always return the same AddressInfo
    object.
    """
    global g_addrs_by_filename
    try:
        by_address = g_addrs_by_filename[filename]
    except KeyError:
        by_address = {}
        g_addrs_by_filename[filename] = by_address

    try:
        addr_info = by_address[address]
    except KeyError:
        addr_info = AddressInfo(filename, address)
        by_address[address] = addr_info
    return addr_info


def translate_file_addresses(filename, addresses, options):
    """
    Use addr2line to look up information for the specified addresses.
    All of the addresses must belong to the same object file.
    """
    # Do nothing if we can't find the file
    if not os.path.isfile(filename):
        return

    args = ['addr2line']
    if options.printFunctions:
        args.append('-f')
    args.extend(['-e', filename])

    proc = subprocess.Popen(args, stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE)
    for address in addresses:
        assert address.objectFile == filename
        proc.stdin.write(address.address + '\n')

        if options.printFunctions:
            function = proc.stdout.readline()
            function = function.strip()
            if not function:
                raise Exception('unexpected EOF from addr2line')
            address.function = function

        file_and_line = proc.stdout.readline()
        file_and_line = file_and_line.strip()
        if not file_and_line:
            raise Exception('unexpected EOF from addr2line')
        idx = file_and_line.rfind(':')
        if idx < 0:
            msg = 'expected file and line number from addr2line; got %r' % \
                (file_and_line,)
            msg += '\nfile=%r, address=%r' % (filename, address.address)
            raise Exception(msg)

        address.sourceFile = file_and_line[:idx]
        address.sourceLine = file_and_line[idx + 1:]

    (remaining_out, cmd_err) = proc.communicate()
    retcode = proc.wait()
    if retcode != 0:
        raise subprocess.CalledProcessError(retcode, args)


def lookup_addresses(options):
    """
    Look up source file information for all of the addresses currently stored
    in the global list of AddressInfo objects.
    """
    global g_addrs_by_filename
    for (file, addresses) in g_addrs_by_filename.items():
        translate_file_addresses(file, addresses.values(), options)


class Entry(object):
    """
    An entry in the thrift profile output.
    Contains a header line, and a backtrace.
    """
    def __init__(self, header):
        self.header = header
        self.bt = []

    def addFrame(self, filename, address):
        # If libc was able to determine the symbols names, the filename
        # argument will be of the form <filename>(<function>+<offset>)
        # So, strip off anything after the last '('
        idx = filename.rfind('(')
        if idx >= 0:
            filename = filename[:idx]

        addr = get_address(filename, address)
        self.bt.append(addr)

    def write(self, f, options):
        f.write(self.header)
        f.write('\n')
        n = 0
        for address in self.bt:
            f.write('  #%-2d %s:%s\n' % (n, address.sourceFile,
                                         address.sourceLine))
            n += 1
            if options.printFunctions:
                if address.function:
                    f.write('      %s\n' % (address.function,))
                else:
                    f.write('      ??\n')


def process_file(in_file, out_file, options):
    """
    Read thrift profile output from the specified input file, and print
    prettier information on the output file.
    """
    #
    # A naive approach would be to read the input line by line,
    # and each time we come to a filename and address, pass it to addr2line
    # and print the resulting information.  Unfortunately, addr2line can be
    # quite slow, especially with large executables.
    #
    # This approach is much faster.  We read in all of the input, storing
    # the addresses in each file that need to be resolved.  We then call
    # addr2line just once for each file.  This is much faster than calling
    # addr2line once per address.
    #

    virt_call_regex = re.compile(r'^\s*T_VIRTUAL_CALL: (\d+) calls on (.*):$')
    gen_prot_regex = re.compile(
        r'^\s*T_GENERIC_PROTOCOL: (\d+) calls to (.*) with a (.*):$')
    bt_regex = re.compile(r'^\s*#(\d+)\s*(.*) \[(0x[0-9A-Za-z]+)\]$')

    # Parse all of the input, and store it as Entry objects
    entries = []
    current_entry = None
    while True:
        line = in_file.readline()
        if not line:
            break

        if line == '\n' or line.startswith('Thrift virtual call info:'):
            continue

        virt_call_match = virt_call_regex.match(line)
        if virt_call_match:
            num_calls = int(virt_call_match.group(1))
            type_name = virt_call_match.group(2)
            if options.cxxfilt:
                # Type names reported by typeid() are internal names.
                # By default, c++filt doesn't demangle internal type names.
                # (Some versions of c++filt have a "-t" option to enable this.
                # Other versions don't have this argument, but demangle type
                # names passed as an argument, but not on stdin.)
                #
                # If the output is being filtered through c++filt, prepend
                # "_Z" to the type name to make it look like an external name.
                type_name = '_Z' + type_name
            header = 'T_VIRTUAL_CALL: %d calls on "%s"' % \
                (num_calls, type_name)
            if current_entry is not None:
                entries.append(current_entry)
            current_entry = Entry(header)
            continue

        gen_prot_match = gen_prot_regex.match(line)
        if gen_prot_match:
            num_calls = int(gen_prot_match.group(1))
            type_name1 = gen_prot_match.group(2)
            type_name2 = gen_prot_match.group(3)
            if options.cxxfilt:
                type_name1 = '_Z' + type_name1
                type_name2 = '_Z' + type_name2
            header = 'T_GENERIC_PROTOCOL: %d calls to "%s" with a "%s"' % \
                (num_calls, type_name1, type_name2)
            if current_entry is not None:
                entries.append(current_entry)
            current_entry = Entry(header)
            continue

        bt_match = bt_regex.match(line)
        if bt_match:
            if current_entry is None:
                raise Exception('found backtrace frame before entry header')
            frame_num = int(bt_match.group(1))
            filename = bt_match.group(2)
            address = bt_match.group(3)
            current_entry.addFrame(filename, address)
            continue

        raise Exception('unexpected line in input: %r' % (line,))

    # Add the last entry we were processing to the list
    if current_entry is not None:
        entries.append(current_entry)
        current_entry = None

    # Look up all of the addresses
    lookup_addresses(options)

    # Print out the entries, now that the information has been translated
    for entry in entries:
        entry.write(out_file, options)
        out_file.write('\n')


def start_cppfilt():
    (read_pipe, write_pipe) = os.pipe()

    # Fork.  Run c++filt in the parent process,
    # and then continue normal processing in the child.
    pid = os.fork()
    if pid == 0:
        # child
        os.dup2(write_pipe, sys.stdout.fileno())
        os.close(read_pipe)
        os.close(write_pipe)
        return
    else:
        # parent
        os.dup2(read_pipe, sys.stdin.fileno())
        os.close(read_pipe)
        os.close(write_pipe)

        cmd = ['c++filt']
        os.execvp(cmd[0], cmd)


def main(argv):
    parser = optparse.OptionParser(usage='%prog [options] [<file>]')
    parser.add_option('--no-functions', help='Don\'t print function names',
                      dest='printFunctions', action='store_false',
                      default=True)
    parser.add_option('--no-demangle',
                      help='Don\'t demangle C++ symbol names',
                      dest='cxxfilt', action='store_false',
                      default=True)

    (options, args) = parser.parse_args(argv[1:])
    num_args = len(args)
    if num_args == 0:
        in_file = sys.stdin
    elif num_args == 1:
        in_file = open(argv[1], 'r')
    else:
        parser.print_usage(sys.stderr)
        print >> sys.stderr, 'trailing arguments: %s' % (' '.join(args[1:],))
        return 1

    if options.cxxfilt:
        start_cppfilt()

    process_file(in_file, sys.stdout, options)


if __name__ == '__main__':
    rc = main(sys.argv)
    sys.exit(rc)
