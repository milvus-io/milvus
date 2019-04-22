#  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

'''Filter for error messages in test output:
    - Receives merged stdout/stderr from test on stdin
    - Finds patterns of known error messages for test name (first argument)
    - Prints those error messages to stdout
'''

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import re
import sys


class ErrorParserBase(object):
    def parse_error(self, line):
        '''Parses a line of test output. If it contains an error, returns a
        formatted message describing the error; otherwise, returns None.
        Subclasses must override this method.
        '''
        raise NotImplementedError


class GTestErrorParser(ErrorParserBase):
    '''A parser that remembers the last test that began running so it can print
    that test's name upon detecting failure.
    '''
    _GTEST_NAME_PATTERN = re.compile(r'\[ RUN      \] (\S+)$')
    # format: '<filename or "unknown file">:<line #>: Failure'
    _GTEST_FAIL_PATTERN = re.compile(r'(unknown file|\S+:\d+): Failure$')

    def __init__(self):
        self._last_gtest_name = 'Unknown test'

    def parse_error(self, line):
        gtest_name_match = self._GTEST_NAME_PATTERN.match(line)
        if gtest_name_match:
            self._last_gtest_name = gtest_name_match.group(1)
            return None
        gtest_fail_match = self._GTEST_FAIL_PATTERN.match(line)
        if gtest_fail_match:
            return '%s failed: %s' % (
                    self._last_gtest_name, gtest_fail_match.group(1))
        return None


class MatchErrorParser(ErrorParserBase):
    '''A simple parser that returns the whole line if it matches the pattern.
    '''
    def __init__(self, pattern):
        self._pattern = re.compile(pattern)

    def parse_error(self, line):
        if self._pattern.match(line):
            return line
        return None


class CompilerErrorParser(MatchErrorParser):
    def __init__(self):
        # format (compile error):
        #   '<filename>:<line #>:<column #>: error: <error msg>'
        # format (link error):
        #   '<filename>:<line #>: error: <error msg>'
        # The below regex catches both
        super(CompilerErrorParser, self).__init__(r'\S+:\d+: error:')


class ScanBuildErrorParser(MatchErrorParser):
    def __init__(self):
        super(ScanBuildErrorParser, self).__init__(
                r'scan-build: \d+ bugs found.$')


class DbCrashErrorParser(MatchErrorParser):
    def __init__(self):
        super(DbCrashErrorParser, self).__init__(r'\*\*\*.*\^$|TEST FAILED.')


class WriteStressErrorParser(MatchErrorParser):
    def __init__(self):
        super(WriteStressErrorParser, self).__init__(
                r'ERROR: write_stress died with exitcode=\d+')


class AsanErrorParser(MatchErrorParser):
    def __init__(self):
        super(AsanErrorParser, self).__init__(
                r'==\d+==ERROR: AddressSanitizer:')


class UbsanErrorParser(MatchErrorParser):
    def __init__(self):
        # format: '<filename>:<line #>:<column #>: runtime error: <error msg>'
        super(UbsanErrorParser, self).__init__(r'\S+:\d+:\d+: runtime error:')


class ValgrindErrorParser(MatchErrorParser):
    def __init__(self):
        # just grab the summary, valgrind doesn't clearly distinguish errors
        # from other log messages.
        super(ValgrindErrorParser, self).__init__(r'==\d+== ERROR SUMMARY:')


class CompatErrorParser(MatchErrorParser):
    def __init__(self):
        super(CompatErrorParser, self).__init__(r'==== .*[Ee]rror.* ====$')


class TsanErrorParser(MatchErrorParser):
    def __init__(self):
        super(TsanErrorParser, self).__init__(r'WARNING: ThreadSanitizer:')


_TEST_NAME_TO_PARSERS = {
    'punit': [CompilerErrorParser, GTestErrorParser],
    'unit': [CompilerErrorParser, GTestErrorParser],
    'release': [CompilerErrorParser, GTestErrorParser],
    'unit_481': [CompilerErrorParser, GTestErrorParser],
    'release_481': [CompilerErrorParser, GTestErrorParser],
    'clang_unit': [CompilerErrorParser, GTestErrorParser],
    'clang_release': [CompilerErrorParser, GTestErrorParser],
    'clang_analyze': [CompilerErrorParser, ScanBuildErrorParser],
    'code_cov': [CompilerErrorParser, GTestErrorParser],
    'unity': [CompilerErrorParser, GTestErrorParser],
    'lite': [CompilerErrorParser],
    'lite_test': [CompilerErrorParser, GTestErrorParser],
    'stress_crash': [CompilerErrorParser, DbCrashErrorParser],
    'stress_crash_with_atomic_flush': [CompilerErrorParser, DbCrashErrorParser],
    'write_stress': [CompilerErrorParser, WriteStressErrorParser],
    'asan': [CompilerErrorParser, GTestErrorParser, AsanErrorParser],
    'asan_crash': [CompilerErrorParser, AsanErrorParser, DbCrashErrorParser],
    'asan_crash_with_atomic_flush': [CompilerErrorParser, AsanErrorParser, DbCrashErrorParser],
    'ubsan': [CompilerErrorParser, GTestErrorParser, UbsanErrorParser],
    'ubsan_crash': [CompilerErrorParser, UbsanErrorParser, DbCrashErrorParser],
    'ubsan_crash_with_atomic_flush': [CompilerErrorParser, UbsanErrorParser, DbCrashErrorParser],
    'valgrind': [CompilerErrorParser, GTestErrorParser, ValgrindErrorParser],
    'tsan': [CompilerErrorParser, GTestErrorParser, TsanErrorParser],
    'format_compatible': [CompilerErrorParser, CompatErrorParser],
    'run_format_compatible': [CompilerErrorParser, CompatErrorParser],
    'no_compression': [CompilerErrorParser, GTestErrorParser],
    'run_no_compression': [CompilerErrorParser, GTestErrorParser],
    'regression': [CompilerErrorParser],
    'run_regression': [CompilerErrorParser],
}


def main():
    if len(sys.argv) != 2:
        return 'Usage: %s <test name>' % sys.argv[0]
    test_name = sys.argv[1]
    if test_name not in _TEST_NAME_TO_PARSERS:
        return 'Unknown test name: %s' % test_name

    error_parsers = []
    for parser_cls in _TEST_NAME_TO_PARSERS[test_name]:
        error_parsers.append(parser_cls())

    for line in sys.stdin:
        line = line.strip()
        for error_parser in error_parsers:
            error_msg = error_parser.parse_error(line)
            if error_msg is not None:
                print(error_msg)


if __name__ == '__main__':
    sys.exit(main())
