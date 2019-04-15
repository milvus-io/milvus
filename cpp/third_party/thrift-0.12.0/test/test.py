#!/usr/bin/env python3
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

#
# Apache Thrift - integration (cross) test suite
#
# tests different server-client, protocol and transport combinations
#
# This script requires python 3.x due to the improvements in
# subprocess management that are needed for reliability.
#

from __future__ import print_function
from itertools import chain
import json
import logging
import multiprocessing
import argparse
import os
import sys

import crossrunner
from crossrunner.compat import path_join

# 3.3 introduced subprocess timeouts on waiting for child
req_version = (3, 3)
cur_version = sys.version_info
assert (cur_version >= req_version), "Python 3.3 or later is required for proper operation."


ROOT_DIR = os.path.dirname(os.path.realpath(os.path.dirname(__file__)))
TEST_DIR_RELATIVE = 'test'
TEST_DIR = path_join(ROOT_DIR, TEST_DIR_RELATIVE)
FEATURE_DIR_RELATIVE = path_join(TEST_DIR_RELATIVE, 'features')
CONFIG_FILE = 'tests.json'


def run_cross_tests(server_match, client_match, jobs, skip_known_failures, only_known_failures, retry_count, regex):
    logger = multiprocessing.get_logger()
    logger.debug('Collecting tests')
    with open(path_join(TEST_DIR, CONFIG_FILE), 'r') as fp:
        j = json.load(fp)
    tests = crossrunner.collect_cross_tests(j, server_match, client_match, regex)
    if not tests:
        print('No test found that matches the criteria', file=sys.stderr)
        print('  servers: %s' % server_match, file=sys.stderr)
        print('  clients: %s' % client_match, file=sys.stderr)
        return False
    if only_known_failures:
        logger.debug('Only running known failures')
        known = crossrunner.load_known_failures(TEST_DIR)
        tests = list(filter(lambda t: crossrunner.test_name(**t) in known, tests))
    if skip_known_failures:
        logger.debug('Skipping known failures')
        known = crossrunner.load_known_failures(TEST_DIR)
        tests = list(filter(lambda t: crossrunner.test_name(**t) not in known, tests))

    dispatcher = crossrunner.TestDispatcher(TEST_DIR, ROOT_DIR, TEST_DIR_RELATIVE, jobs)
    logger.debug('Executing %d tests' % len(tests))
    try:
        for r in [dispatcher.dispatch(test, retry_count) for test in tests]:
            r.wait()
        logger.debug('Waiting for completion')
        return dispatcher.wait()
    except (KeyboardInterrupt, SystemExit):
        logger.debug('Interrupted, shutting down')
        dispatcher.terminate()
        return False


def run_feature_tests(server_match, feature_match, jobs, skip_known_failures, only_known_failures, retry_count, regex):
    basedir = path_join(ROOT_DIR, FEATURE_DIR_RELATIVE)
    logger = multiprocessing.get_logger()
    logger.debug('Collecting tests')
    with open(path_join(TEST_DIR, CONFIG_FILE), 'r') as fp:
        j = json.load(fp)
    with open(path_join(basedir, CONFIG_FILE), 'r') as fp:
        j2 = json.load(fp)
    tests = crossrunner.collect_feature_tests(j, j2, server_match, feature_match, regex)
    if not tests:
        print('No test found that matches the criteria', file=sys.stderr)
        print('  servers: %s' % server_match, file=sys.stderr)
        print('  features: %s' % feature_match, file=sys.stderr)
        return False
    if only_known_failures:
        logger.debug('Only running known failures')
        known = crossrunner.load_known_failures(basedir)
        tests = list(filter(lambda t: crossrunner.test_name(**t) in known, tests))
    if skip_known_failures:
        logger.debug('Skipping known failures')
        known = crossrunner.load_known_failures(basedir)
        tests = list(filter(lambda t: crossrunner.test_name(**t) not in known, tests))

    dispatcher = crossrunner.TestDispatcher(TEST_DIR, ROOT_DIR, FEATURE_DIR_RELATIVE, jobs)
    logger.debug('Executing %d tests' % len(tests))
    try:
        for r in [dispatcher.dispatch(test, retry_count) for test in tests]:
            r.wait()
        logger.debug('Waiting for completion')
        return dispatcher.wait()
    except (KeyboardInterrupt, SystemExit):
        logger.debug('Interrupted, shutting down')
        dispatcher.terminate()
        return False


def default_concurrenty():
    try:
        return int(os.environ.get('THRIFT_CROSSTEST_CONCURRENCY'))
    except (TypeError, ValueError):
        # Since much time is spent sleeping, use many threads
        return int(multiprocessing.cpu_count() * 1.25) + 1


def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--server', default='', nargs='*',
                        help='list of servers to test')
    parser.add_argument('--client', default='', nargs='*',
                        help='list of clients to test')
    parser.add_argument('-F', '--features', nargs='*', default=None,
                        help='run server feature tests instead of cross language tests')
    parser.add_argument('-R', '--regex', help='test name pattern to run')
    parser.add_argument('-o', '--only-known_failures', action='store_true', dest='only_known_failures',
                        help='only execute tests that are known to fail')
    parser.add_argument('-s', '--skip-known-failures', action='store_true', dest='skip_known_failures',
                        help='do not execute tests that are known to fail')
    parser.add_argument('-r', '--retry-count', type=int,
                        default=0, help='maximum retry on failure')
    parser.add_argument('-j', '--jobs', type=int,
                        default=default_concurrenty(),
                        help='number of concurrent test executions')

    g = parser.add_argument_group(title='Advanced')
    g.add_argument('-v', '--verbose', action='store_const',
                   dest='log_level', const=logging.DEBUG, default=logging.WARNING,
                   help='show debug output for test runner')
    g.add_argument('-P', '--print-expected-failures', choices=['merge', 'overwrite'],
                   dest='print_failures',
                   help="generate expected failures based on last result and print to stdout")
    g.add_argument('-U', '--update-expected-failures', choices=['merge', 'overwrite'],
                   dest='update_failures',
                   help="generate expected failures based on last result and save to default file location")
    options = parser.parse_args(argv)

    logger = multiprocessing.log_to_stderr()
    logger.setLevel(options.log_level)

    if options.features is not None and options.client:
        print('Cannot specify both --features and --client ', file=sys.stderr)
        return 1

    # Allow multiple args separated with ',' for backward compatibility
    server_match = list(chain(*[x.split(',') for x in options.server]))
    client_match = list(chain(*[x.split(',') for x in options.client]))

    if options.update_failures or options.print_failures:
        dire = path_join(ROOT_DIR, FEATURE_DIR_RELATIVE) if options.features is not None else TEST_DIR
        res = crossrunner.generate_known_failures(
            dire, options.update_failures == 'overwrite',
            options.update_failures, options.print_failures)
    elif options.features is not None:
        features = options.features or ['.*']
        res = run_feature_tests(server_match, features, options.jobs,
                                options.skip_known_failures, options.only_known_failures,
                                options.retry_count, options.regex)
    else:
        res = run_cross_tests(server_match, client_match, options.jobs,
                              options.skip_known_failures, options.only_known_failures,
                              options.retry_count, options.regex)
    return 0 if res else 1


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
