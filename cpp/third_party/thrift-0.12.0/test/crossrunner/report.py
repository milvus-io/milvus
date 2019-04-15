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

from __future__ import print_function
import datetime
import json
import multiprocessing
import os
import platform
import re
import subprocess
import sys
import time
import traceback

from .compat import logfile_open, path_join, str_join
from .test import TestEntry

LOG_DIR = 'log'
RESULT_HTML = 'index.html'
RESULT_JSON = 'results.json'
FAIL_JSON = 'known_failures_%s.json'


def generate_known_failures(testdir, overwrite, save, out):
    def collect_failures(results):
        success_index = 5
        for r in results:
            if not r[success_index]:
                yield TestEntry.get_name(*r)
    try:
        with logfile_open(path_join(testdir, RESULT_JSON), 'r') as fp:
            results = json.load(fp)
    except IOError:
        sys.stderr.write('Unable to load last result. Did you run tests ?\n')
        return False
    fails = collect_failures(results['results'])
    if not overwrite:
        known = load_known_failures(testdir)
        known.extend(fails)
        fails = known
    fails_json = json.dumps(sorted(set(fails)), indent=2, separators=(',', ': '))
    if save:
        with logfile_open(os.path.join(testdir, FAIL_JSON % platform.system()), 'w+') as fp:
            fp.write(fails_json)
        sys.stdout.write('Successfully updated known failures.\n')
    if out:
        sys.stdout.write(fails_json)
        sys.stdout.write('\n')
    return True


def load_known_failures(testdir):
    try:
        with logfile_open(path_join(testdir, FAIL_JSON % platform.system()), 'r') as fp:
            return json.load(fp)
    except IOError:
        return []


class TestReporter(object):
    # Unfortunately, standard library doesn't handle timezone well
    # DATETIME_FORMAT = '%a %b %d %H:%M:%S %Z %Y'
    DATETIME_FORMAT = '%a %b %d %H:%M:%S %Y'

    def __init__(self):
        self._log = multiprocessing.get_logger()
        self._lock = multiprocessing.Lock()

    @classmethod
    def test_logfile(cls, test_name, prog_kind, dir=None):
        relpath = path_join('log', '%s_%s.log' % (test_name, prog_kind))
        return relpath if not dir else os.path.realpath(path_join(dir, relpath))

    def _start(self):
        self._start_time = time.time()

    @property
    def _elapsed(self):
        return time.time() - self._start_time

    @classmethod
    def _format_date(cls):
        return '%s' % datetime.datetime.now().strftime(cls.DATETIME_FORMAT)

    def _print_date(self):
        print(self._format_date(), file=self.out)

    def _print_bar(self, out=None):
        print(
            '===============================================================================',
            file=(out or self.out))

    def _print_exec_time(self):
        print('Test execution took {:.1f} seconds.'.format(self._elapsed), file=self.out)


class ExecReporter(TestReporter):
    def __init__(self, testdir, test, prog):
        super(ExecReporter, self).__init__()
        self._test = test
        self._prog = prog
        self.logpath = self.test_logfile(test.name, prog.kind, testdir)
        self.out = None

    def begin(self):
        self._start()
        self._open()
        if self.out and not self.out.closed:
            self._print_header()
        else:
            self._log.debug('Output stream is not available.')

    def end(self, returncode):
        self._lock.acquire()
        try:
            if self.out and not self.out.closed:
                self._print_footer(returncode)
                self._close()
                self.out = None
            else:
                self._log.debug('Output stream is not available.')
        finally:
            self._lock.release()

    def killed(self):
        print(file=self.out)
        print('Server process is successfully killed.', file=self.out)
        self.end(None)

    def died(self):
        print(file=self.out)
        print('*** Server process has died unexpectedly ***', file=self.out)
        self.end(None)

    _init_failure_exprs = {
        'server': list(map(re.compile, [
            '[Aa]ddress already in use',
            'Could not bind',
            'EADDRINUSE',
        ])),
        'client': list(map(re.compile, [
            '[Cc]onnection refused',
            'Could not connect to',
            'Could not open UNIX ',       # domain socket (rb)
            'ECONNREFUSED',
            'econnrefused',               # erl
            'CONNECTION-REFUSED-ERROR',   # cl
            'connect ENOENT',             # nodejs domain socket
            'No such file or directory',  # domain socket
            'Sockets.TcpClient.Connect',  # csharp
        ])),
    }

    def maybe_false_positive(self):
        """Searches through log file for socket bind error.
        Returns True if suspicious expression is found, otherwise False"""
        try:
            if self.out and not self.out.closed:
                self.out.flush()
            exprs = self._init_failure_exprs[self._prog.kind]

            def match(line):
                for expr in exprs:
                    if expr.search(line):
                        self._log.info("maybe false positive: %s" % line)
                        return True

            with logfile_open(self.logpath, 'r') as fp:
                if any(map(match, fp)):
                    return True
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as ex:
            self._log.warn('[%s]: Error while detecting false positive: %s' % (self._test.name, str(ex)))
            self._log.info(traceback.print_exc())
        return False

    def _open(self):
        self.out = logfile_open(self.logpath, 'w+')

    def _close(self):
        self.out.close()

    def _print_header(self):
        self._print_date()
        print('Executing: %s' % str_join(' ', self._prog.command), file=self.out)
        print('Directory: %s' % self._prog.workdir, file=self.out)
        print('config:delay: %s' % self._test.delay, file=self.out)
        print('config:timeout: %s' % self._test.timeout, file=self.out)
        self._print_bar()
        self.out.flush()

    def _print_footer(self, returncode=None):
        self._print_bar()
        if returncode is not None:
            print('Return code: %d (negative values indicate kill by signal)' % returncode, file=self.out)
        else:
            print('Process is killed.', file=self.out)
        self._print_exec_time()
        self._print_date()


class SummaryReporter(TestReporter):
    def __init__(self, basedir, testdir_relative, concurrent=True):
        super(SummaryReporter, self).__init__()
        self._basedir = basedir
        self._testdir_rel = testdir_relative
        self.logdir = path_join(self.testdir, LOG_DIR)
        self.out_path = path_join(self.testdir, RESULT_JSON)
        self.concurrent = concurrent
        self.out = sys.stdout
        self._platform = platform.system()
        self._revision = self._get_revision()
        self._tests = []
        if not os.path.exists(self.logdir):
            os.mkdir(self.logdir)
        self._known_failures = load_known_failures(self.testdir)
        self._unexpected_success = []
        self._flaky_success = []
        self._unexpected_failure = []
        self._expected_failure = []
        self._print_header()

    @property
    def testdir(self):
        return path_join(self._basedir, self._testdir_rel)

    def _result_string(self, test):
        if test.success:
            if test.retry_count == 0:
                return 'success'
            elif test.retry_count == 1:
                return 'flaky(1 retry)'
            else:
                return 'flaky(%d retries)' % test.retry_count
        elif test.expired:
            return 'failure(timeout)'
        else:
            return 'failure(%d)' % test.returncode

    def _get_revision(self):
        p = subprocess.Popen(['git', 'rev-parse', '--short', 'HEAD'],
                             cwd=self.testdir, stdout=subprocess.PIPE)
        out, _ = p.communicate()
        return out.strip()

    def _format_test(self, test, with_result=True):
        name = '%s-%s' % (test.server.name, test.client.name)
        trans = '%s-%s' % (test.transport, test.socket)
        if not with_result:
            return '{:24s}{:18s}{:25s}'.format(name[:23], test.protocol[:17], trans[:24])
        else:
            return '{:24s}{:18s}{:25s}{:s}\n'.format(name[:23], test.protocol[:17],
                                                     trans[:24], self._result_string(test))

    def _print_test_header(self):
        self._print_bar()
        print(
            '{:24s}{:18s}{:25s}{:s}'.format('server-client:', 'protocol:', 'transport:', 'result:'),
            file=self.out)

    def _print_header(self):
        self._start()
        print('Apache Thrift - Integration Test Suite', file=self.out)
        self._print_date()
        self._print_test_header()

    def _print_unexpected_failure(self):
        if len(self._unexpected_failure) > 0:
            self.out.writelines([
                '*** Following %d failures were unexpected ***:\n' % len(self._unexpected_failure),
                'If it is introduced by you, please fix it before submitting the code.\n',
                # 'If not, please report at https://issues.apache.org/jira/browse/THRIFT\n',
            ])
            self._print_test_header()
            for i in self._unexpected_failure:
                self.out.write(self._format_test(self._tests[i]))
            self._print_bar()
        else:
            print('No unexpected failures.', file=self.out)

    def _print_flaky_success(self):
        if len(self._flaky_success) > 0:
            print(
                'Following %d tests were expected to cleanly succeed but needed retry:' % len(self._flaky_success),
                file=self.out)
            self._print_test_header()
            for i in self._flaky_success:
                self.out.write(self._format_test(self._tests[i]))
            self._print_bar()

    def _print_unexpected_success(self):
        if len(self._unexpected_success) > 0:
            print(
                'Following %d tests were known to fail but succeeded (maybe flaky):' % len(self._unexpected_success),
                file=self.out)
            self._print_test_header()
            for i in self._unexpected_success:
                self.out.write(self._format_test(self._tests[i]))
            self._print_bar()

    def _http_server_command(self, port):
        if sys.version_info[0] < 3:
            return 'python -m SimpleHTTPServer %d' % port
        else:
            return 'python -m http.server %d' % port

    def _print_footer(self):
        fail_count = len(self._expected_failure) + len(self._unexpected_failure)
        self._print_bar()
        self._print_unexpected_success()
        self._print_flaky_success()
        self._print_unexpected_failure()
        self._write_html_data()
        self._assemble_log('unexpected failures', self._unexpected_failure)
        self._assemble_log('known failures', self._expected_failure)
        self.out.writelines([
            'You can browse results at:\n',
            '\tfile://%s/%s\n' % (self.testdir, RESULT_HTML),
            '# If you use Chrome, run:\n',
            '# \tcd %s\n#\t%s\n' % (self._basedir, self._http_server_command(8001)),
            '# then browse:\n',
            '# \thttp://localhost:%d/%s/\n' % (8001, self._testdir_rel),
            'Full log for each test is here:\n',
            '\ttest/log/server_client_protocol_transport_client.log\n',
            '\ttest/log/server_client_protocol_transport_server.log\n',
            '%d failed of %d tests in total.\n' % (fail_count, len(self._tests)),
        ])
        self._print_exec_time()
        self._print_date()

    def _render_result(self, test):
        return [
            test.server.name,
            test.client.name,
            test.protocol,
            test.transport,
            test.socket,
            test.success,
            test.as_expected,
            test.returncode,
            {
                'server': self.test_logfile(test.name, test.server.kind),
                'client': self.test_logfile(test.name, test.client.kind),
            },
        ]

    def _write_html_data(self):
        """Writes JSON data to be read by result html"""
        results = [self._render_result(r) for r in self._tests]
        with logfile_open(self.out_path, 'w+') as fp:
            fp.write(json.dumps({
                'date': self._format_date(),
                'revision': str(self._revision),
                'platform': self._platform,
                'duration': '{:.1f}'.format(self._elapsed),
                'results': results,
            }, indent=2))

    def _assemble_log(self, title, indexes):
        if len(indexes) > 0:
            def add_prog_log(fp, test, prog_kind):
                print('*************************** %s message ***************************' % prog_kind,
                      file=fp)
                path = self.test_logfile(test.name, prog_kind, self.testdir)
                if os.path.exists(path):
                    with logfile_open(path, 'r') as prog_fp:
                        print(prog_fp.read(), file=fp)
            filename = title.replace(' ', '_') + '.log'
            with logfile_open(os.path.join(self.logdir, filename), 'w+') as fp:
                for test in map(self._tests.__getitem__, indexes):
                    fp.write('TEST: [%s]\n' % test.name)
                    add_prog_log(fp, test, test.server.kind)
                    add_prog_log(fp, test, test.client.kind)
                    fp.write('**********************************************************************\n\n')
            print('%s are logged to %s/%s/%s' % (title.capitalize(), self._testdir_rel, LOG_DIR, filename))

    def end(self):
        self._print_footer()
        return len(self._unexpected_failure) == 0

    def add_test(self, test_dict):
        test = TestEntry(self.testdir, **test_dict)
        self._lock.acquire()
        try:
            if not self.concurrent:
                self.out.write(self._format_test(test, False))
                self.out.flush()
            self._tests.append(test)
            return len(self._tests) - 1
        finally:
            self._lock.release()

    def add_result(self, index, returncode, expired, retry_count):
        self._lock.acquire()
        try:
            failed = returncode is None or returncode != 0
            flaky = not failed and retry_count != 0
            test = self._tests[index]
            known = test.name in self._known_failures
            if failed:
                if known:
                    self._log.debug('%s failed as expected' % test.name)
                    self._expected_failure.append(index)
                else:
                    self._log.info('unexpected failure: %s' % test.name)
                    self._unexpected_failure.append(index)
            elif flaky and not known:
                self._log.info('unexpected flaky success: %s' % test.name)
                self._flaky_success.append(index)
            elif not flaky and known:
                self._log.info('unexpected success: %s' % test.name)
                self._unexpected_success.append(index)
            test.success = not failed
            test.returncode = returncode
            test.retry_count = retry_count
            test.expired = expired
            test.as_expected = known == failed
            if not self.concurrent:
                self.out.write(self._result_string(test) + '\n')
            else:
                self.out.write(self._format_test(test))
        finally:
            self._lock.release()
