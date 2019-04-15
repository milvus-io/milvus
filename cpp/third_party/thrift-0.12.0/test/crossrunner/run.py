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

import contextlib
import multiprocessing
import multiprocessing.managers
import os
import platform
import random
import socket
import subprocess
import sys
import time

from .compat import str_join
from .report import ExecReporter, SummaryReporter
from .test import TestEntry
from .util import domain_socket_path

RESULT_ERROR = 64
RESULT_TIMEOUT = 128
SIGNONE = 0
SIGKILL = 15

# globals
ports = None
stop = None


class ExecutionContext(object):
    def __init__(self, cmd, cwd, env, stop_signal, is_server, report):
        self._log = multiprocessing.get_logger()
        self.cmd = cmd
        self.cwd = cwd
        self.env = env
        self.stop_signal = stop_signal
        self.is_server = is_server
        self.report = report
        self.expired = False
        self.killed = False
        self.proc = None

    def _popen_args(self):
        args = {
            'cwd': self.cwd,
            'env': self.env,
            'stdout': self.report.out,
            'stderr': subprocess.STDOUT,
        }
        # make sure child processes doesn't remain after killing
        if platform.system() == 'Windows':
            DETACHED_PROCESS = 0x00000008
            args.update(creationflags=DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP)
        else:
            args.update(preexec_fn=os.setsid)
        return args

    def start(self):
        joined = str_join(' ', self.cmd)
        self._log.debug('COMMAND: %s', joined)
        self._log.debug('WORKDIR: %s', self.cwd)
        self._log.debug('LOGFILE: %s', self.report.logpath)
        self.report.begin()
        self.proc = subprocess.Popen(self.cmd, **self._popen_args())
        self._log.debug('    PID: %d', self.proc.pid)
        self._log.debug('   PGID: %d', os.getpgid(self.proc.pid))
        return self._scoped()

    @contextlib.contextmanager
    def _scoped(self):
        yield self
        if self.is_server:
            # the server is supposed to run until we stop it
            if self.returncode is not None:
                self.report.died()
            else:
                if self.stop_signal != SIGNONE:
                    if self.sigwait(self.stop_signal):
                        self.report.end(self.returncode)
                    else:
                        self.report.killed()
                else:
                    self.sigwait(SIGKILL)
        else:
            # the client is supposed to exit normally
            if self.returncode is not None:
                self.report.end(self.returncode)
            else:
                self.sigwait(SIGKILL)
                self.report.killed()
        self._log.debug('[{0}] exited with return code {1}'.format(self.proc.pid, self.returncode))

    # Send a signal to the process and then wait for it to end
    # If the signal requested is SIGNONE, no signal is sent, and
    # instead we just wait for the process to end; further if it
    # does not end normally with SIGNONE, we mark it as expired.
    # If the process fails to end and the signal is not SIGKILL,
    # it re-runs with SIGKILL so that a real process kill occurs
    # returns True if the process ended, False if it may not have
    def sigwait(self, sig=SIGKILL, timeout=2):
        try:
            if sig != SIGNONE:
                self._log.debug('[{0}] send signal {1}'.format(self.proc.pid, sig))
                if sig == SIGKILL:
                    self.killed = True
                try:
                    if platform.system() != 'Windows':
                        os.killpg(os.getpgid(self.proc.pid), sig)
                    else:
                        self.proc.send_signal(sig)
                except Exception:
                    self._log.info('[{0}] Failed to kill process'.format(self.proc.pid), exc_info=sys.exc_info())
            self._log.debug('[{0}] wait begin, timeout {1} sec(s)'.format(self.proc.pid, timeout))
            self.proc.communicate(timeout=timeout)
            self._log.debug('[{0}] process ended with return code {1}'.format(self.proc.pid, self.returncode))
            self.report.end(self.returncode)
            return True
        except subprocess.TimeoutExpired:
            self._log.info('[{0}] timeout waiting for process to end'.format(self.proc.pid))
            if sig == SIGNONE:
                self.expired = True
            return False if sig == SIGKILL else self.sigwait(SIGKILL, 1)

    # called on the client process to wait for it to end naturally
    def wait(self, timeout):
        self.sigwait(SIGNONE, timeout)

    @property
    def returncode(self):
        return self.proc.returncode if self.proc else None


def exec_context(port, logdir, test, prog, is_server):
    report = ExecReporter(logdir, test, prog)
    prog.build_command(port)
    return ExecutionContext(prog.command, prog.workdir, prog.env, prog.stop_signal, is_server, report)


def run_test(testdir, logdir, test_dict, max_retry, async_mode=True):
    logger = multiprocessing.get_logger()

    def ensure_socket_open(sv, port, test):
        slept = 0.1
        time.sleep(slept)
        sleep_step = 0.1
        while True:
            if slept > test.delay:
                logger.warn('[{0}] slept for {1} seconds but server is not open'.format(sv.proc.pid, slept))
                return False
            if test.socket == 'domain':
                if not os.path.exists(domain_socket_path(port)):
                    logger.debug('[{0}] domain(unix) socket not available yet. slept for {1} seconds so far'.format(sv.proc.pid, slept))
                    time.sleep(sleep_step)
                    slept += sleep_step
            elif test.socket == 'abstract':
                return True
            else:
                # Create sockets every iteration because refused sockets cannot be
                # reused on some systems.
                sock4 = socket.socket()
                sock6 = socket.socket(family=socket.AF_INET6)
                try:
                    if sock4.connect_ex(('127.0.0.1', port)) == 0 \
                            or sock6.connect_ex(('::1', port)) == 0:
                        return True
                    if sv.proc.poll() is not None:
                        logger.warn('[{0}] server process is exited'.format(sv.proc.pid))
                        return False
                    logger.debug('[{0}] socket not available yet. slept for {1} seconds so far'.format(sv.proc.pid, slept))
                    time.sleep(sleep_step)
                    slept += sleep_step
                finally:
                    sock4.close()
                    sock6.close()
            logger.debug('[{0}] server ready - waited for {1} seconds'.format(sv.proc.pid, slept))
            return True

    try:
        max_bind_retry = 3
        retry_count = 0
        bind_retry_count = 0
        test = TestEntry(testdir, **test_dict)
        while True:
            if stop.is_set():
                logger.debug('Skipping because shutting down')
                return (retry_count, None)
            logger.debug('Start')
            with PortAllocator.alloc_port_scoped(ports, test.socket) as port:
                logger.debug('Start with port %d' % port)
                sv = exec_context(port, logdir, test, test.server, True)
                cl = exec_context(port, logdir, test, test.client, False)

                logger.debug('Starting server')
                with sv.start():
                    port_ok = ensure_socket_open(sv, port, test)
                    if port_ok:
                        connect_retry_count = 0
                        max_connect_retry = 12
                        connect_retry_wait = 0.25
                        while True:
                            if sv.proc.poll() is not None:
                                logger.info('not starting client because server process is absent')
                                break
                            logger.debug('Starting client')
                            cl.start()
                            logger.debug('Waiting client (up to %d secs)' % test.timeout)
                            cl.wait(test.timeout)
                            if not cl.report.maybe_false_positive() or connect_retry_count >= max_connect_retry:
                                if connect_retry_count > 0 and connect_retry_count < max_connect_retry:
                                    logger.info('[%s]: Connected after %d retry (%.2f sec each)' % (test.server.name, connect_retry_count, connect_retry_wait))
                                # Wait for 50ms to see if server does not die at the end.
                                time.sleep(0.05)
                                break
                            logger.debug('Server may not be ready, waiting %.2f second...' % connect_retry_wait)
                            time.sleep(connect_retry_wait)
                            connect_retry_count += 1

            if sv.report.maybe_false_positive() and bind_retry_count < max_bind_retry:
                logger.warn('[%s]: Detected socket bind failure, retrying...', test.server.name)
                bind_retry_count += 1
            else:
                result = RESULT_TIMEOUT if cl.expired else cl.returncode if (cl.proc and cl.proc.poll()) is not None else RESULT_ERROR

                # For servers that handle a controlled shutdown by signal
                # if they are killed, or return an error code, that is a
                # problem.  For servers that are not signal-aware, we simply
                # kill them off; if we didn't kill them off, something else
                # happened (crashed?)
                if test.server.stop_signal != 0:
                    if sv.killed or sv.returncode > 0:
                        result |= RESULT_ERROR
                else:
                    if not sv.killed:
                        result |= RESULT_ERROR

                if result == 0 or retry_count >= max_retry:
                    return (retry_count, result)
                else:
                    logger.info('[%s-%s]: test failed, retrying...', test.server.name, test.client.name)
                    retry_count += 1
    except Exception:
        if not async_mode:
            raise
        logger.warn('Error executing [%s]', test.name, exc_info=True)
        return (retry_count, RESULT_ERROR)
    except:
        logger.info('Interrupted execution', exc_info=True)
        if not async_mode:
            raise
        stop.set()
        return (retry_count, RESULT_ERROR)


class PortAllocator(object):
    def __init__(self):
        self._log = multiprocessing.get_logger()
        self._lock = multiprocessing.Lock()
        self._ports = set()
        self._dom_ports = set()
        self._last_alloc = 0

    def _get_tcp_port(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('', 0))
        port = sock.getsockname()[1]
        self._lock.acquire()
        try:
            ok = port not in self._ports
            if ok:
                self._ports.add(port)
                self._last_alloc = time.time()
        finally:
            self._lock.release()
            sock.close()
        return port if ok else self._get_tcp_port()

    def _get_domain_port(self):
        port = random.randint(1024, 65536)
        self._lock.acquire()
        try:
            ok = port not in self._dom_ports
            if ok:
                self._dom_ports.add(port)
        finally:
            self._lock.release()
        return port if ok else self._get_domain_port()

    def alloc_port(self, socket_type):
        if socket_type in ('domain', 'abstract'):
            return self._get_domain_port()
        else:
            return self._get_tcp_port()

    # static method for inter-process invokation
    @staticmethod
    @contextlib.contextmanager
    def alloc_port_scoped(allocator, socket_type):
        port = allocator.alloc_port(socket_type)
        yield port
        allocator.free_port(socket_type, port)

    def free_port(self, socket_type, port):
        self._log.debug('free_port')
        self._lock.acquire()
        try:
            if socket_type == 'domain':
                self._dom_ports.remove(port)
                path = domain_socket_path(port)
                if os.path.exists(path):
                    os.remove(path)
            elif socket_type == 'abstract':
                self._dom_ports.remove(port)
            else:
                self._ports.remove(port)
        except IOError:
            self._log.info('Error while freeing port', exc_info=sys.exc_info())
        finally:
            self._lock.release()


class NonAsyncResult(object):
    def __init__(self, value):
        self._value = value

    def get(self, timeout=None):
        return self._value

    def wait(self, timeout=None):
        pass

    def ready(self):
        return True

    def successful(self):
        return self._value == 0


class TestDispatcher(object):
    def __init__(self, testdir, basedir, logdir_rel, concurrency):
        self._log = multiprocessing.get_logger()
        self.testdir = testdir
        self._report = SummaryReporter(basedir, logdir_rel, concurrency > 1)
        self.logdir = self._report.testdir
        # seems needed for python 2.x to handle keyboard interrupt
        self._stop = multiprocessing.Event()
        self._async = concurrency > 1
        if not self._async:
            self._pool = None
            global stop
            global ports
            stop = self._stop
            ports = PortAllocator()
        else:
            self._m = multiprocessing.managers.BaseManager()
            self._m.register('ports', PortAllocator)
            self._m.start()
            self._pool = multiprocessing.Pool(concurrency, self._pool_init, (self._m.address,))
        self._log.debug(
            'TestDispatcher started with %d concurrent jobs' % concurrency)

    def _pool_init(self, address):
        global stop
        global m
        global ports
        stop = self._stop
        m = multiprocessing.managers.BaseManager(address)
        m.connect()
        ports = m.ports()

    def _dispatch_sync(self, test, cont, max_retry):
        r = run_test(self.testdir, self.logdir, test, max_retry, async_mode=False)
        cont(r)
        return NonAsyncResult(r)

    def _dispatch_async(self, test, cont, max_retry):
        self._log.debug('_dispatch_async')
        return self._pool.apply_async(func=run_test, args=(self.testdir, self.logdir, test, max_retry), callback=cont)

    def dispatch(self, test, max_retry):
        index = self._report.add_test(test)

        def cont(result):
            if not self._stop.is_set():
                if result and len(result) == 2:
                    retry_count, returncode = result
                else:
                    retry_count = 0
                    returncode = RESULT_ERROR
                self._log.debug('freeing port')
                self._log.debug('adding result')
                self._report.add_result(index, returncode, returncode == RESULT_TIMEOUT, retry_count)
                self._log.debug('finish continuation')
        fn = self._dispatch_async if self._async else self._dispatch_sync
        return fn(test, cont, max_retry)

    def wait(self):
        if self._async:
            self._pool.close()
            self._pool.join()
            self._m.shutdown()
        return self._report.end()

    def terminate(self):
        self._stop.set()
        if self._async:
            self._pool.terminate()
            self._pool.join()
            self._m.shutdown()
