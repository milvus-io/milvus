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

import copy
import multiprocessing
import os
import sys
from .compat import path_join
from .util import merge_dict


def domain_socket_path(port):
    return '/tmp/ThriftTest.thrift.%d' % port


class TestProgram(object):
    def __init__(self, kind, name, protocol, transport, socket, workdir, command, env=None,
                 extra_args=[], extra_args2=[], join_args=False, **kwargs):
        self.kind = kind
        self.name = name
        self.protocol = protocol
        self.transport = transport
        self.socket = socket
        self.workdir = workdir
        self.command = None
        self._base_command = self._fix_cmd_path(command)
        if env:
            self.env = copy.copy(os.environ)
            self.env.update(env)
        else:
            self.env = os.environ
        self._extra_args = extra_args
        self._extra_args2 = extra_args2
        self._join_args = join_args

    def _fix_cmd_path(self, cmd):
        # if the arg is a file in the current directory, make it path
        def abs_if_exists(arg):
            p = path_join(self.workdir, arg)
            return p if os.path.exists(p) else arg

        if cmd[0] == 'python':
            cmd[0] = sys.executable
        else:
            cmd[0] = abs_if_exists(cmd[0])
        return cmd

    def _socket_args(self, socket, port):
        return {
            'ip-ssl': ['--ssl'],
            'domain': ['--domain-socket=%s' % domain_socket_path(port)],
            'abstract': ['--abstract-namespace', '--domain-socket=%s' % domain_socket_path(port)],
        }.get(socket, None)

    def build_command(self, port):
        cmd = copy.copy(self._base_command)
        args = copy.copy(self._extra_args2)
        args.append('--protocol=' + self.protocol)
        args.append('--transport=' + self.transport)
        socket_args = self._socket_args(self.socket, port)
        if socket_args:
            args += socket_args
        args.append('--port=%d' % port)
        if self._join_args:
            cmd.append('%s' % " ".join(args))
        else:
            cmd.extend(args)
        if self._extra_args:
            cmd.extend(self._extra_args)
        self.command = cmd
        return self.command


class TestEntry(object):
    def __init__(self, testdir, server, client, delay, timeout, **kwargs):
        self.testdir = testdir
        self._log = multiprocessing.get_logger()
        self._config = kwargs
        self.protocol = kwargs['protocol']
        self.transport = kwargs['transport']
        self.socket = kwargs['socket']
        srv_dict = self._fix_workdir(merge_dict(self._config, server))
        cli_dict = self._fix_workdir(merge_dict(self._config, client))
        cli_dict['extra_args2'] = srv_dict.pop('remote_args', [])
        srv_dict['extra_args2'] = cli_dict.pop('remote_args', [])
        self.server = TestProgram('server', **srv_dict)
        self.client = TestProgram('client', **cli_dict)
        self.delay = delay
        self.timeout = timeout
        self._name = None
        # results
        self.success = None
        self.as_expected = None
        self.returncode = None
        self.expired = False
        self.retry_count = 0

    def _fix_workdir(self, config):
        key = 'workdir'
        path = config.get(key, None)
        if not path:
            path = self.testdir
        if os.path.isabs(path):
            path = os.path.realpath(path)
        else:
            path = os.path.realpath(path_join(self.testdir, path))
        config.update({key: path})
        return config

    @classmethod
    def get_name(cls, server, client, protocol, transport, socket, *args, **kwargs):
        return '%s-%s_%s_%s-%s' % (server, client, protocol, transport, socket)

    @property
    def name(self):
        if not self._name:
            self._name = self.get_name(
                self.server.name, self.client.name, self.protocol, self.transport, self.socket)
        return self._name

    @property
    def transport_name(self):
        return '%s-%s' % (self.transport, self.socket)


def test_name(server, client, protocol, transport, socket, **kwargs):
    return TestEntry.get_name(server['name'], client['name'], protocol, transport, socket)
