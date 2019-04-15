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

import inspect
import logging
import os
import platform
import ssl
import sys
import tempfile
import threading
import unittest
import warnings
from contextlib import contextmanager

import _import_local_thrift  # noqa

SCRIPT_DIR = os.path.realpath(os.path.dirname(__file__))
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(SCRIPT_DIR)))
SERVER_PEM = os.path.join(ROOT_DIR, 'test', 'keys', 'server.pem')
SERVER_CERT = os.path.join(ROOT_DIR, 'test', 'keys', 'server.crt')
SERVER_KEY = os.path.join(ROOT_DIR, 'test', 'keys', 'server.key')
CLIENT_CERT_NO_IP = os.path.join(ROOT_DIR, 'test', 'keys', 'client.crt')
CLIENT_KEY_NO_IP = os.path.join(ROOT_DIR, 'test', 'keys', 'client.key')
CLIENT_CERT = os.path.join(ROOT_DIR, 'test', 'keys', 'client_v3.crt')
CLIENT_KEY = os.path.join(ROOT_DIR, 'test', 'keys', 'client_v3.key')
CLIENT_CA = os.path.join(ROOT_DIR, 'test', 'keys', 'CA.pem')

TEST_CIPHERS = 'DES-CBC3-SHA:ECDHE-RSA-AES128-GCM-SHA256'


class ServerAcceptor(threading.Thread):
    def __init__(self, server, expect_failure=False):
        super(ServerAcceptor, self).__init__()
        self.daemon = True
        self._server = server
        self._listening = threading.Event()
        self._port = None
        self._port_bound = threading.Event()
        self._client = None
        self._client_accepted = threading.Event()
        self._expect_failure = expect_failure
        frame = inspect.stack(3)[2]
        self.name = frame[3]
        del frame

    def run(self):
        self._server.listen()
        self._listening.set()

        try:
            address = self._server.handle.getsockname()
            if len(address) > 1:
                # AF_INET addresses are 2-tuples (host, port) and AF_INET6 are
                # 4-tuples (host, port, ...), but in each case port is in the second slot.
                self._port = address[1]
        finally:
            self._port_bound.set()

        try:
            self._client = self._server.accept()
        except Exception:
            logging.exception('error on server side (%s):' % self.name)
            if not self._expect_failure:
                raise
        finally:
            self._client_accepted.set()

    def await_listening(self):
        self._listening.wait()

    @property
    def port(self):
        self._port_bound.wait()
        return self._port

    @property
    def client(self):
        self._client_accepted.wait()
        return self._client

    def close(self):
        if self._client:
            self._client.close()
        self._server.close()


# Python 2.6 compat
class AssertRaises(object):
    def __init__(self, expected):
        self._expected = expected

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        if not exc_type or not issubclass(exc_type, self._expected):
            raise Exception('fail')
        return True


class TSSLSocketTest(unittest.TestCase):
    def _server_socket(self, **kwargs):
        return TSSLServerSocket(port=0, **kwargs)

    @contextmanager
    def _connectable_client(self, server, expect_failure=False, path=None, **client_kwargs):
        acc = ServerAcceptor(server, expect_failure)
        try:
            acc.start()
            acc.await_listening()

            host, port = ('localhost', acc.port) if path is None else (None, None)
            client = TSSLSocket(host, port, unix_socket=path, **client_kwargs)
            yield acc, client
        finally:
            acc.close()

    def _assert_connection_failure(self, server, path=None, **client_args):
        logging.disable(logging.CRITICAL)
        try:
            with self._connectable_client(server, True, path=path, **client_args) as (acc, client):
                # We need to wait for a connection failure, but not too long.  20ms is a tunable
                # compromise between test speed and stability
                client.setTimeout(20)
                with self._assert_raises(TTransportException):
                    client.open()
                self.assertTrue(acc.client is None)
        finally:
            logging.disable(logging.NOTSET)

    def _assert_raises(self, exc):
        if sys.hexversion >= 0x020700F0:
            return self.assertRaises(exc)
        else:
            return AssertRaises(exc)

    def _assert_connection_success(self, server, path=None, **client_args):
        with self._connectable_client(server, path=path, **client_args) as (acc, client):
            client.open()
            try:
                self.assertTrue(acc.client is not None)
            finally:
                client.close()

    # deprecated feature
    def test_deprecation(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.filterwarnings('always', category=DeprecationWarning, module=self.__module__)
            TSSLSocket('localhost', 0, validate=True, ca_certs=SERVER_CERT)
            self.assertEqual(len(w), 1)

        with warnings.catch_warnings(record=True) as w:
            warnings.filterwarnings('always', category=DeprecationWarning, module=self.__module__)
            # Deprecated signature
            # def __init__(self, host='localhost', port=9090, validate=True, ca_certs=None, keyfile=None, certfile=None, unix_socket=None, ciphers=None):
            TSSLSocket('localhost', 0, True, SERVER_CERT, CLIENT_KEY, CLIENT_CERT, None, TEST_CIPHERS)
            self.assertEqual(len(w), 7)

        with warnings.catch_warnings(record=True) as w:
            warnings.filterwarnings('always', category=DeprecationWarning, module=self.__module__)
            # Deprecated signature
            # def __init__(self, host=None, port=9090, certfile='cert.pem', unix_socket=None, ciphers=None):
            TSSLServerSocket(None, 0, SERVER_PEM, None, TEST_CIPHERS)
            self.assertEqual(len(w), 3)

    # deprecated feature
    def test_set_cert_reqs_by_validate(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.filterwarnings('always', category=DeprecationWarning, module=self.__module__)
            c1 = TSSLSocket('localhost', 0, validate=True, ca_certs=SERVER_CERT)
            self.assertEqual(c1.cert_reqs, ssl.CERT_REQUIRED)

            c1 = TSSLSocket('localhost', 0, validate=False)
            self.assertEqual(c1.cert_reqs, ssl.CERT_NONE)

            self.assertEqual(len(w), 2)

    # deprecated feature
    def test_set_validate_by_cert_reqs(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.filterwarnings('always', category=DeprecationWarning, module=self.__module__)
            c1 = TSSLSocket('localhost', 0, cert_reqs=ssl.CERT_NONE)
            self.assertFalse(c1.validate)

            c2 = TSSLSocket('localhost', 0, cert_reqs=ssl.CERT_REQUIRED, ca_certs=SERVER_CERT)
            self.assertTrue(c2.validate)

            c3 = TSSLSocket('localhost', 0, cert_reqs=ssl.CERT_OPTIONAL, ca_certs=SERVER_CERT)
            self.assertTrue(c3.validate)

            self.assertEqual(len(w), 3)

    def test_unix_domain_socket(self):
        if platform.system() == 'Windows':
            print('skipping test_unix_domain_socket')
            return
        fd, path = tempfile.mkstemp()
        os.close(fd)
        try:
            server = self._server_socket(unix_socket=path, keyfile=SERVER_KEY, certfile=SERVER_CERT)
            self._assert_connection_success(server, path=path, cert_reqs=ssl.CERT_NONE)
        finally:
            os.unlink(path)

    def test_server_cert(self):
        server = self._server_socket(keyfile=SERVER_KEY, certfile=SERVER_CERT)
        self._assert_connection_success(server, cert_reqs=ssl.CERT_REQUIRED, ca_certs=SERVER_CERT)

        server = self._server_socket(keyfile=SERVER_KEY, certfile=SERVER_CERT)
        # server cert not in ca_certs
        self._assert_connection_failure(server, cert_reqs=ssl.CERT_REQUIRED, ca_certs=CLIENT_CERT)

        server = self._server_socket(keyfile=SERVER_KEY, certfile=SERVER_CERT)
        self._assert_connection_success(server, cert_reqs=ssl.CERT_NONE)

    def test_set_server_cert(self):
        server = self._server_socket(keyfile=SERVER_KEY, certfile=CLIENT_CERT)
        with self._assert_raises(Exception):
            server.certfile = 'foo'
        with self._assert_raises(Exception):
            server.certfile = None
        server.certfile = SERVER_CERT
        self._assert_connection_success(server, cert_reqs=ssl.CERT_REQUIRED, ca_certs=SERVER_CERT)

    def test_client_cert(self):
        if not _match_has_ipaddress:
            print('skipping test_client_cert')
            return
        server = self._server_socket(
            cert_reqs=ssl.CERT_REQUIRED, keyfile=SERVER_KEY,
            certfile=SERVER_CERT, ca_certs=CLIENT_CERT)
        self._assert_connection_failure(server, cert_reqs=ssl.CERT_NONE, certfile=SERVER_CERT, keyfile=SERVER_KEY)

        server = self._server_socket(
            cert_reqs=ssl.CERT_REQUIRED, keyfile=SERVER_KEY,
            certfile=SERVER_CERT, ca_certs=CLIENT_CA)
        self._assert_connection_failure(server, cert_reqs=ssl.CERT_NONE, certfile=CLIENT_CERT_NO_IP, keyfile=CLIENT_KEY_NO_IP)

        server = self._server_socket(
            cert_reqs=ssl.CERT_REQUIRED, keyfile=SERVER_KEY,
            certfile=SERVER_CERT, ca_certs=CLIENT_CA)
        self._assert_connection_success(server, cert_reqs=ssl.CERT_NONE, certfile=CLIENT_CERT, keyfile=CLIENT_KEY)

        server = self._server_socket(
            cert_reqs=ssl.CERT_OPTIONAL, keyfile=SERVER_KEY,
            certfile=SERVER_CERT, ca_certs=CLIENT_CA)
        self._assert_connection_success(server, cert_reqs=ssl.CERT_NONE, certfile=CLIENT_CERT, keyfile=CLIENT_KEY)

    def test_ciphers(self):
        server = self._server_socket(keyfile=SERVER_KEY, certfile=SERVER_CERT, ciphers=TEST_CIPHERS)
        self._assert_connection_success(server, ca_certs=SERVER_CERT, ciphers=TEST_CIPHERS)

        if not TSSLSocket._has_ciphers:
            # unittest.skip is not available for Python 2.6
            print('skipping test_ciphers')
            return
        server = self._server_socket(keyfile=SERVER_KEY, certfile=SERVER_CERT)
        self._assert_connection_failure(server, ca_certs=SERVER_CERT, ciphers='NULL')

        server = self._server_socket(keyfile=SERVER_KEY, certfile=SERVER_CERT, ciphers=TEST_CIPHERS)
        self._assert_connection_failure(server, ca_certs=SERVER_CERT, ciphers='NULL')

    def test_ssl2_and_ssl3_disabled(self):
        if not hasattr(ssl, 'PROTOCOL_SSLv3'):
            print('PROTOCOL_SSLv3 is not available')
        else:
            server = self._server_socket(keyfile=SERVER_KEY, certfile=SERVER_CERT)
            self._assert_connection_failure(server, ca_certs=SERVER_CERT, ssl_version=ssl.PROTOCOL_SSLv3)

            server = self._server_socket(keyfile=SERVER_KEY, certfile=SERVER_CERT, ssl_version=ssl.PROTOCOL_SSLv3)
            self._assert_connection_failure(server, ca_certs=SERVER_CERT)

        if not hasattr(ssl, 'PROTOCOL_SSLv2'):
            print('PROTOCOL_SSLv2 is not available')
        else:
            server = self._server_socket(keyfile=SERVER_KEY, certfile=SERVER_CERT)
            self._assert_connection_failure(server, ca_certs=SERVER_CERT, ssl_version=ssl.PROTOCOL_SSLv2)

            server = self._server_socket(keyfile=SERVER_KEY, certfile=SERVER_CERT, ssl_version=ssl.PROTOCOL_SSLv2)
            self._assert_connection_failure(server, ca_certs=SERVER_CERT)

    def test_newer_tls(self):
        if not TSSLSocket._has_ssl_context:
            # unittest.skip is not available for Python 2.6
            print('skipping test_newer_tls')
            return
        if not hasattr(ssl, 'PROTOCOL_TLSv1_2'):
            print('PROTOCOL_TLSv1_2 is not available')
        else:
            server = self._server_socket(keyfile=SERVER_KEY, certfile=SERVER_CERT, ssl_version=ssl.PROTOCOL_TLSv1_2)
            self._assert_connection_success(server, ca_certs=SERVER_CERT, ssl_version=ssl.PROTOCOL_TLSv1_2)

        if not hasattr(ssl, 'PROTOCOL_TLSv1_1'):
            print('PROTOCOL_TLSv1_1 is not available')
        else:
            server = self._server_socket(keyfile=SERVER_KEY, certfile=SERVER_CERT, ssl_version=ssl.PROTOCOL_TLSv1_1)
            self._assert_connection_success(server, ca_certs=SERVER_CERT, ssl_version=ssl.PROTOCOL_TLSv1_1)

        if not hasattr(ssl, 'PROTOCOL_TLSv1_1') or not hasattr(ssl, 'PROTOCOL_TLSv1_2'):
            print('PROTOCOL_TLSv1_1 and/or PROTOCOL_TLSv1_2 is not available')
        else:
            server = self._server_socket(keyfile=SERVER_KEY, certfile=SERVER_CERT, ssl_version=ssl.PROTOCOL_TLSv1_2)
            self._assert_connection_failure(server, ca_certs=SERVER_CERT, ssl_version=ssl.PROTOCOL_TLSv1_1)

    def test_ssl_context(self):
        if not TSSLSocket._has_ssl_context:
            # unittest.skip is not available for Python 2.6
            print('skipping test_ssl_context')
            return
        server_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        server_context.load_cert_chain(SERVER_CERT, SERVER_KEY)
        server_context.load_verify_locations(CLIENT_CA)
        server_context.verify_mode = ssl.CERT_REQUIRED
        server = self._server_socket(ssl_context=server_context)

        client_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        client_context.load_cert_chain(CLIENT_CERT, CLIENT_KEY)
        client_context.load_verify_locations(SERVER_CERT)
        client_context.verify_mode = ssl.CERT_REQUIRED

        self._assert_connection_success(server, ssl_context=client_context)


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARN)
    from thrift.transport.TSSLSocket import TSSLSocket, TSSLServerSocket, _match_has_ipaddress
    from thrift.transport.TTransport import TTransportException

    unittest.main()
