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

import sys
import unittest

import _import_local_thrift  # noqa
from thrift.protocol.TJSONProtocol import TJSONProtocol
from thrift.transport import TTransport

#
# In order to run the test under Windows. We need to create symbolic link
# name 'thrift' to '../src' folder by using:
#
# mklink /D thrift ..\src
#


class TestJSONString(unittest.TestCase):

    def test_escaped_unicode_string(self):
        unicode_json = b'"hello \\u0e01\\u0e02\\u0e03\\ud835\\udcab\\udb40\\udc70 unicode"'
        unicode_text = u'hello \u0e01\u0e02\u0e03\U0001D4AB\U000E0070 unicode'

        buf = TTransport.TMemoryBuffer(unicode_json)
        transport = TTransport.TBufferedTransportFactory().getTransport(buf)
        protocol = TJSONProtocol(transport)

        if sys.version_info[0] == 2:
            unicode_text = unicode_text.encode('utf8')
        self.assertEqual(protocol.readString(), unicode_text)


if __name__ == '__main__':
    unittest.main()
