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

import glob
import os
import sys

_SCRIPT_DIR = os.path.realpath(os.path.dirname(__file__))
_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(_SCRIPT_DIR)))
_LIBDIR = os.path.join(_ROOT_DIR, 'lib', 'py', 'build', 'lib.*')

for libpath in glob.glob(_LIBDIR):
    if libpath.endswith('-%d.%d' % (sys.version_info[0], sys.version_info[1])):
        sys.path.insert(0, libpath)
        thrift = __import__('thrift')
        break
