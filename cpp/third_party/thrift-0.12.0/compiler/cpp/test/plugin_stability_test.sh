#!/bin/sh

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

# this file is intended to be invoked by make.
#
# This file runs the compiler twice, using a plugin that just invokes
# /bin/cat, and compares the output.  If GeneratorInput is
# nondeterminsitic, you'd expect the output to differ from run-to-run.
# So this tests that in fact, the output is stable from run-to-run.
set -e
mkdir -p gen-bincat
PATH=.:"$PATH" ../thrift -r -gen bincat ../../../test/Include.thrift > gen-bincat/1.ser
PATH=.:"$PATH" ../thrift -r -gen bincat ../../../test/Include.thrift > gen-bincat/2.ser
diff --binary gen-bincat/1.ser gen-bincat/2.ser
