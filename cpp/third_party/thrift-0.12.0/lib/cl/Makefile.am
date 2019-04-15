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

THRIFT = $(top_builddir)/compiler/cpp/thrift

all-local:
	bash ensure-externals.sh

run-tests: test/make-test-binary.lisp
	$(SBCL) --script test/make-test-binary.lisp

check-local: run-tests
	./run-tests

clean-local:
	$(RM) run-tests quicklisp.lisp backport-update.zip
	$(RM) -rf lib externals quicklisp

EXTRA_DIST = \
	README.md \
	READMES \
	load-locally.lisp \
	test \
	ensure-externals.sh
