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

# We call npm twice to work around npm issues

stubs: $(top_srcdir)/test/ThriftTest.thrift
	mkdir -p test-compiled
	$(THRIFT) --gen js:node,ts -o test/ $(top_srcdir)/test/ThriftTest.thrift && $(THRIFT) --gen js:node,ts -o test-compiled $(top_srcdir)/test/ThriftTest.thrift

ts-compile: stubs
	mkdir -p test-compiled
	../../node_modules/typescript/bin/tsc --outDir test-compiled/ --project test/tsconfig.json

deps: $(top_srcdir)/package.json
	$(NPM) install $(top_srcdir)/ || $(NPM) install $(top_srcdir)/

all-local: deps ts-compile

precross: deps stubs ts-compile

check: deps ts-compile
	cd $(top_srcdir) && $(NPM) run test-ts && cd lib/nodets

clean-local:
	$(RM) -r test/gen-nodejs
	$(RM) -r $(top_srcdir)/node_modules
	$(RM) -r test-compiled

EXTRA_DIST = \
	test \
	coding_standards.md
