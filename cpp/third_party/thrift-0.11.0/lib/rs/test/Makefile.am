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

stubs: thrifts/Base_One.thrift thrifts/Base_Two.thrift thrifts/Midlayer.thrift thrifts/Ultimate.thrift $(top_builddir)/test/Recursive.thrift $(THRIFT)
	$(THRIFT) -I ./thrifts -out src --gen rs thrifts/Base_One.thrift
	$(THRIFT) -I ./thrifts -out src --gen rs thrifts/Base_Two.thrift
	$(THRIFT) -I ./thrifts -out src --gen rs thrifts/Midlayer.thrift
	$(THRIFT) -I ./thrifts -out src --gen rs thrifts/Ultimate.thrift
	$(THRIFT) -out src --gen rs $(top_builddir)/test/Recursive.thrift

check: stubs
	$(CARGO) build
	$(CARGO) test
	[ -d bin ] || mkdir bin
	cp target/debug/kitchen_sink_server bin/kitchen_sink_server
	cp target/debug/kitchen_sink_client bin/kitchen_sink_client

clean-local:
	$(CARGO) clean
	-$(RM) Cargo.lock
	-$(RM) src/base_one.rs
	-$(RM) src/base_two.rs
	-$(RM) src/midlayer.rs
	-$(RM) src/ultimate.rs
	-$(RM) -r bin

EXTRA_DIST = \
	Cargo.toml \
	thrifts/Base_One.thrift \
	thrifts/Base_Two.thrift \
	thrifts/Midlayer.thrift \
	thrifts/Ultimate.thrift \
	src/lib.rs \
	src/bin/kitchen_sink_server.rs \
	src/bin/kitchen_sink_client.rs

