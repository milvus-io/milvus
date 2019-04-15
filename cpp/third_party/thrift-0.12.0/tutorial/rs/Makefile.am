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

gen-rs/tutorial.rs gen-rs/shared.rs: $(top_srcdir)/tutorial/tutorial.thrift
	$(THRIFT) -out src --gen rs -r $<

all-local: gen-rs/tutorial.rs
	$(CARGO) build
	[ -d bin ] || mkdir bin
	cp target/debug/tutorial_server bin/tutorial_server
	cp target/debug/tutorial_client bin/tutorial_client

check: all

tutorialserver: all
	bin/tutorial_server	

tutorialclient: all
	bin/tutorial_client

clean-local:
	$(CARGO) clean
	-$(RM) Cargo.lock
	-$(RM) src/shared.rs
	-$(RM) src/tutorial.rs
	-$(RM) -r bin

EXTRA_DIST = \
	Cargo.toml \
	src/lib.rs \
	src/bin/tutorial_server.rs \
	src/bin/tutorial_client.rs \
	README.md

