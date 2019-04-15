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

gen-go/tutorial/calculator.go gen-go/shared/shared_service.go: $(top_srcdir)/tutorial/tutorial.thrift
	$(THRIFT) --gen go$(COMPILER_EXTRAFLAG) -r $<

all-local: gen-go/tutorial/calculator.go

check: src/github.com/apache/thrift/lib/go/thrift thirdparty-dep
	$(THRIFT) -r --gen go$(COMPILER_EXTRAFLAG) $(top_srcdir)/tutorial/tutorial.thrift
	cp -r gen-go/* src/
	GOPATH=`pwd` $(GO) build -o go-tutorial ./src
	GOPATH=`pwd` $(GO) build -o calculator-remote src/tutorial/calculator-remote/calculator-remote.go

src/github.com/apache/thrift/lib/go/thrift:
	mkdir -p src/github.com/apache/thrift/lib/go
	ln -sf $(realpath $(top_srcdir)/lib/go/thrift) src/github.com/apache/thrift/lib/go/thrift

thirdparty-dep:

tutorialserver: all
	GOPATH=`pwd` $(GO) run src/*.go -server=true

tutorialclient: all
	GOPATH=`pwd` $(GO) run src/*.go

tutorialsecureserver: all
	GOPATH=`pwd` $(GO) run src/*.go -server=true -secure=true

tutorialsecureclient: all
	GOPATH=`pwd` $(GO) run src/*.go -secure=true

clean-local:
	$(RM) -r gen-* src/shared src/tutorial src/git.apache.org go-tutorial calculator-remote

EXTRA_DIST = \
	src/client.go \
	src/handler.go \
	src/server.go \
	src/main.go \
	server.crt \
	server.key

