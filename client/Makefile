# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

GO		  ?= go
PWD 	  := $(shell pwd)
GOPATH	:= $(shell $(GO) env GOPATH)
SHELL 	:= /bin/bash
OBJPREFIX := "github.com/milvus-io/milvus/cmd/milvus/v2"

# TODO pass golangci-lint path
lint:
	@echo "Running lint checks..." 

unittest:
	@echo "Running unittests..."
	@(env bash $(PWD)/scripts/run_unittest.sh)

generate-mockery:
	@echo "Generating mockery Milvus service server"
	@../bin/mockery --srcpkg=github.com/milvus-io/milvus-proto/go-api/v2/milvuspb --name=MilvusServiceServer --filename=mock_milvus_server_test.go --output=. --outpkg=client --with-expecter 
