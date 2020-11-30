# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License.

GO		?= go
PWD 	:= $(shell pwd)
GOPATH 	:= $(shell $(GO) env GOPATH)

INSTALL_PATH := $(PWD)/bin
LIBRARY_PATH := $(PWD)/lib

all: build-cpp build-go

get-build-deps:
	@(env bash $(PWD)/scripts/install_deps.sh)

cppcheck:
	@(env bash ${PWD}/scripts/core_build.sh -l)

generated-proto-go:export protoc:=${PWD}/cmake_build/thirdparty/protobuf/protobuf-build/protoc
generated-proto-go: build-cpp
	@(env bash $(PWD)/scripts/proto_gen_go.sh)

check-proto-product: generated-proto-go
	@(env bash $(PWD)/scripts/check_proto_product.sh)

fmt:
	@echo "Running $@ check"
	@GO111MODULE=on env bash $(PWD)/scripts/gofmt.sh cmd/
	@GO111MODULE=on env bash $(PWD)/scripts/gofmt.sh internal/
	@GO111MODULE=on env bash $(PWD)/scripts/gofmt.sh tests/go/

#TODO: Check code specifications by golangci-lint
lint:
	@echo "Running $@ check"
	@GO111MODULE=on ${GOPATH}/bin/golangci-lint cache clean
	@GO111MODULE=on ${GOPATH}/bin/golangci-lint run --timeout=3m --config ./.golangci.yml ./internal/...
	@GO111MODULE=on ${GOPATH}/bin/golangci-lint run --timeout=3m --config ./.golangci.yml ./cmd/...
	@GO111MODULE=on ${GOPATH}/bin/golangci-lint run --timeout=3m --config ./.golangci.yml ./tests/go/...

ruleguard:
	@echo "Running $@ check"
	@${GOPATH}/bin/ruleguard -rules ruleguard.rules.go ./internal/...
	@${GOPATH}/bin/ruleguard -rules ruleguard.rules.go ./cmd/...
	@${GOPATH}/bin/ruleguard -rules ruleguard.rules.go ./tests/go/...

verifiers: cppcheck fmt lint ruleguard

# Builds various components locally.
build-go:
	@echo "Building each component's binary to './bin'"
	@echo "Building master ..."
	@mkdir -p $(INSTALL_PATH) && go env -w CGO_ENABLED="0" && GO111MODULE=on $(GO) build -o $(INSTALL_PATH)/master $(PWD)/cmd/master/main.go 1>/dev/null
	@echo "Building proxy ..."
	@mkdir -p $(INSTALL_PATH) && go env -w CGO_ENABLED="0" && GO111MODULE=on $(GO) build -o $(INSTALL_PATH)/proxy $(PWD)/cmd/proxy/proxy.go 1>/dev/null
	@echo "Building query node ..."
	@mkdir -p $(INSTALL_PATH) && go env -w CGO_ENABLED="1" && GO111MODULE=on $(GO) build -o $(INSTALL_PATH)/querynode $(PWD)/cmd/querynode/query_node.go 1>/dev/null

build-cpp:
	@(env bash $(PWD)/scripts/core_build.sh)

build-cpp-with-unittest:
	@(env bash $(PWD)/scripts/core_build.sh -u)

# Runs the tests.
unittest: test-cpp test-go

#TODO: proxy master query node writer's unittest
test-go:
	@echo "Running go unittests..."
	@(env bash $(PWD)/scripts/run_go_unittest.sh)

test-cpp: build-cpp-with-unittest
	@echo "Running cpp unittests..."
	@(env bash $(PWD)/scripts/run_cpp_unittest.sh)

#TODO: build each component to docker
docker: verifiers
	@echo "Building query node docker image '$(TAG)'"
	@echo "Building proxy docker image '$(TAG)'"
	@echo "Building master docker image '$(TAG)'"

# Builds each component and installs it to $GOPATH/bin.
install: all
	@echo "Installing binary to './bin'"
	@mkdir -p $(GOPATH)/bin && cp -f $(PWD)/bin/querynode $(GOPATH)/bin/querynode
	@mkdir -p $(GOPATH)/bin && cp -f $(PWD)/bin/master $(GOPATH)/bin/master
	@mkdir -p $(GOPATH)/bin && cp -f $(PWD)/bin/proxy $(GOPATH)/bin/proxy
	@mkdir -p $(LIBRARY_PATH) && cp -f $(PWD)/internal/core/output/lib/* $(LIBRARY_PATH)
	@echo "Installation successful."

clean:
	@echo "Cleaning up all the generated files"
	@find . -name '*.test' | xargs rm -fv
	@find . -name '*~' | xargs rm -fv
	@rm -rvf querynode
	@rm -rvf master
	@rm -rvf proxy
