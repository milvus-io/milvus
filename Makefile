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

#TODO: Use ruleguard to check code specifications
get-check-deps:
	@mkdir -p ${GOPATH}/bin
	@which golangci-lint 1>/dev/null || (echo "Installing golangci-lint" && curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin v1.27.0)
	@which ruleguard 1>/dev/null || (echo "Installing ruleguard" && GO111MODULE=off $(GO) get github.com/quasilyte/go-ruleguard/...)

get-build-deps:
	@(env bash $(PWD)/scripts/install_deps.sh)

fmt:
	@echo "Running $@ check"
	@GO111MODULE=on gofmt -d cmd/
	@GO111MODULE=on gofmt -d internal/

#TODO: Check code specifications by golangci-lint
lint:
	@echo "Running $@ check"
	@GO111MODULE=on ${GOPATH}/bin/golangci-lint cache clean
	@GO111MODULE=on ${GOPATH}/bin/golangci-lint run --timeout=1m --config ./.golangci.yml ./internal/ || true
	@GO111MODULE=on ${GOPATH}/bin/golangci-lint run --timeout=1m --config ./.golangci.yml ./cmd/ || true
	@GO111MODULE=on ${GOPATH}/bin/golangci-lint run --timeout=1m --config ./.golangci.yml ./test/ || true

ruleguard:
	@echo "Running $@ check"
	@${GOPATH}/bin/ruleguard -rules ruleguard.rule.go ./... || true

verifiers: get-check-deps fmt lint ruleguard

# Builds various components locally.
build-go:
	@echo "Building each component's binary to './'"
	@echo "Building reader ..."
	@mkdir -p $(INSTALL_PATH) && GO111MODULE=on $(GO) build -o $(INSTALL_PATH)/reader $(PWD)/cmd/reader/reader.go 1>/dev/null
	@echo "Building master ..."
	@mkdir -p $(INSTALL_PATH) && GO111MODULE=on $(GO) build -o $(INSTALL_PATH)/master $(PWD)/cmd/master/main.go 1>/dev/null
	@echo "Building proxy ..."
	@mkdir -p $(INSTALL_PATH) && GO111MODULE=on $(GO) build -o $(INSTALL_PATH)/proxy $(PWD)/cmd/proxy/proxy.go 1>/dev/null

build-cpp:
	@(env bash $(PWD)/scripts/core_build.sh)

build-cpp-with-unittest:
	@(env bash $(PWD)/scripts/core_build.sh -u)

# Runs the tests.
unittest: test-cpp test-go

#TODO: proxy master reader writer's unittest
test-go: verifiers build-go
	@echo "Running go unittests..."
	@(env bash $(PWD)/scripts/run_go_unittest.sh)

test-cpp: build-cpp-with-unittest
	@echo "Running cpp unittests..."
	@(env bash $(PWD)/scripts/run_cpp_unittest.sh)

#TODO: build each component to docker
docker: verifiers
	@echo "Building reader docker image '$(TAG)'"
	@echo "Building proxy docker image '$(TAG)'"
	@echo "Building master docker image '$(TAG)'"

# Builds each component and installs it to $GOPATH/bin.
install: all
	@echo "Installing binary to './bin'"
	@mkdir -p $(GOPATH)/bin && cp -f $(PWD)/bin/reader $(GOPATH)/bin/reader
	@mkdir -p $(GOPATH)/bin && cp -f $(PWD)/bin/master $(GOPATH)/bin/master
	@mkdir -p $(GOPATH)/bin && cp -f $(PWD)/bin/proxy $(GOPATH)/bin/proxy
	@mkdir -p $(LIBRARY_PATH) && cp -f $(PWD)/internal/core/output/lib/* $(LIBRARY_PATH)
	@echo "Installation successful."

clean:
	@echo "Cleaning up all the generated files"
	@find . -name '*.test' | xargs rm -fv
	@find . -name '*~' | xargs rm -fv
	@rm -rvf reader
	@rm -rvf master
	@rm -rvf proxy
