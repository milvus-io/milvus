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

get-build-deps: ## Get required build dependencies
	@(env bash $(PWD)/scripts/install_deps.sh)

getdeps: ## Get dependencies
	@mkdir -p ${GOPATH}/bin
	@which golangci-lint 1>/dev/null || (echo "Installing golangci-lint" && curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin v1.27.0)
	@which ruleguard 1>/dev/null || (echo "Installing ruleguard" && go get github.com/quasilyte/go-ruleguard/cmd/ruleguard@v0.2.1)

tools/bin/revive: tools/check/go.mod ## Build and invoke ../bin/revive
	cd tools/check; \
	$(GO) build -o ../bin/revive github.com/mgechev/revive

cppcheck: ## Perfom checks for cpp
	@(env bash ${PWD}/scripts/core_build.sh -l)

generated-proto-go: export protoc:=${PWD}/cmake_build/thirdparty/protobuf/protobuf-build/protoc ## Generate go protobuf
generated-proto-go: build-cpp
	@mkdir -p ${GOPATH}/bin
	@which protoc-gen-go 1>/dev/null || (echo "Installing protoc-gen-go" && go get github.com/golang/protobuf/protoc-gen-go@v1.3.2)
	@(env bash $(PWD)/scripts/proto_gen_go.sh)

check-proto-product: generated-proto-go ## Check generated go protobuf
	@(env bash $(PWD)/scripts/check_proto_product.sh)

fmt: ## Perform formating
ifdef GO_DIFF_FILES
	@echo "Running $@ check"
	@GO111MODULE=on env bash $(PWD)/scripts/gofmt.sh $(GO_DIFF_FILES)
else
	@echo "Running $@ check"
	@GO111MODULE=on env bash $(PWD)/scripts/gofmt.sh cmd/
	@GO111MODULE=on env bash $(PWD)/scripts/gofmt.sh internal/
	@GO111MODULE=on env bash $(PWD)/scripts/gofmt.sh tests/go/
endif

lint: tools/bin/revive ## Run linters
	@echo "Running $@ check"
	@tools/bin/revive -formatter friendly -config tools/check/revive.toml ./...

#TODO: Check code specifications by golangci-lint
static-check: ## Perform static check
	@echo "Running $@ check"
	@GO111MODULE=on ${GOPATH}/bin/golangci-lint cache clean
	@GO111MODULE=on ${GOPATH}/bin/golangci-lint run --timeout=30m --config ./.golangci.yml ./internal/...
	@GO111MODULE=on ${GOPATH}/bin/golangci-lint run --timeout=30m --config ./.golangci.yml ./cmd/...
#	@GO111MODULE=on ${GOPATH}/bin/golangci-lint run --timeout=30m --config ./.golangci.yml ./tests/go_client/...

ruleguard: ## Execute ruleguard
ifdef GO_DIFF_FILES
	@echo "Running $@ check"
	@${GOPATH}/bin/ruleguard -rules ruleguard.rules.go $(GO_DIFF_FILES)
else
	@echo "Running $@ check"
	@${GOPATH}/bin/ruleguard -rules ruleguard.rules.go ./internal/...
	@${GOPATH}/bin/ruleguard -rules ruleguard.rules.go ./cmd/...
#	@${GOPATH}/bin/ruleguard -rules ruleguard.rules.go ./tests/go/...
endif

verifiers: build-cpp getdeps cppcheck fmt static-check ruleguard ## Run Verifiers

binlog: ## Build various components locally
	@echo "Building binlog ..."
	@mkdir -p $(INSTALL_PATH) && go env -w CGO_ENABLED="1" && GO111MODULE=on $(GO) build -o $(INSTALL_PATH)/binlog $(PWD)/cmd/tools/binlog/main.go 1>/dev/null

BUILD_TAGS = $(shell git describe --tags --always --dirty="-dev")
BUILD_TIME = $(shell date --utc)
GIT_COMMIT = $(shell git rev-parse --short HEAD)
GO_VERSION = $(shell go version)

milvus: build-cpp ## Build Milvus
	@echo "Building Milvus ..."
	@mkdir -p $(INSTALL_PATH) && go env -w CGO_ENABLED="1" && GO111MODULE=on $(GO) build \
		-ldflags="-X 'main.BuildTags=$(BUILD_TAGS)' -X 'main.BuildTime=$(BUILD_TIME)' -X 'main.GitCommit=$(GIT_COMMIT)' -X 'main.GoVersion=$(GO_VERSION)'" \
		-o $(INSTALL_PATH)/milvus $(PWD)/cmd/main.go 1>/dev/null

build-go: milvus ## Build Milvus (alias)

build-cpp: ## Build cpp
	@echo "Building Milvus cpp library ..."
	@(env bash $(PWD)/scripts/core_build.sh -f "$(CUSTOM_THIRDPARTY_PATH)")
	@(env bash $(PWD)/scripts/cwrapper_build.sh -t Release -f "$(CUSTOM_THIRDPARTY_PATH)")
	@(env bash $(PWD)/scripts/cwrapper_dablooms_build.sh -t Release -f "$(CUSTOM_THIRDPARTY_PATH)")
	@(env bash $(PWD)/scripts/cwrapper_rocksdb_build.sh -t Release -f "$(CUSTOM_THIRDPARTY_PATH)")

build-cpp-with-unittest: ## Build cpp with unittest
	@echo "Building Milvus cpp library with unittest ..."
	@(env bash $(PWD)/scripts/core_build.sh -u -c -f "$(CUSTOM_THIRDPARTY_PATH)")
	@(env bash $(PWD)/scripts/cwrapper_build.sh -t Release -f "$(CUSTOM_THIRDPARTY_PATH)")

unittest: test-cpp test-go ## Run the tests

test-go: build-cpp-with-unittest ## Run test-go
	@echo "Running go unittests..."
	@(env bash $(PWD)/scripts/run_go_codecov.sh)
# 	@(env bash $(PWD)/scripts/run_go_unittest.sh)

test-cpp: build-cpp-with-unittest ## Run test-cpp
	@echo "Running cpp unittests..."
	@(env bash $(PWD)/scripts/run_cpp_unittest.sh)

codecov: codecov-go codecov-cpp ## Run code coverage

codecov-go: build-cpp-with-unittest ## Run codecov-go
	@echo "Running go coverage..."
	@(env bash $(PWD)/scripts/run_go_codecov.sh)

codecov-cpp: build-cpp-with-unittest ## Run codecov-cpp
	@echo "Running cpp coverage..."
	@(env bash $(PWD)/scripts/run_cpp_codecov.sh)

# TODO: fix error occur at starting up
docker: install ## Package docker image locally
	./build/build_image.sh

install: all ## Build each component and install binary to $GOPATH/bin
	@echo "Installing binary to './bin'"
	@mkdir -p $(GOPATH)/bin && cp -f $(PWD)/bin/milvus $(GOPATH)/bin/milvus
	@mkdir -p $(LIBRARY_PATH) && cp -P $(PWD)/internal/core/output/lib/* $(LIBRARY_PATH)
	@echo "Installation successful."

clean: ## Perform clean-ups
	@echo "Cleaning up all the generated files"
	@find . -name '*.test' | xargs rm -fv
	@find . -name '*~' | xargs rm -fv
	@rm -rf bin/
	@rm -rf lib/
	@rm -rf $(GOPATH)/bin/milvus

milvus-tools: ## Build Milvus tools
	@echo "Building tools ..."
	@mkdir -p $(INSTALL_PATH)/tools && go env -w CGO_ENABLED="1" && GO111MODULE=on $(GO) build \
		-ldflags="-X 'main.BuildTags=$(BUILD_TAGS)' -X 'main.BuildTime=$(BUILD_TIME)' -X 'main.GitCommit=$(GIT_COMMIT)' -X 'main.GoVersion=$(GO_VERSION)'" \
		-o $(INSTALL_PATH)/tools $(PWD)/cmd/tools/* 1>/dev/null

help: SHELL := /bin/sh
help: ## List available commands and their usage
	@awk 'BEGIN {FS = ":.*?##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} /^[\/0-9a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2 } ' $(MAKEFILE_LIST)