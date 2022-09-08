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

GO		  ?= go
PWD 	  := $(shell pwd)
GOPATH	:= $(shell $(GO) env GOPATH)
SHELL 	:= /bin/bash
OBJPREFIX := "github.com/milvus-io/milvus/cmd/milvus"

INSTALL_PATH := $(PWD)/bin
LIBRARY_PATH := $(PWD)/lib
OS := $(shell uname -s)
ARCH := $(shell arch)
mode = Release

all: build-cpp build-go

get-build-deps:
	@(env bash $(PWD)/scripts/install_deps.sh)

# attention: upgrade golangci-lint should also change Dockerfiles in build/docker/builder/cpu/<os>
getdeps:
	@mkdir -p ${GOPATH}/bin
	@which golangci-lint 1>/dev/null || (echo "Installing golangci-lint" && curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin v1.46.2)

tools/bin/revive: tools/check/go.mod
	cd tools/check; \
	$(GO) build -o ../bin/revive github.com/mgechev/revive

cppcheck:
	@(env bash ${PWD}/scripts/core_build.sh -l)

generated-proto-go: export protoc:=${PWD}/cmake_build/thirdparty/protobuf/protobuf-build/protoc
generated-proto-go: build-cpp
	@mkdir -p ${GOPATH}/bin
	@which protoc-gen-go 1>/dev/null || (echo "Installing protoc-gen-go" && cd /tmp && go install github.com/golang/protobuf/protoc-gen-go@v1.3.2)
	@(env bash $(PWD)/scripts/proto_gen_go.sh)

check-proto-product: generated-proto-go
	@(env bash $(PWD)/scripts/check_proto_product.sh)

fmt:
ifdef GO_DIFF_FILES
	@echo "Running $@ check"
	@GO111MODULE=on env bash $(PWD)/scripts/gofmt.sh $(GO_DIFF_FILES)
else
	@echo "Running $@ check"
	@GO111MODULE=on env bash $(PWD)/scripts/gofmt.sh cmd/
	@GO111MODULE=on env bash $(PWD)/scripts/gofmt.sh internal/
	@GO111MODULE=on env bash $(PWD)/scripts/gofmt.sh tests/go/
endif

lint: tools/bin/revive
	@echo "Running $@ check"
	@tools/bin/revive -formatter friendly -config tools/check/revive.toml ./...

#TODO: Check code specifications by golangci-lint
static-check:
	@echo "Running $@ check"
	@GO111MODULE=on ${GOPATH}/bin/golangci-lint cache clean
	@source $(PWD)/scripts/setenv.sh && GO111MODULE=on ${GOPATH}/bin/golangci-lint run --timeout=30m --config ./.golangci.yml ./internal/...
	@source $(PWD)/scripts/setenv.sh && GO111MODULE=on ${GOPATH}/bin/golangci-lint run --timeout=30m --config ./.golangci.yml ./cmd/...
#	@GO111MODULE=on ${GOPATH}/bin/golangci-lint run --timeout=30m --config ./.golangci.yml ./tests/go_client/...

verifiers: build-cpp getdeps cppcheck fmt static-check

# Build various components locally.
binlog:
	@echo "Building binlog ..."
	@source $(PWD)/scripts/setenv.sh && \
		mkdir -p $(INSTALL_PATH) && go env -w CGO_ENABLED="1" && GO111MODULE=on $(GO) build -o $(INSTALL_PATH)/binlog $(PWD)/cmd/tools/binlog/main.go 1>/dev/null

BUILD_TAGS = $(shell git describe --tags --always --dirty="-dev")
BUILD_TIME = $(shell date -u)
GIT_COMMIT = $(shell git rev-parse --short HEAD)
GO_VERSION = $(shell go version)
ifeq ($(OS),Darwin)
ifeq ($(ARCH),arm64)
	APPLE_SILICON_FLAG = -tags dynamic
endif
endif

print-build-info:
	@echo "Build Tag: $(BUILD_TAGS)"
	@echo "Build Time: $(BUILD_TIME)"
	@echo "Git Commit: $(GIT_COMMIT)"
	@echo "Go Version: $(GO_VERSION)"

milvus: build-cpp print-build-info
	@echo "Building Milvus ..."
	@source $(PWD)/scripts/setenv.sh && \
		mkdir -p $(INSTALL_PATH) && go env -w CGO_ENABLED="1" && \
		GO111MODULE=on $(GO) build -ldflags="-r $${RPATH} -X '$(OBJPREFIX).BuildTags=$(BUILD_TAGS)' -X '$(OBJPREFIX).BuildTime=$(BUILD_TIME)' -X '$(OBJPREFIX).GitCommit=$(GIT_COMMIT)' -X '$(OBJPREFIX).GoVersion=$(GO_VERSION)'" \
		${APPLE_SILICON_FLAG} -o $(INSTALL_PATH)/milvus $(PWD)/cmd/main.go 1>/dev/null

embd-milvus: build-cpp-embd print-build-info
	@echo "Building **Embedded** Milvus ..."
	@source $(PWD)/scripts/setenv.sh && \
		mkdir -p $(INSTALL_PATH) && go env -w CGO_ENABLED="1" && \
		GO111MODULE=on $(GO) build -ldflags="-r /tmp/milvus/lib/ -X '$(OBJPREFIX).BuildTags=$(BUILD_TAGS)' -X '$(OBJPREFIX).BuildTime=$(BUILD_TIME)' -X '$(OBJPREFIX).GitCommit=$(GIT_COMMIT)' -X '$(OBJPREFIX).GoVersion=$(GO_VERSION)'" \
		${APPLE_SILICON_FLAG} -buildmode=c-shared -o $(INSTALL_PATH)/embd-milvus.so $(PWD)/pkg/embedded/embedded.go 1>/dev/null

build-go: milvus

build-cpp: 
	@echo "Building Milvus cpp library ..."
	@(env bash $(PWD)/scripts/core_build.sh -t ${mode} -f "$(CUSTOM_THIRDPARTY_PATH)")

build-cpp-embd: 
	@echo "Building **Embedded** Milvus cpp library ..."
	@(env bash $(PWD)/scripts/core_build.sh -b -t ${mode} -f "$(CUSTOM_THIRDPARTY_PATH)")

build-cpp-with-unittest: 
	@echo "Building Milvus cpp library with unittest ..."
	@(env bash $(PWD)/scripts/core_build.sh -t ${mode} -u -f "$(CUSTOM_THIRDPARTY_PATH)")

build-cpp-with-coverage: 
	@echo "Building Milvus cpp library with coverage and unittest ..."
	@(env bash $(PWD)/scripts/core_build.sh -t ${mode} -u -c -f "$(CUSTOM_THIRDPARTY_PATH)")


# Run the tests.
unittest: test-cpp test-go

test-util:
	@echo "Running go unittests..."
	@(env bash $(PWD)/scripts/run_go_unittest.sh -t util)

test-storage:
	@echo "Running go unittests..."
	@(env bash $(PWD)/scripts/run_go_unittest.sh -t storage)

test-allocator:
	@echo "Running go unittests..."
	@(env bash $(PWD)/scripts/run_go_unittest.sh -t allocator)

test-config:
	@echo "Running go unittests..."
	@(env bash $(PWD)/scripts/run_go_unittest.sh -t config)

test-tso:
	@echo "Running go unittests..."
	@(env bash $(PWD)/scripts/run_go_unittest.sh -t tso)

test-kv:
	@echo "Running go unittests..."
	@(env bash $(PWD)/scripts/run_go_unittest.sh -t kv)

test-mq:
	@echo "Running go unittests..."
	@(env bash $(PWD)/scripts/run_go_unittest.sh -t mq)

test-rootcoord:
	@echo "Running go unittests..."
	@(env bash $(PWD)/scripts/run_go_unittest.sh -t rootcoord)

test-indexnode:
	@echo "Running go unittests..."
	@(env bash $(PWD)/scripts/run_go_unittest.sh -t indexnode)

test-proxy:
	@echo "Running go unittests..."
	@(env bash $(PWD)/scripts/run_go_unittest.sh -t proxy)

test-datacoord:
	@echo "Running go unittests..."
	@(env bash $(PWD)/scripts/run_go_unittest.sh -t datacoord)

test-datanode:
	@echo "Running go unittests..."
	@(env bash $(PWD)/scripts/run_go_unittest.sh -t datanode)

test-querynode:
	@echo "Running go unittests..."
	@(env bash $(PWD)/scripts/run_go_unittest.sh -t querynode)

test-querycoord:
	@echo "Running go unittests..."
	@(env bash $(PWD)/scripts/run_go_unittest.sh -t querycoord)

test-metastore:
	@echo "Running go unittests..."
	@(env bash $(PWD)/scripts/run_go_unittest.sh -t metastore)

test-go: build-cpp-with-unittest
	@echo "Running go unittests..."
	@(env bash $(PWD)/scripts/run_go_unittest.sh)

test-cpp: build-cpp-with-unittest
	@echo "Running cpp unittests..."
	@(env bash $(PWD)/scripts/run_cpp_unittest.sh)

# Run code coverage.
codecov: codecov-go codecov-cpp

# Run codecov-go
codecov-go: build-cpp-with-coverage
	@echo "Running go coverage..."
	@(env bash $(PWD)/scripts/run_go_codecov.sh)

# Run codecov-cpp
codecov-cpp: build-cpp-with-coverage
	@echo "Running cpp coverage..."
	@(env bash $(PWD)/scripts/run_cpp_codecov.sh)

# Package docker image locally.
# TODO: fix error occur at starting up
docker: install
	./build/build_image.sh

# Build each component and install binary to $GOPATH/bin.
install: all
	@echo "Installing binary to './bin'"
	@mkdir -p $(GOPATH)/bin && cp -f $(PWD)/bin/milvus $(GOPATH)/bin/milvus
	@mkdir -p $(LIBRARY_PATH) && cp -r -P $(PWD)/internal/core/output/lib/*.so* $(LIBRARY_PATH)
	@echo "Installation successful."

clean:
	@echo "Cleaning up all the generated files"
	@find . -name '*.test' | xargs rm -fv
	@find . -name '*~' | xargs rm -fv
	@rm -rf bin/
	@rm -rf lib/
	@rm -rf $(GOPATH)/bin/milvus
	@rm -rf cmake_build
	@rm -rf cwrapper_build
	@rm -rf internal/core/output

milvus-tools: print-build-info
	@echo "Building tools ..."
	@mkdir -p $(INSTALL_PATH)/tools && go env -w CGO_ENABLED="1" && GO111MODULE=on $(GO) build \
		-ldflags="-X 'main.BuildTags=$(BUILD_TAGS)' -X 'main.BuildTime=$(BUILD_TIME)' -X 'main.GitCommit=$(GIT_COMMIT)' -X 'main.GoVersion=$(GO_VERSION)'" \
		-o $(INSTALL_PATH)/tools $(PWD)/cmd/tools/* 1>/dev/null

rpm-setup:
	@echo "Setuping rpm env ...;"
	@build/rpm/setup-env.sh

rpm: install
	@echo "Note: run 'make rpm-setup' to setup build env for rpm builder"
	@echo "Building rpm ...;"
	@yum -y install rpm-build rpmdevtools wget
	@rm -rf ~/rpmbuild/BUILD/*
	@rpmdev-setuptree
	@wget https://github.com/etcd-io/etcd/releases/download/v3.5.0/etcd-v3.5.0-linux-amd64.tar.gz && tar -xf etcd-v3.5.0-linux-amd64.tar.gz
	@cp etcd-v3.5.0-linux-amd64/etcd bin/etcd
	@wget https://dl.min.io/server/minio/release/linux-amd64/archive/minio.RELEASE.2021-02-14T04-01-33Z -O bin/minio
	@cp -r bin ~/rpmbuild/BUILD/
	@cp -r lib ~/rpmbuild/BUILD/
	@cp -r configs ~/rpmbuild/BUILD/
	@cp -r build/rpm/services ~/rpmbuild/BUILD/
	@QA_RPATHS="$$[ 0x001|0x0002|0x0020 ]" rpmbuild -ba ./build/rpm/milvus.spec
