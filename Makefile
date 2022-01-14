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
OS := $(shell uname -s)
ARCH := $(shell arch)

all: build-cpp build-go

get-build-deps:
	@(env bash $(PWD)/scripts/install_deps.sh)

# attention: upgrade golangci-lint should also change Dockerfiles in build/docker/builder/cpu/<os>
getdeps:
	@mkdir -p ${GOPATH}/bin
	@which golangci-lint 1>/dev/null || (echo "Installing golangci-lint" && curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin v1.43.0)
	@which ruleguard 1>/dev/null || (echo "Installing ruleguard" && go get github.com/quasilyte/go-ruleguard/cmd/ruleguard@v0.2.1)

tools/bin/revive: tools/check/go.mod
	cd tools/check; \
	$(GO) build -o ../bin/revive github.com/mgechev/revive

cppcheck:
	@(env bash ${PWD}/scripts/core_build.sh -l)

generated-proto-go: export protoc:=${PWD}/cmake_build/thirdparty/protobuf/protobuf-build/protoc
generated-proto-go: build-cpp
	@mkdir -p ${GOPATH}/bin
	@which protoc-gen-go 1>/dev/null || (echo "Installing protoc-gen-go" && go get github.com/golang/protobuf/protoc-gen-go@v1.3.2)
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
	@GO111MODULE=on ${GOPATH}/bin/golangci-lint run --timeout=30m --config ./.golangci.yml ./internal/...
	@GO111MODULE=on ${GOPATH}/bin/golangci-lint run --timeout=30m --config ./.golangci.yml ./cmd/...
#	@GO111MODULE=on ${GOPATH}/bin/golangci-lint run --timeout=30m --config ./.golangci.yml ./tests/go_client/...

ruleguard:
ifdef GO_DIFF_FILES
	@echo "Running $@ check"
	@${GOPATH}/bin/ruleguard -rules ruleguard.rules.go $(GO_DIFF_FILES)
else
	@echo "Running $@ check"
ifeq ($(OS),Darwin) # MacOS X
ifeq ($(ARCH),arm64)
	@${GOPATH}/bin/darwin_arm64/ruleguard -rules ruleguard.rules.go ./internal/...
	@${GOPATH}/bin/darwin_arm64/ruleguard -rules ruleguard.rules.go ./cmd/...
else
	@${GOPATH}/bin/ruleguard -rules ruleguard.rules.go ./internal/...
	@${GOPATH}/bin/ruleguard -rules ruleguard.rules.go ./cmd/...
endif
else
	@${GOPATH}/bin/ruleguard -rules ruleguard.rules.go ./internal/...
	@${GOPATH}/bin/ruleguard -rules ruleguard.rules.go ./cmd/...
endif
	#@${GOPATH}/bin/ruleguard -rules ruleguard.rules.go ./tests/go/...
endif

verifiers: build-cpp getdeps cppcheck fmt static-check ruleguard

# Build various components locally.
binlog:
	@echo "Building binlog ..."
	@mkdir -p $(INSTALL_PATH) && go env -w CGO_ENABLED="1" && GO111MODULE=on $(GO) build -o $(INSTALL_PATH)/binlog $(PWD)/cmd/tools/binlog/main.go 1>/dev/null

BUILD_TAGS = $(shell git describe --tags --always --dirty="-dev")
BUILD_TIME = $(shell date -u)
GIT_COMMIT = $(shell git rev-parse --short HEAD)
GO_VERSION = $(shell go version)

print-build-info:
	@echo "Build Tag: $(BUILD_TAGS)"
	@echo "Build Time: $(BUILD_TIME)"
	@echo "Git Commit: $(GIT_COMMIT)"
	@echo "Go Version: $(GO_VERSION)"

milvus: build-cpp print-build-info
	@echo "Building Milvus ..."
	@mkdir -p $(INSTALL_PATH) && go env -w CGO_ENABLED="1" && GO111MODULE=on $(GO) build \
		-ldflags="-X 'main.BuildTags=$(BUILD_TAGS)' -X 'main.BuildTime=$(BUILD_TIME)' -X 'main.GitCommit=$(GIT_COMMIT)' -X 'main.GoVersion=$(GO_VERSION)'" \
		-o $(INSTALL_PATH)/milvus $(PWD)/cmd/main.go 1>/dev/null

build-go: milvus

build-cpp:
	@echo "Building Milvus cpp library ..."
	@(env bash $(PWD)/scripts/core_build.sh -f "$(CUSTOM_THIRDPARTY_PATH)")
	@(env bash $(PWD)/scripts/cwrapper_build.sh -t Release -f "$(CUSTOM_THIRDPARTY_PATH)")
	@(env bash $(PWD)/scripts/cwrapper_rocksdb_build.sh -t Release -f "$(CUSTOM_THIRDPARTY_PATH)")

build-cpp-with-unittest:
	@echo "Building Milvus cpp library with unittest ..."
	@(env bash $(PWD)/scripts/core_build.sh -u -c -f "$(CUSTOM_THIRDPARTY_PATH)")
	@(env bash $(PWD)/scripts/cwrapper_build.sh -t Release -f "$(CUSTOM_THIRDPARTY_PATH)")
	@(env bash $(PWD)/scripts/cwrapper_rocksdb_build.sh -t Release -f "$(CUSTOM_THIRDPARTY_PATH)")

# Run the tests.
unittest: test-cpp test-go

test-proxy:
	@echo "Running go unittests..."
	go test -race -coverpkg=./... -coverprofile=profile.out -covermode=atomic -timeout 5m github.com/milvus-io/milvus/internal/proxy -v

test-datanode:
	@echo "Running go unittests..."
	go test -race -coverpkg=./... -coverprofile=profile.out -covermode=atomic -timeout 5m github.com/milvus-io/milvus/internal/datanode -v

test-querycoord:
	@echo "Running go unittests..."
	go test -race -coverpkg=./... -coverprofile=profile.out -covermode=atomic -timeout 5m github.com/milvus-io/milvus/internal/querycoord	-v


test-go: build-cpp-with-unittest
	@echo "Running go unittests..."
	@(env bash $(PWD)/scripts/run_go_unittest.sh)

test-cpp: build-cpp-with-unittest
	@echo "Running cpp unittests..."
	@(env bash $(PWD)/scripts/run_cpp_unittest.sh)

# Run code coverage.
codecov: codecov-go codecov-cpp

# Run codecov-go
codecov-go: build-cpp-with-unittest
	@echo "Running go coverage..."
	@(env bash $(PWD)/scripts/run_go_codecov.sh)

# Run codecov-cpp
codecov-cpp: build-cpp-with-unittest
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
	@mkdir -p $(LIBRARY_PATH) && cp -P $(PWD)/internal/core/output/lib/* $(LIBRARY_PATH)
	@echo "Installation successful."

clean:
	@echo "Cleaning up all the generated files"
	@find . -name '*.test' | xargs rm -fv
	@find . -name '*~' | xargs rm -fv
	@rm -rf bin/
	@rm -rf lib/
	@rm -rf $(GOPATH)/bin/milvus

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
