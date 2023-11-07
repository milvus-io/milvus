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
mode = Release

use_disk_index = OFF
ifdef disk_index
	use_disk_index = ${disk_index}
endif

use_asan = OFF
ifdef USE_ASAN
	use_asan =${USE_ASAN}
endif

use_dynamic_simd = OFF
ifdef USE_DYNAMIC_SIMD
	use_dynamic_simd = ${USE_DYNAMIC_SIMD}
endif
# golangci-lint
GOLANGCI_LINT_VERSION := 1.53.1
GOLANGCI_LINT_OUTPUT := $(shell $(INSTALL_PATH)/golangci-lint --version 2>/dev/null)
INSTALL_GOLANGCI_LINT := $(findstring $(GOLANGCI_LINT_VERSION), $(GOLANGCI_LINT_OUTPUT))
# mockery
MOCKERY_VERSION := 2.32.4
MOCKERY_OUTPUT := $(shell $(INSTALL_PATH)/mockery --version 2>/dev/null)
INSTALL_MOCKERY := $(findstring $(MOCKERY_VERSION),$(MOCKERY_OUTPUT))
# gci
GCI_VERSION := 0.11.2
GCI_OUTPUT := $(shell $(INSTALL_PATH)/gci --version 2>/dev/null)
INSTALL_GCI := $(findstring $(GCI_VERSION),$(GCI_OUTPUT))
# gofumpt
GOFUMPT_VERSION := 0.5.0
GOFUMPT_OUTPUT := $(shell $(INSTALL_PATH)/gofumpt --version 2>/dev/null)
INSTALL_GOFUMPT := $(findstring $(GOFUMPT_VERSION),$(GOFUMPT_OUTPUT))

index_engine = knowhere

export GIT_BRANCH=master

ifeq (${ENABLE_AZURE}, false)
	AZURE_OPTION := -Z
endif

milvus: build-cpp print-build-info
	@echo "Building Milvus ..."
	@source $(PWD)/scripts/setenv.sh && \
		mkdir -p $(INSTALL_PATH) && go env -w CGO_ENABLED="1" && \
		GO111MODULE=on $(GO) build -ldflags="-r $${RPATH} -X '$(OBJPREFIX).BuildTags=$(BUILD_TAGS)' -X '$(OBJPREFIX).BuildTime=$(BUILD_TIME)' -X '$(OBJPREFIX).GitCommit=$(GIT_COMMIT)' -X '$(OBJPREFIX).GoVersion=$(GO_VERSION)'" \
		-tags dynamic -o $(INSTALL_PATH)/milvus $(PWD)/cmd/main.go 1>/dev/null

milvus-gpu: build-cpp-gpu print-gpu-build-info
	@echo "Building Milvus-gpu ..."
	@source $(PWD)/scripts/setenv.sh && \
		mkdir -p $(INSTALL_PATH) && go env -w CGO_ENABLED="1" && \
		GO111MODULE=on $(GO) build -ldflags="-r $${RPATH} -X '$(OBJPREFIX).BuildTags=$(BUILD_TAGS_GPU)' -X '$(OBJPREFIX).BuildTime=$(BUILD_TIME)' -X '$(OBJPREFIX).GitCommit=$(GIT_COMMIT)' -X '$(OBJPREFIX).GoVersion=$(GO_VERSION)'" \
		-tags dynamic -o $(INSTALL_PATH)/milvus $(PWD)/cmd/main.go 1>/dev/null

get-build-deps:
	@(env bash $(PWD)/scripts/install_deps.sh)

# attention: upgrade golangci-lint should also change Dockerfiles in build/docker/builder/cpu/<os>
getdeps:
	@mkdir -p $(INSTALL_PATH)
	@if [ -z "$(INSTALL_GOLANGCI_LINT)" ]; then \
		echo "Installing golangci-lint into ./bin/" && curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(INSTALL_PATH) v1.53.1 ; \
	else \
		echo "golangci-lint v@$(GOLANGCI_LINT_VERSION) already installed"; \
	fi
	@if [ -z "$(INSTALL_MOCKERY)" ]; then \
		echo "Installing mockery v$(MOCKERY_VERSION) to ./bin/" && GOBIN=$(INSTALL_PATH) go install github.com/vektra/mockery/v2@v$(MOCKERY_VERSION); \
	else \
		echo "Mockery v$(MOCKERY_VERSION) already installed"; \
	fi

tools/bin/revive: tools/check/go.mod
	cd tools/check; \
	$(GO) build -o ../bin/revive github.com/mgechev/revive

cppcheck:
	@(env bash ${PWD}/scripts/core_build.sh -l)


fmt:
ifdef GO_DIFF_FILES
	@echo "Running $@ check"
	@GO111MODULE=on env bash $(PWD)/scripts/gofmt.sh $(GO_DIFF_FILES)
else
	@echo "Running $@ check"
	@GO111MODULE=on env bash $(PWD)/scripts/gofmt.sh cmd/
	@GO111MODULE=on env bash $(PWD)/scripts/gofmt.sh internal/
	@GO111MODULE=on env bash $(PWD)/scripts/gofmt.sh tests/integration/
	@GO111MODULE=on env bash $(PWD)/scripts/gofmt.sh tests/go/
	@GO111MODULE=on env bash $(PWD)/scripts/gofmt.sh pkg/
endif

lint-fix: getdeps
	@mkdir -p $(INSTALL_PATH)
	@if [ -z "$(INSTALL_GCI)" ]; then \
		echo "Installing gci v$(GCI_VERSION) to ./bin/" && GOBIN=$(INSTALL_PATH) go install github.com/daixiang0/gci@v$(GCI_VERSION); \
	else \
		echo "gci v$(GCI_VERSION) already installed"; \
	fi
	@if [ -z "$(INSTALL_GOFUMPT)" ]; then \
		echo "Installing gofumpt v$(GOFUMPT_VERSION) to ./bin/" && GOBIN=$(INSTALL_PATH) go install mvdan.cc/gofumpt@v$(GOFUMPT_VERSION); \
	else \
		echo "gofumpt v$(GOFUMPT_VERSION) already installed"; \
	fi
	@echo "Running gofumpt fix"
	@$(INSTALL_PATH)/gofumpt -l -w internal/
	@$(INSTALL_PATH)/gofumpt -l -w cmd/
	@$(INSTALL_PATH)/gofumpt -l -w pkg/
	@$(INSTALL_PATH)/gofumpt -l -w tests/integration/
	@echo "Running gci fix"
	@$(INSTALL_PATH)/gci write cmd/ --skip-generated -s standard -s default -s "prefix(github.com/milvus-io)" --custom-order
	@$(INSTALL_PATH)/gci write internal/ --skip-generated -s standard -s default -s "prefix(github.com/milvus-io)" --custom-order
	@$(INSTALL_PATH)/gci write pkg/ --skip-generated -s standard -s default -s "prefix(github.com/milvus-io)" --custom-order
	@$(INSTALL_PATH)/gci write tests/ --skip-generated -s standard -s default -s "prefix(github.com/milvus-io)" --custom-order
	@echo "Running golangci-lint auto-fix"
	@source $(PWD)/scripts/setenv.sh && GO111MODULE=on $(INSTALL_PATH)/golangci-lint run --fix --timeout=30m --config $(PWD)/.golangci.yml; cd pkg && GO111MODULE=on $(INSTALL_PATH)/golangci-lint run --fix --timeout=30m --config $(PWD)/.golangci.yml

#TODO: Check code specifications by golangci-lint
static-check: getdeps
	@echo "Running $@ check"
	@source $(PWD)/scripts/setenv.sh && GO111MODULE=on $(INSTALL_PATH)/golangci-lint run --timeout=30m --config $(PWD)/.golangci.yml
	@source $(PWD)/scripts/setenv.sh && cd pkg && GO111MODULE=on $(INSTALL_PATH)/golangci-lint run --timeout=30m --config $(PWD)/.golangci.yml

verifiers: build-cpp getdeps cppcheck fmt static-check

# Build various components locally.
binlog:
	@echo "Building binlog ..."
	@source $(PWD)/scripts/setenv.sh && \
		mkdir -p $(INSTALL_PATH) && go env -w CGO_ENABLED="1" && \
		GO111MODULE=on $(GO) build -ldflags="-r $${RPATH}" -o $(INSTALL_PATH)/binlog $(PWD)/cmd/tools/binlog/main.go 1>/dev/null

MIGRATION_PATH = $(PWD)/cmd/tools/migration
meta-migration:
	@echo "Building migration tool ..."
	@source $(PWD)/scripts/setenv.sh && \
    		mkdir -p $(INSTALL_PATH) && go env -w CGO_ENABLED="1" && \
    		GO111MODULE=on $(GO) build -ldflags="-r $${RPATH} -X '$(OBJPREFIX).BuildTags=$(BUILD_TAGS)' -X '$(OBJPREFIX).BuildTime=$(BUILD_TIME)' -X '$(OBJPREFIX).GitCommit=$(GIT_COMMIT)' -X '$(OBJPREFIX).GoVersion=$(GO_VERSION)'" \
    		-tags dynamic -o $(INSTALL_PATH)/meta-migration $(MIGRATION_PATH)/main.go 1>/dev/null

INTERATION_PATH = $(PWD)/tests/integration
integration-test:
	@echo "Building integration tests ..."
	@source $(PWD)/scripts/setenv.sh && \
    		mkdir -p $(INSTALL_PATH) && go env -w CGO_ENABLED="1" && \
    		GO111MODULE=on $(GO) build -ldflags="-r $${RPATH} -X '$(OBJPREFIX).BuildTags=$(BUILD_TAGS)' -X '$(OBJPREFIX).BuildTime=$(BUILD_TIME)' -X '$(OBJPREFIX).GitCommit=$(GIT_COMMIT)' -X '$(OBJPREFIX).GoVersion=$(GO_VERSION)'" \
    		-tags dynamic -o $(INSTALL_PATH)/integration-test $(INTERATION_PATH)/ 1>/dev/null

BUILD_TAGS = $(shell git describe --tags --always --dirty="-dev")
BUILD_TAGS_GPU = ${BUILD_TAGS}-gpu
BUILD_TIME = $(shell date -u)
GIT_COMMIT = $(shell git rev-parse --short HEAD)
GO_VERSION = $(shell go version)

print-build-info:
	$(shell git config --global --add safe.directory '*')
	@echo "Build Tag: $(BUILD_TAGS)"
	@echo "Build Time: $(BUILD_TIME)"
	@echo "Git Commit: $(GIT_COMMIT)"
	@echo "Go Version: $(GO_VERSION)"

print-gpu-build-info:
	$(shell git config --global --add safe.directory '*')
	@echo "Build Tag: $(BUILD_TAGS_GPU)"
	@echo "Build Time: $(BUILD_TIME)"
	@echo "Git Commit: $(GIT_COMMIT)"
	@echo "Go Version: $(GO_VERSION)"

update-milvus-api: download-milvus-proto
	@echo "Update milvus/api version ..."
	@(env bash $(PWD)/scripts/update-api-version.sh $(PROTO_API_VERSION))

download-milvus-proto:
	@echo "Download milvus-proto repo ..."
	@(env bash $(PWD)/scripts/download_milvus_proto.sh)

build-3rdparty:
	@echo "Build 3rdparty ..."
	@(env bash $(PWD)/scripts/3rdparty_build.sh)

generated-proto: download-milvus-proto build-3rdparty
	@echo "Generate proto ..."
	@mkdir -p ${GOPATH}/bin
	@which protoc-gen-go 1>/dev/null || (echo "Installing protoc-gen-go" && cd /tmp && go install github.com/golang/protobuf/protoc-gen-go@v1.3.2)
	@(env bash $(PWD)/scripts/generate_proto.sh)

build-cpp: generated-proto
	@echo "Building Milvus cpp library ..."
	@(env bash $(PWD)/scripts/core_build.sh -t ${mode} -n ${use_disk_index} -y ${use_dynamic_simd} ${AZURE_OPTION} -x ${index_engine})

build-cpp-gpu: generated-proto
	@echo "Building Milvus cpp gpu library ... "
	@(env bash $(PWD)/scripts/core_build.sh -t ${mode} -g -n ${use_disk_index} -y ${use_dynamic_simd} ${AZURE_OPTION} -x ${index_engine})

build-cpp-with-unittest: generated-proto
	@echo "Building Milvus cpp library with unittest ... "
	@(env bash $(PWD)/scripts/core_build.sh -t ${mode} -u -n ${use_disk_index} -y ${use_dynamic_simd} ${AZURE_OPTION} -x ${index_engine})

build-cpp-with-coverage: generated-proto
	@echo "Building Milvus cpp library with coverage and unittest ..."
	@(env bash $(PWD)/scripts/core_build.sh -t ${mode} -a ${use_asan} -u -c -n ${use_disk_index} -y ${use_dynamic_simd} ${AZURE_OPTION} -x ${index_engine})

check-proto-product: generated-proto
	 @(env bash $(PWD)/scripts/check_proto_product.sh)


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

test-indexcoord:
	@echo "Running go unittests..."
	@(env bash $(PWD)/scripts/run_go_unittest.sh -t indexcoord)

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

# Build each component and install binary to $GOPATH/bin.
install: milvus
	@echo "Installing binary to './bin'"
	@mkdir -p $(GOPATH)/bin && cp -f $(PWD)/bin/milvus $(GOPATH)/bin/milvus
	@mkdir -p $(LIBRARY_PATH)
	-cp -r -P $(PWD)/internal/core/output/lib/*.dylib* $(LIBRARY_PATH) 2>/dev/null
	-cp -r -P $(PWD)/internal/core/output/lib/*.so* $(LIBRARY_PATH) 2>/dev/null
	-cp -r -P $(PWD)/internal/core/output/lib64/*.so* $(LIBRARY_PATH) 2>/dev/null
	@echo "Installation successful."

gpu-install: milvus-gpu
	@echo "Installing binary to './bin'"
	@mkdir -p $(GOPATH)/bin && cp -f $(PWD)/bin/milvus $(GOPATH)/bin/milvus
	@mkdir -p $(LIBRARY_PATH)
	-cp -r -P $(PWD)/internal/core/output/lib/*.dylib* $(LIBRARY_PATH) 2>/dev/null
	-cp -r -P $(PWD)/internal/core/output/lib/*.so* $(LIBRARY_PATH) 2>/dev/null
	-cp -r -P $(PWD)/internal/core/output/lib64/*.so* $(LIBRARY_PATH) 2>/dev/null
	@echo "Installation successful."

clean:
	@echo "Cleaning up all the generated files"
	@rm -rf bin/
	@rm -rf lib/
	@rm -rf $(GOPATH)/bin/milvus
	@rm -rf cmake_build
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

generate-mockery-types: getdeps
	# RootCoord
	$(INSTALL_PATH)/mockery --name=RootCoordComponent --dir=$(PWD)/internal/types --output=$(PWD)/internal/mocks --filename=mock_rootcoord.go --with-expecter --structname=RootCoord
	# Proxy
	$(INSTALL_PATH)/mockery --name=ProxyComponent --dir=$(PWD)/internal/types --output=$(PWD)/internal/mocks --filename=mock_proxy.go --with-expecter --structname=MockProxy
	# QueryCoord
	$(INSTALL_PATH)/mockery --name=QueryCoordComponent --dir=$(PWD)/internal/types --output=$(PWD)/internal/mocks --filename=mock_querycoord.go --with-expecter --structname=MockQueryCoord
	# QueryNode
	$(INSTALL_PATH)/mockery --name=QueryNodeComponent --dir=$(PWD)/internal/types --output=$(PWD)/internal/mocks --filename=mock_querynode.go --with-expecter --structname=MockQueryNode
	# DataCoord
	$(INSTALL_PATH)/mockery --name=DataCoordComponent --dir=$(PWD)/internal/types --output=$(PWD)/internal/mocks --filename=mock_datacoord.go --with-expecter --structname=MockDataCoord
	# DataNode
	$(INSTALL_PATH)/mockery --name=DataNodeComponent --dir=$(PWD)/internal/types --output=$(PWD)/internal/mocks --filename=mock_datanode.go --with-expecter --structname=MockDataNode
	# IndexNode
	$(INSTALL_PATH)/mockery --name=IndexNodeComponent --dir=$(PWD)/internal/types --output=$(PWD)/internal/mocks --filename=mock_indexnode.go --with-expecter --structname=MockIndexNode

	# Clients
	$(INSTALL_PATH)/mockery --name=RootCoordClient --dir=$(PWD)/internal/types --output=$(PWD)/internal/mocks --filename=mock_rootcoord_client.go --with-expecter --structname=MockRootCoordClient
	$(INSTALL_PATH)/mockery --name=QueryCoordClient --dir=$(PWD)/internal/types --output=$(PWD)/internal/mocks --filename=mock_querycoord_client.go --with-expecter --structname=MockQueryCoordClient
	$(INSTALL_PATH)/mockery --name=DataCoordClient --dir=$(PWD)/internal/types --output=$(PWD)/internal/mocks --filename=mock_datacoord_client.go --with-expecter --structname=MockDataCoordClient
	$(INSTALL_PATH)/mockery --name=QueryNodeClient --dir=$(PWD)/internal/types --output=$(PWD)/internal/mocks --filename=mock_querynode_client.go --with-expecter --structname=MockQueryNodeClient
	$(INSTALL_PATH)/mockery --name=DataNodeClient --dir=$(PWD)/internal/types --output=$(PWD)/internal/mocks --filename=mock_datanode_client.go --with-expecter --structname=MockDataNodeClient
	$(INSTALL_PATH)/mockery --name=IndexNodeClient --dir=$(PWD)/internal/types --output=$(PWD)/internal/mocks --filename=mock_indexnode_client.go --with-expecter --structname=MockIndexNodeClient
	$(INSTALL_PATH)/mockery --name=ProxyClient --dir=$(PWD)/internal/types --output=$(PWD)/internal/mocks --filename=mock_proxy_client.go --with-expecter --structname=MockProxyClient

generate-mockery-rootcoord: getdeps
	$(INSTALL_PATH)/mockery --name=IMetaTable --dir=$(PWD)/internal/rootcoord --output=$(PWD)/internal/rootcoord/mocks --filename=meta_table.go --with-expecter --outpkg=mockrootcoord
	$(INSTALL_PATH)/mockery --name=GarbageCollector --dir=$(PWD)/internal/rootcoord --output=$(PWD)/internal/rootcoord/mocks --filename=garbage_collector.go --with-expecter --outpkg=mockrootcoord

generate-mockery-proxy: getdeps
	$(INSTALL_PATH)/mockery --name=Cache --dir=$(PWD)/internal/proxy --output=$(PWD)/internal/proxy --filename=mock_cache.go --structname=MockCache --with-expecter --outpkg=proxy --inpackage
	$(INSTALL_PATH)/mockery --name=timestampAllocatorInterface --dir=$(PWD)/internal/proxy --output=$(PWD)/internal/proxy --filename=mock_tso_test.go --structname=mockTimestampAllocator --with-expecter --outpkg=proxy --inpackage
	$(INSTALL_PATH)/mockery --name=LBPolicy --dir=$(PWD)/internal/proxy --output=$(PWD)/internal/proxy --filename=mock_lb_policy.go --structname=MockLBPolicy --with-expecter --outpkg=proxy --inpackage
	$(INSTALL_PATH)/mockery --name=LBBalancer --dir=$(PWD)/internal/proxy --output=$(PWD)/internal/proxy --filename=mock_lb_balancer.go --structname=MockLBBalancer --with-expecter --outpkg=proxy  --inpackage
	$(INSTALL_PATH)/mockery --name=shardClientMgr --dir=$(PWD)/internal/proxy --output=$(PWD)/internal/proxy --filename=mock_shardclient_manager.go --structname=MockShardClientManager --with-expecter --outpkg=proxy --inpackage
	$(INSTALL_PATH)/mockery --name=channelsMgr --dir=$(PWD)/internal/proxy --output=$(PWD)/internal/proxy --filename=mock_channels_manager.go --structname=MockChannelsMgr --with-expecter --outpkg=proxy --inpackage

generate-mockery-querycoord: getdeps
	$(INSTALL_PATH)/mockery --name=QueryNodeServer --dir=$(PWD)/internal/proto/querypb/ --output=$(PWD)/internal/querycoordv2/mocks --filename=mock_querynode.go --with-expecter --structname=MockQueryNodeServer
	$(INSTALL_PATH)/mockery --name=Broker --dir=$(PWD)/internal/querycoordv2/meta --output=$(PWD)/internal/querycoordv2/meta --filename=mock_broker.go --with-expecter --structname=MockBroker --outpkg=meta
	$(INSTALL_PATH)/mockery --name=Scheduler --dir=$(PWD)/internal/querycoordv2/task --output=$(PWD)/internal/querycoordv2/task --filename=mock_scheduler.go --with-expecter --structname=MockScheduler --outpkg=task --inpackage
	$(INSTALL_PATH)/mockery --name=Cluster --dir=$(PWD)/internal/querycoordv2/session --output=$(PWD)/internal/querycoordv2/session --filename=mock_cluster.go --with-expecter --structname=MockCluster --outpkg=session --inpackage
	$(INSTALL_PATH)/mockery --name=Balance --dir=$(PWD)/internal/querycoordv2/balance --output=$(PWD)/internal/querycoordv2/balance --filename=mock_balancer.go --with-expecter --structname=MockBalancer --outpkg=balance --inpackage
	$(INSTALL_PATH)/mockery --name=Controller --dir=$(PWD)/internal/querycoordv2/dist --output=$(PWD)/internal/querycoordv2/dist --filename=mock_controller.go --with-expecter --structname=MockController --outpkg=dist --inpackage

generate-mockery-querynode: getdeps build-cpp
	@source $(PWD)/scripts/setenv.sh # setup PKG_CONFIG_PATH
	$(INSTALL_PATH)/mockery --name=QueryHook --dir=$(PWD)/internal/querynodev2/optimizers --output=$(PWD)/internal/querynodev2/optimizers --filename=mock_query_hook.go --with-expecter --outpkg=optimizers --structname=MockQueryHook --inpackage
	$(INSTALL_PATH)/mockery --name=Manager --dir=$(PWD)/internal/querynodev2/cluster --output=$(PWD)/internal/querynodev2/cluster --filename=mock_manager.go --with-expecter --outpkg=cluster --structname=MockManager --inpackage
	$(INSTALL_PATH)/mockery --name=SegmentManager --dir=$(PWD)/internal/querynodev2/segments --output=$(PWD)/internal/querynodev2/segments --filename=mock_segment_manager.go --with-expecter --outpkg=segments --structname=MockSegmentManager --inpackage
	$(INSTALL_PATH)/mockery --name=CollectionManager --dir=$(PWD)/internal/querynodev2/segments --output=$(PWD)/internal/querynodev2/segments --filename=mock_collection_manager.go --with-expecter --outpkg=segments --structname=MockCollectionManager --inpackage
	$(INSTALL_PATH)/mockery --name=Loader --dir=$(PWD)/internal/querynodev2/segments --output=$(PWD)/internal/querynodev2/segments --filename=mock_loader.go --with-expecter --outpkg=segments --structname=MockLoader --inpackage
	$(INSTALL_PATH)/mockery --name=Segment --dir=$(PWD)/internal/querynodev2/segments --output=$(PWD)/internal/querynodev2/segments --filename=mock_segment.go --with-expecter --outpkg=segments --structname=MockSegment  --inpackage
	$(INSTALL_PATH)/mockery --name=Worker --dir=$(PWD)/internal/querynodev2/cluster --output=$(PWD)/internal/querynodev2/cluster --filename=mock_worker.go --with-expecter --outpkg=worker --structname=MockWorker --inpackage
	$(INSTALL_PATH)/mockery --name=ShardDelegator --dir=$(PWD)/internal/querynodev2/delegator/ --output=$(PWD)/internal/querynodev2/delegator/ --filename=mock_delegator.go --with-expecter --outpkg=delegator --structname=MockShardDelegator --inpackage

generate-mockery-datacoord: getdeps
	$(INSTALL_PATH)/mockery --name=compactionPlanContext --dir=internal/datacoord --filename=mock_compaction_plan_context.go --output=internal/datacoord  --structname=MockCompactionPlanContext --with-expecter --inpackage
	$(INSTALL_PATH)/mockery --name=Handler --dir=internal/datacoord --filename=mock_handler.go --output=internal/datacoord  --structname=NMockHandler --with-expecter --inpackage
	$(INSTALL_PATH)/mockery --name=allocator --dir=internal/datacoord --filename=mock_allocator_test.go --output=internal/datacoord  --structname=NMockAllocator --with-expecter --inpackage
	$(INSTALL_PATH)/mockery --name=RWChannelStore --dir=internal/datacoord --filename=mock_channel_store.go --output=internal/datacoord  --structname=MockRWChannelStore --with-expecter --inpackage

generate-mockery-datanode: getdeps
	$(INSTALL_PATH)/mockery --name=Allocator --dir=$(PWD)/internal/datanode/allocator --output=$(PWD)/internal/datanode/allocator --filename=mock_allocator.go --with-expecter --structname=MockAllocator --outpkg=allocator --inpackage
	$(INSTALL_PATH)/mockery --name=Broker --dir=$(PWD)/internal/datanode/broker --output=$(PWD)/internal/datanode/broker/ --filename=mock_broker.go --with-expecter --structname=MockBroker --outpkg=broker --inpackage

generate-mockery-metastore: getdeps
	$(INSTALL_PATH)/mockery --name=RootCoordCatalog --dir=$(PWD)/internal/metastore --output=$(PWD)/internal/metastore/mocks --filename=mock_rootcoord_catalog.go --with-expecter --structname=RootCoordCatalog --outpkg=mocks
	$(INSTALL_PATH)/mockery --name=DataCoordCatalog --dir=$(PWD)/internal/metastore --output=$(PWD)/internal/metastore/mocks --filename=mock_datacoord_catalog.go --with-expecter --structname=DataCoordCatalog --outpkg=mocks
	$(INSTALL_PATH)/mockery --name=QueryCoordCatalog --dir=$(PWD)/internal/metastore --output=$(PWD)/internal/metastore/mocks --filename=mock_querycoord_catalog.go --with-expecter --structname=QueryCoordCatalog --outpkg=mocks

generate-mockery-utils: getdeps
	# dependency.Factory
	$(INSTALL_PATH)/mockery --name=Factory --dir=internal/util/dependency --output=internal/util/dependency --filename=mock_factory.go --with-expecter --structname=MockFactory --inpackage
	# tso.Allocator
	$(INSTALL_PATH)/mockery --name=Allocator --dir=internal/tso --output=internal/tso/mocks --filename=allocator.go --with-expecter --structname=Allocator --outpkg=mocktso
	$(INSTALL_PATH)/mockery --name=SessionInterface --dir=$(PWD)/internal/util/sessionutil --output=$(PWD)/internal/util/sessionutil --filename=mock_session.go --with-expecter --structname=MockSession --inpackage

generate-mockery-kv: getdeps
	$(INSTALL_PATH)/mockery --name=TxnKV --dir=$(PWD)/internal/kv --output=$(PWD)/internal/kv/mocks --filename=txn_kv.go --with-expecter
	$(INSTALL_PATH)/mockery --name=MetaKv --dir=$(PWD)/internal/kv --output=$(PWD)/internal/kv/mocks --filename=meta_kv.go --with-expecter
	$(INSTALL_PATH)/mockery --name=WatchKV --dir=$(PWD)/internal/kv --output=$(PWD)/internal/kv/mocks --filename=watch_kv.go --with-expecter
	$(INSTALL_PATH)/mockery --name=SnapShotKV --dir=$(PWD)/internal/kv --output=$(PWD)/internal/kv/mocks --filename=snapshot_kv.go --with-expecter
	$(INSTALL_PATH)/mockery --name=Predicate --dir=$(PWD)/internal/kv/predicates --output=$(PWD)/internal/kv/predicates --filename=mock_predicate.go --with-expecter --inpackage

generate-mockery-pkg:
	$(MAKE) -C pkg generate-mockery

generate-mockery: generate-mockery-types generate-mockery-kv generate-mockery-rootcoord generate-mockery-proxy generate-mockery-querycoord generate-mockery-querynode generate-mockery-datacoord generate-mockery-pkg

