// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>

#include <google/protobuf/util/json_util.h>
#include <velox/connectors/Connector.h>
#include <velox/exec/tests/utils/PlanBuilder.h>
#include <velox/parse/TypeResolver.h>

#include "segcore/SegmentInterface.h"

namespace milvus::engine {

void
ReadProtoFromJsonFile(const std::string& msgPath,
                      google::protobuf::Message& msg);

class SegmentConnectorSplit
    : public facebook::velox::connector::ConnectorSplit {
 public:
    SegmentConnectorSplit(
        std::string connector_id,
        const milvus::segcore::SegmentInternalInterface* segment)
        : ConnectorSplit(connector_id), segment_(segment) {
    }

 public:
    const milvus::segcore::SegmentInternalInterface* segment_;
};

class SegmentColumnHandle : public facebook::velox::connector::ColumnHandle {
 public:
    explicit SegmentColumnHandle(const std::string& name) : name_(name) {
    }

    const std::string&
    name() const {
        return name_;
    }

 private:
    const std::string name_;
};

class SegmentTableHandle
    : public facebook::velox::connector::ConnectorTableHandle {
 public:
    explicit SegmentTableHandle(
        const std::string& connector_id,
        const milvus::segcore::SegmentInternalInterface* segment)
        : facebook::velox::connector::ConnectorTableHandle(connector_id),
          segment_(segment) {
    }

    ~SegmentTableHandle() override {
    }

    std::string
    toString() const override {
        std::stringstream ss;
        ss << "segment connector id:  " << connectorId();
        return ss.str();
    }

 private:
    const milvus::segcore::SegmentInternalInterface* segment_;
};

class SegmentDataSource : public facebook::velox::connector::DataSource {
 public:
    explicit SegmentDataSource(
        const std::shared_ptr<const facebook::velox::RowType>& output_type,
        const std::shared_ptr<facebook::velox::connector::ConnectorTableHandle>&
            table_handle,
        const std::unordered_map<
            std::string,
            std::shared_ptr<facebook::velox::connector::ColumnHandle>>&
            column_handles,
        facebook::velox::memory::MemoryPool* pool)
        : output_type_(output_type), pool_(pool) {
    }

    void
    addSplit(std::shared_ptr<facebook::velox::connector::ConnectorSplit> split)
        override {
        current_split_ =
            std::dynamic_pointer_cast<SegmentConnectorSplit>(split);
        VELOX_CHECK_NOT_NULL(current_split_->segment_,
                             "Split's segment can not be null");
        split_offset_ = 0;
    }

    void
    addDynamicFilter(
        facebook::velox::column_index_t /*outputChannel*/,
        const std::shared_ptr<facebook::velox::common::Filter>& /*filter*/)
        override {
        VELOX_NYI("Dynamic filters not supported by SegmentConnector.");
    }

    std::optional<facebook::velox::RowVectorPtr>
    next(uint64_t size, facebook::velox::ContinueFuture& future) override;

    uint64_t
    getCompletedRows() override {
        return completed_rows_;
    }

    uint64_t
    getCompletedBytes() override {
        return completed_bytes_;
    }

    std::unordered_map<std::string, facebook::velox::RuntimeCounter>
    runtimeStats() override {
        return {};
    }

 private:
    facebook::velox::RowTypePtr output_type_;

    std::shared_ptr<SegmentConnectorSplit> current_split_;
    size_t split_offset_;

    size_t completed_rows_{0};
    size_t completed_bytes_{0};

    facebook::velox::memory::MemoryPool* pool_;
};

class SegmentConnector final : public facebook::velox::connector::Connector {
 public:
    SegmentConnector(const std::string& id,
                     std::shared_ptr<const facebook::velox::Config> properties,
                     folly::Executor* /*executor*/)
        : facebook::velox::connector::Connector(id, properties) {
    }

    std::shared_ptr<facebook::velox::connector::DataSource>
    createDataSource(
        const std::shared_ptr<const facebook::velox::RowType>& output_type,
        const std::shared_ptr<facebook::velox::connector::ConnectorTableHandle>&
            table_handle,
        const std::unordered_map<
            std::string,
            std::shared_ptr<facebook::velox::connector::ColumnHandle>>&
            column_handles,
        facebook::velox::connector::ConnectorQueryCtx* connector_queryCtx)
        final {
        return std::make_shared<SegmentDataSource>(
            output_type,
            table_handle,
            column_handles,
            connector_queryCtx->memoryPool());
    }

    std::shared_ptr<facebook::velox::connector::DataSink>
    createDataSink(
        facebook::velox::RowTypePtr,
        std::shared_ptr<facebook::velox::connector::ConnectorInsertTableHandle>,
        facebook::velox::connector::ConnectorQueryCtx*,
        facebook::velox::connector::CommitStrategy) final {
        VELOX_NYI("SegmentConnector does not support data sink.");
    }
};

class SegmentConnectorFactory
    : public facebook::velox::connector::ConnectorFactory {
 public:
    static constexpr const char* FOLLY_NONNULL kSegmentConnectorName =
        "milvus_segment";

    SegmentConnectorFactory()
        : facebook::velox::connector::ConnectorFactory(kSegmentConnectorName) {
    }

    explicit SegmentConnectorFactory(const char* FOLLY_NONNULL connectorName)
        : facebook::velox::connector::ConnectorFactory(connectorName) {
    }

    std::shared_ptr<facebook::velox::connector::Connector>
    newConnector(const std::string& id,
                 std::shared_ptr<const facebook::velox::Config> properties,
                 folly::Executor* FOLLY_NULLABLE executor = nullptr) override {
        return std::make_shared<SegmentConnector>(id, properties, executor);
    }
};

class PlanBuilder : public facebook::velox::exec::test::PlanBuilder {
 public:
    facebook::velox::exec::test::PlanBuilder&
    tableScan(const milvus::segcore::SegmentInternalInterface* segment,
              const std::vector<std::string>& subfieldFilters = {},
              const std::string& remainingFilter = "");
};

}  // namespace milvus::engine
