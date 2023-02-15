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

#include "segcore/engine/SegmentConnector.h"
#include <fstream>
#include <sstream>
#include "common/Consts.h"

#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentSealedImpl.h"

namespace milvus::engine {

void
ReadProtoFromJsonFile(const std::string& msgPath,
                      google::protobuf::Message& msg) {
    // Read json file and resume the Substrait plan.
    std::ifstream msgJson(msgPath);
    if (msgJson.fail()) {
        throw std::runtime_error(std::string("Failed to open file:") + msgPath +
                                 " errno:" + strerror(errno));
    }
    std::stringstream buffer;
    buffer << msgJson.rdbuf();
    std::string msgData = buffer.str();
    auto status = google::protobuf::util::JsonStringToMessage(msgData, &msg);
    if (!status.ok()) {
        throw std::runtime_error(
            std::string("Failed to parse Substrait JSON: ") +
            status.message().ToString());
    }
}

std::optional<facebook::velox::RowVectorPtr>
SegmentDataSource::next(uint64_t size,
                        facebook::velox::ContinueFuture& future) {
    VELOX_CHECK_NOT_NULL(current_split_,
                         "No split to process. Call addSplit() first.");
    auto* segment = current_split_->segment_;

    if (split_offset_ == segment->num_chunk()) {
        return nullptr;
    }

    std::vector<VectorPtr> output_columns;
    auto& schema = segment->get_schema();
    for (auto& column_name : output_type_->names()) {
        // name => field id
        auto field_id = schema.get_field_id(FieldName(column_name));
        auto* sealed_impl =
            dynamic_cast<const milvus::segcore::SegmentSealedImpl*>(segment);
        if (sealed_impl != nullptr) {
            std::cout << "field id:" << field_id.get() << std::endl;
            auto* vector_base =
                sealed_impl->get_insert_record().get_field_data_base(field_id);
            output_columns.push_back(
                vector_base->get_engine_vector(split_offset_));
        } else {
            auto* growing_impl =
                dynamic_cast<const milvus::segcore::SegmentGrowingImpl*>(
                    segment);
            VELOX_CHECK_NOT_NULL(growing_impl,
                                 "segment must belong to sealed or growing");
            std::cout << "field id:" << field_id.get() << std::endl;
            auto* vector_base =
                growing_impl->get_insert_record().get_field_data_base(field_id);
            output_columns.push_back(
                vector_base->get_engine_vector(split_offset_));
        }
    }
    split_offset_++;

    int64_t row_count = output_columns.at(0)->size();
    completed_rows_ += row_count;
    std::cout << "row count " << row_count << std::endl;
    return std::make_shared<facebook::velox::RowVector>(
        pool_, output_type_, nullptr, row_count, output_columns);
}

facebook::velox::exec::test::PlanBuilder&
PlanBuilder::tableScan(const milvus::segcore::SegmentInternalInterface* segment,
                       const std::vector<std::string>& subfieldFilters,
                       const std::string& remainingFilter) {
    std::vector<facebook::velox::TypePtr> output_types;
    std::vector<std::string> column_names;

    auto& schema = segment->get_schema();
    auto& fields = schema.get_fields();
    auto pk_field = schema.get_primary_field_id();
    for (auto& field : fields) {
        auto field_meta = field.second;
        auto field_name = field_meta.get_name().get();
        if (!field_meta.is_vector() &&
            !(pk_field.has_value() && field.first == pk_field.value()) &&
            std::find(subfieldFilters.begin(),
                      subfieldFilters.end(),
                      field_name) == subfieldFilters.end()) {
            column_names.push_back(field_name);
            output_types.push_back(ToVeloxType(field_meta.get_data_type()));
        }
    }
    auto output_type =
        facebook::velox::ROW(std::move(column_names), std::move(output_types));

    return facebook::velox::exec::test::PlanBuilder::tableScan(
        output_type,
        std::make_shared<SegmentTableHandle>(SEGMENT_CONNECTOR_ID, segment),
        {});
}

VELOX_REGISTER_CONNECTOR_FACTORY(std::make_shared<SegmentConnectorFactory>())

}  // namespace milvus::engine
