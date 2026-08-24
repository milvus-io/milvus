// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ProjectNode.h"

#include <algorithm>
#include <optional>
#include <utility>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/FastMem.h"
#include "common/FieldData.h"
#include "exec/QueryContext.h"
#include "exec/expression/Utils.h"
#include "exec/operator/Operator.h"
#include "plan/PlanNode.h"
#include "segcore/InsertRecord.h"
#include "segcore/SegmentInterface.h"
#include "segcore/Utils.h"

namespace milvus {
namespace exec {
namespace {

struct SelectedOffsets {
    std::vector<int64_t> row_offsets;
    std::vector<int32_t> element_indices;
};

SelectedOffsets
SelectOffsets(const TargetBitmapView& raw_data_view,
              QueryContext* query_context,
              const segcore::SegmentInternalInterface* segment) {
    if (!query_context->bitset_is_element_level()) {
        auto result_pair =
            segment->find_first_n(segcore::Unlimited, raw_data_view);
        return {std::move(result_pair.first), {}};
    }

    auto array_offsets = query_context->get_array_offsets();
    AssertInfo(array_offsets != nullptr,
               "element-level ProjectNode requires array offsets");

    auto [doc_offsets, element_indices_by_doc, _] =
        segment->find_first_n_element(segcore::Unlimited,
                                      raw_data_view,
                                      array_offsets.get(),
                                      std::nullopt);

    size_t selected_count = 0;
    for (const auto& element_indices : element_indices_by_doc) {
        selected_count += element_indices.size();
    }

    SelectedOffsets selected;
    selected.row_offsets.reserve(selected_count);
    selected.element_indices.reserve(selected_count);

    for (size_t i = 0; i < doc_offsets.size(); i++) {
        for (auto element_index : element_indices_by_doc[i]) {
            selected.row_offsets.emplace_back(doc_offsets[i]);
            selected.element_indices.emplace_back(element_index);
        }
    }
    return selected;
}

ColumnVectorPtr
MakeInt64Column(const std::vector<int64_t>& values) {
    auto selected_count = values.size();
    FixedVector<int64_t> output(selected_count);
    milvus::fastmem::FastMemcpy(
        output.data(), values.data(), selected_count * sizeof(int64_t));
    auto field_data = std::make_shared<FieldData<int64_t>>(
        DataType::INT64, false, std::move(output));
    auto valid_map = TargetBitmap(selected_count, true);
    return std::make_shared<ColumnVector>(
        std::move(field_data), std::move(valid_map), 0);
}

ColumnVectorPtr
MakeElementIndexColumn(const std::vector<int32_t>& element_indices) {
    auto selected_count = element_indices.size();
    FixedVector<int64_t> output(selected_count);
    for (size_t i = 0; i < selected_count; i++) {
        output[i] = element_indices[i];
    }
    auto field_data = std::make_shared<FieldData<int64_t>>(
        DataType::INT64, false, std::move(output));
    auto valid_map = TargetBitmap(selected_count, true);
    return std::make_shared<ColumnVector>(
        std::move(field_data), std::move(valid_map), 0);
}

}  // namespace

PhyProjectNode::PhyProjectNode(
    int32_t operator_id,
    milvus::exec::DriverContext* ctx,
    const std::shared_ptr<const plan::ProjectNode>& projectNode)
    : Operator(ctx,
               projectNode->output_type(),
               operator_id,
               projectNode->id(),
               "Project"),
      fields_to_project_(projectNode->FieldsToProject()),
      query_context_(nullptr),
      op_context_(nullptr) {
    auto exec_context = operator_context_->get_exec_context();
    query_context_ = exec_context->get_query_context();
    segment_ = query_context_->get_segment();
    op_context_ = query_context_->get_op_context();
    AssertInfo(op_context_, "op_context_ cannot be nullptr for ProjectNode");
    AssertInfo(segment_, "segment_ cannot be nullptr for ProjectNode");
}

void
PhyProjectNode::AddInput(milvus::RowVectorPtr& input) {
    input_ = std::move(input);
}

RowVectorPtr
PhyProjectNode::GetOutput() {
    if (is_finished_ || input_ == nullptr) {
        return nullptr;
    }
    auto col_input = GetColumnVector(input_);
    // raw data view
    TargetBitmapView raw_data_view(col_input->GetRawData(), col_input->size());

    // When no fields need to be projected (e.g., count(*) only), count valid
    // logical rows directly from the bitmap.  For element-level bitmaps this is
    // the matching element count. find_first deduplicates by PK in growing
    // segments (OffsetOrderedMap), which would undercount rows with duplicate
    // PKs.
    if (fields_to_project_.empty()) {
        auto valid_count =
            static_cast<int64_t>(col_input->size()) - raw_data_view.count();
        is_finished_ = true;
        if (valid_count == 0) {
            return nullptr;
        }
        auto row_vector = std::make_shared<RowVector>(std::vector<VectorPtr>{});
        row_vector->resize(valid_count);
        return row_vector;
    }

    auto selected = SelectOffsets(raw_data_view, query_context_, segment_);
    auto& selected_offsets = selected.row_offsets;
    auto& selected_element_indices = selected.element_indices;
    auto selected_count = selected_offsets.size();
    // When all rows are filtered out, return nullptr.
    // Driver requires GetOutput to return nullptr or a non-empty vector.
    if (selected_count == 0) {
        is_finished_ = true;
        return nullptr;
    }
    auto row_type = OutputType();
    std::vector<VectorPtr> column_vectors;
    column_vectors.reserve(fields_to_project_.size());
    for (int i = 0; i < fields_to_project_.size(); i++) {
        auto column_type = row_type->column_type(i);
        auto field_id = fields_to_project_.at(i);

        if (field_id == SegmentOffsetFieldID) {
            column_vectors.emplace_back(MakeInt64Column(selected_offsets));
            continue;
        }

        if (field_id == ElementIndexFieldID) {
            AssertInfo(selected_element_indices.size() == selected_count,
                       "element indices size ({}) must match selected row "
                       "count ({})",
                       selected_element_indices.size(),
                       selected_count);
            column_vectors.emplace_back(
                MakeElementIndexColumn(selected_element_indices));
            continue;
        }

        if (!segment_->is_field_exist(field_id)) {
            auto field_data =
                InitScalarFieldDataWithLength(column_type, selected_count);
            auto valid_map = TargetBitmap(selected_count, false);
            auto col = std::make_shared<ColumnVector>(
                std::move(field_data), std::move(valid_map), selected_count);
            column_vectors.emplace_back(std::move(col));
            continue;
        }

        TargetBitmap valid_map(selected_count);
        auto field_data = bulk_script_field_data(op_context_,
                                                 field_id,
                                                 column_type,
                                                 selected_offsets.data(),
                                                 selected_count,
                                                 segment_,
                                                 valid_map,
                                                 true);
        auto null_count = selected_count - valid_map.count();
        auto column_vector = std::make_shared<ColumnVector>(
            std::move(field_data), std::move(valid_map), null_count);
        column_vectors.emplace_back(std::move(column_vector));
    }
    is_finished_ = true;
    auto row_vector = std::make_shared<RowVector>(std::move(column_vectors));
    return row_vector;
}

};  // namespace exec
};  // namespace milvus
