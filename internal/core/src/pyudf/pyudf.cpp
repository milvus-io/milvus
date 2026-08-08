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

#include "pyudf/pyudf.h"

#include <limits>
#include <stdexcept>

namespace milvus::pyudf {
namespace {

size_t
CheckedSlotCount(int32_t outer_count, int32_t inner_count) {
    if (outer_count < 0 || inner_count < 0) {
        throw std::invalid_argument("py_udf: dimensions cannot be negative");
    }
    auto outer = static_cast<size_t>(outer_count);
    auto inner = static_cast<size_t>(inner_count);
    if (inner != 0 && outer > std::numeric_limits<size_t>::max() / inner) {
        throw std::overflow_error("py_udf: descriptor slot count overflow");
    }
    return outer * inner;
}

void
ReleaseSlots(std::vector<ArrowArray>& arrays,
             std::vector<ArrowSchema>& schemas) {
    for (auto& array : arrays) {
        MilvusGoArrowArrayRelease(&array);
    }
    for (auto& schema : schemas) {
        MilvusGoArrowSchemaRelease(&schema);
    }
}

bool
IsPopulated(const ArrowArray& array, const ArrowSchema& schema) {
    return array.release != nullptr && schema.release != nullptr;
}

void
MoveDescriptor(ArrowArray* destination, ArrowArray* source) {
    if (destination == nullptr || source == nullptr ||
        destination->release != nullptr) {
        throw std::invalid_argument(
            "py_udf: ArrowArray move requires an empty destination");
    }
    *destination = *source;
    *source = {};
}

void
MoveDescriptor(ArrowSchema* destination, ArrowSchema* source) {
    if (destination == nullptr || source == nullptr ||
        destination->release != nullptr) {
        throw std::invalid_argument(
            "py_udf: ArrowSchema move requires an empty destination");
    }
    *destination = *source;
    *source = {};
}

}  // namespace

PyUDFInvocation::PyUDFInvocation(int32_t num_inputs,
                                 int32_t num_chunks,
                                 const int64_t* chunk_sizes)
    : num_inputs_(num_inputs), num_chunks_(num_chunks) {
    auto slot_count = CheckedSlotCount(num_inputs, num_chunks);
    if (num_chunks > 0 && chunk_sizes == nullptr) {
        throw std::invalid_argument("py_udf: chunk sizes are nil");
    }

    chunk_sizes_.reserve(static_cast<size_t>(num_chunks));
    for (int32_t i = 0; i < num_chunks; ++i) {
        if (chunk_sizes[i] < 0) {
            throw std::invalid_argument(
                "py_udf: chunk size cannot be negative");
        }
        chunk_sizes_.push_back(chunk_sizes[i]);
    }
    input_arrays_.resize(slot_count);
    input_schemas_.resize(slot_count);
}

PyUDFInvocation::~PyUDFInvocation() {
    ReleaseSlots(input_arrays_, input_schemas_);
}

int32_t
PyUDFInvocation::num_inputs() const {
    return num_inputs_;
}

int32_t
PyUDFInvocation::num_chunks() const {
    return num_chunks_;
}

int64_t
PyUDFInvocation::chunk_size(int32_t chunk_index) const {
    if (chunk_index < 0 || chunk_index >= num_chunks_) {
        return -1;
    }
    return chunk_sizes_[static_cast<size_t>(chunk_index)];
}

ArrowArray*
PyUDFInvocation::input_array(int32_t input_index, int32_t chunk_index) {
    if (input_index < 0 || input_index >= num_inputs_ || chunk_index < 0 ||
        chunk_index >= num_chunks_) {
        return nullptr;
    }
    return &input_arrays_[slot_index(input_index, chunk_index)];
}

ArrowSchema*
PyUDFInvocation::input_schema(int32_t input_index, int32_t chunk_index) {
    if (input_index < 0 || input_index >= num_inputs_ || chunk_index < 0 ||
        chunk_index >= num_chunks_) {
        return nullptr;
    }
    return &input_schemas_[slot_index(input_index, chunk_index)];
}

void
PyUDFInvocation::ValidatePopulated() const {
    for (size_t slot = 0; slot < input_arrays_.size(); ++slot) {
        if (!IsPopulated(input_arrays_[slot], input_schemas_[slot])) {
            throw std::invalid_argument(
                "py_udf: all invocation input slots must be populated");
        }
    }
}

std::unique_ptr<PyUDFResult>
PyUDFInvocation::RunIdentity() {
    ValidatePopulated();

    std::vector<int32_t> output_chunks(static_cast<size_t>(num_inputs_),
                                       num_chunks_);
    auto result =
        std::make_unique<PyUDFResult>(num_inputs_, output_chunks.data());
    for (int32_t input = 0; input < num_inputs_; ++input) {
        for (int32_t chunk = 0; chunk < num_chunks_; ++chunk) {
            MoveDescriptor(result->output_array(input, chunk),
                           input_array(input, chunk));
            MoveDescriptor(result->output_schema(input, chunk),
                           input_schema(input, chunk));
        }
    }
    return result;
}

size_t
PyUDFInvocation::slot_index(int32_t input_index, int32_t chunk_index) const {
    return static_cast<size_t>(input_index) * static_cast<size_t>(num_chunks_) +
           static_cast<size_t>(chunk_index);
}

PyUDFResult::PyUDFResult(int32_t num_outputs, const int32_t* num_chunks) {
    if (num_outputs < 0) {
        throw std::invalid_argument("py_udf: output count cannot be negative");
    }
    if (num_outputs > 0 && num_chunks == nullptr) {
        throw std::invalid_argument("py_udf: output chunk counts are nil");
    }

    auto output_count = static_cast<size_t>(num_outputs);
    num_chunks_.reserve(output_count);
    output_offsets_.reserve(output_count);
    size_t slot_count = 0;
    for (int32_t i = 0; i < num_outputs; ++i) {
        if (num_chunks[i] < 0) {
            throw std::invalid_argument(
                "py_udf: output chunk count cannot be negative");
        }
        auto chunk_count = static_cast<size_t>(num_chunks[i]);
        if (slot_count > std::numeric_limits<size_t>::max() - chunk_count) {
            throw std::overflow_error(
                "py_udf: result descriptor slot count overflow");
        }
        output_offsets_.push_back(slot_count);
        num_chunks_.push_back(num_chunks[i]);
        slot_count += chunk_count;
    }
    output_arrays_.resize(slot_count);
    output_schemas_.resize(slot_count);
}

PyUDFResult::~PyUDFResult() {
    ReleaseSlots(output_arrays_, output_schemas_);
}

int32_t
PyUDFResult::num_outputs() const {
    return static_cast<int32_t>(num_chunks_.size());
}

int32_t
PyUDFResult::num_chunks(int32_t output_index) const {
    if (output_index < 0 ||
        static_cast<size_t>(output_index) >= num_chunks_.size()) {
        return -1;
    }
    return num_chunks_[static_cast<size_t>(output_index)];
}

ArrowArray*
PyUDFResult::output_array(int32_t output_index, int32_t chunk_index) {
    auto chunk_count = num_chunks(output_index);
    if (chunk_index < 0 || chunk_count < 0 || chunk_index >= chunk_count) {
        return nullptr;
    }
    return &output_arrays_[slot_index(output_index, chunk_index)];
}

ArrowSchema*
PyUDFResult::output_schema(int32_t output_index, int32_t chunk_index) {
    auto chunk_count = num_chunks(output_index);
    if (chunk_index < 0 || chunk_count < 0 || chunk_index >= chunk_count) {
        return nullptr;
    }
    return &output_schemas_[slot_index(output_index, chunk_index)];
}

size_t
PyUDFResult::slot_index(int32_t output_index, int32_t chunk_index) const {
    return output_offsets_[static_cast<size_t>(output_index)] +
           static_cast<size_t>(chunk_index);
}

}  // namespace milvus::pyudf
