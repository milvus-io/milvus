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

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "common/arrow_c_data_c.h"

namespace milvus::pyudf {

class PyUDFResult;

class PyUDFInvocation {
 public:
    PyUDFInvocation(int32_t num_inputs,
                    int32_t num_chunks,
                    const int64_t* chunk_sizes);
    ~PyUDFInvocation();

    PyUDFInvocation(const PyUDFInvocation&) = delete;
    PyUDFInvocation&
    operator=(const PyUDFInvocation&) = delete;
    PyUDFInvocation(PyUDFInvocation&&) = delete;
    PyUDFInvocation&
    operator=(PyUDFInvocation&&) = delete;

    int32_t
    num_inputs() const;

    int32_t
    num_chunks() const;

    int64_t
    chunk_size(int32_t chunk_index) const;

    ArrowArray*
    input_array(int32_t input_index, int32_t chunk_index);

    ArrowSchema*
    input_schema(int32_t input_index, int32_t chunk_index);

    void
    ValidatePopulated() const;

    std::unique_ptr<PyUDFResult>
    RunIdentity();

 private:
    size_t
    slot_index(int32_t input_index, int32_t chunk_index) const;

    int32_t num_inputs_;
    int32_t num_chunks_;
    std::vector<int64_t> chunk_sizes_;
    std::vector<ArrowArray> input_arrays_;
    std::vector<ArrowSchema> input_schemas_;
};

class PyUDFResult {
 public:
    PyUDFResult(int32_t num_outputs, const int32_t* num_chunks);
    ~PyUDFResult();

    PyUDFResult(const PyUDFResult&) = delete;
    PyUDFResult&
    operator=(const PyUDFResult&) = delete;
    PyUDFResult(PyUDFResult&&) = delete;
    PyUDFResult&
    operator=(PyUDFResult&&) = delete;

    int32_t
    num_outputs() const;

    int32_t
    num_chunks(int32_t output_index) const;

    ArrowArray*
    output_array(int32_t output_index, int32_t chunk_index);

    ArrowSchema*
    output_schema(int32_t output_index, int32_t chunk_index);

 private:
    size_t
    slot_index(int32_t output_index, int32_t chunk_index) const;

    std::vector<int32_t> num_chunks_;
    std::vector<size_t> output_offsets_;
    std::vector<ArrowArray> output_arrays_;
    std::vector<ArrowSchema> output_schemas_;
};

}  // namespace milvus::pyudf
