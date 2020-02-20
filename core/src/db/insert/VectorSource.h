// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include "db/IDGenerator.h"
#include "db/engine/ExecutionEngine.h"
#include "db/meta/Meta.h"
#include "utils/Status.h"

#include <memory>

namespace milvus {
namespace engine {

class VectorSource {
 public:
    explicit VectorSource(VectorsData& vectors);

    Status
    Add(const ExecutionEnginePtr& execution_engine, const meta::TableFileSchema& table_file_schema,
        const size_t& num_vectors_to_add, size_t& num_vectors_added);

    size_t
    GetNumVectorsAdded();

    size_t
    SingleVectorSize(uint16_t dimension);

    bool
    AllAdded();

    IDNumbers
    GetVectorIds();

 private:
    VectorsData& vectors_;
    IDNumbers vector_ids_;

    size_t current_num_vectors_added;

    std::shared_ptr<IDGenerator> id_generator_;
};  // VectorSource

using VectorSourcePtr = std::shared_ptr<VectorSource>;

}  // namespace engine
}  // namespace milvus
