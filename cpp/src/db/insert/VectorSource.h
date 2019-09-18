// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#pragma once

#include "db/meta/Meta.h"
#include "db/IDGenerator.h"
#include "db/engine/ExecutionEngine.h"
#include "utils/Status.h"


namespace zilliz {
namespace milvus {
namespace engine {

class VectorSource {
 public:
    VectorSource(const size_t &n, const float *vectors);

    Status Add(const ExecutionEnginePtr &execution_engine,
               const meta::TableFileSchema &table_file_schema,
               const size_t &num_vectors_to_add,
               size_t &num_vectors_added,
               IDNumbers &vector_ids);

    size_t GetNumVectorsAdded();

    bool AllAdded();

    IDNumbers GetVectorIds();

 private:

    const size_t n_;
    const float *vectors_;
    IDNumbers vector_ids_;

    size_t current_num_vectors_added;

    std::shared_ptr<IDGenerator> id_generator_;

}; //VectorSource

using VectorSourcePtr = std::shared_ptr<VectorSource>;

} // namespace engine
} // namespace milvus
} // namespace zilliz