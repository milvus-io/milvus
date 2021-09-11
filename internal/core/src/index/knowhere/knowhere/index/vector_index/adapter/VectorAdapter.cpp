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

#include <memory>

#include "knowhere/common/Dataset.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"

namespace milvus {
namespace knowhere {

DatasetPtr
GenDataset(const int64_t nb, const int64_t dim, const void* xb) {
    auto ret_ds = std::make_shared<Dataset>();
    ret_ds->Set(meta::ROWS, nb);
    ret_ds->Set(meta::DIM, dim);
    ret_ds->Set(meta::TENSOR, xb);
    return ret_ds;
}

}  // namespace knowhere
}  // namespace milvus
