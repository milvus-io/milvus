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


#include "wrapper/DataTransfer.h"

#include <vector>
#include <memory>
#include <utility>

namespace zilliz {
namespace milvus {
namespace engine {

knowhere::DatasetPtr
GenDatasetWithIds(const int64_t &nb, const int64_t &dim, const float *xb, const int64_t *ids) {
    std::vector<int64_t> shape{nb, dim};
    auto tensor = knowhere::ConstructFloatTensor((uint8_t *) xb, nb * dim * sizeof(float), shape);
    std::vector<knowhere::TensorPtr> tensors{tensor};
    std::vector<knowhere::FieldPtr> tensor_fields{knowhere::ConstructFloatField("data")};
    auto tensor_schema = std::make_shared<knowhere::Schema>(tensor_fields);

    auto id_array = knowhere::ConstructInt64Array((uint8_t *) ids, nb * sizeof(int64_t));
    std::vector<knowhere::ArrayPtr> arrays{id_array};
    std::vector<knowhere::FieldPtr> array_fields{knowhere::ConstructInt64Field("id")};
    auto array_schema = std::make_shared<knowhere::Schema>(tensor_fields);

    auto dataset = std::make_shared<knowhere::Dataset>(std::move(arrays), array_schema,
                                             std::move(tensors), tensor_schema);
    return dataset;
}

knowhere::DatasetPtr
GenDataset(const int64_t &nb, const int64_t &dim, const float *xb) {
    std::vector<int64_t> shape{nb, dim};
    auto tensor = knowhere::ConstructFloatTensor((uint8_t *) xb, nb * dim * sizeof(float), shape);
    std::vector<knowhere::TensorPtr> tensors{tensor};
    std::vector<knowhere::FieldPtr> tensor_fields{knowhere::ConstructFloatField("data")};
    auto tensor_schema = std::make_shared<knowhere::Schema>(tensor_fields);

    auto dataset = std::make_shared<knowhere::Dataset>(std::move(tensors), tensor_schema);
    return dataset;
}

} // namespace engine
} // namespace milvus
} // namespace zilliz
