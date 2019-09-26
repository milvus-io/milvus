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


#include "DataTransfer.h"


namespace zilliz {
namespace milvus {
namespace engine {

using namespace zilliz::knowhere;

DatasetPtr
GenDatasetWithIds(const int64_t &nb, const int64_t &dim, const float *xb, const long *ids) {
    std::vector<int64_t> shape{nb, dim};
    auto tensor = ConstructFloatTensor((uint8_t *) xb, nb * dim * sizeof(float), shape);
    std::vector<TensorPtr> tensors{tensor};
    std::vector<FieldPtr> tensor_fields{ConstructFloatField("data")};
    auto tensor_schema = std::make_shared<Schema>(tensor_fields);

    auto id_array = ConstructInt64Array((uint8_t *) ids, nb * sizeof(int64_t));
    std::vector<ArrayPtr> arrays{id_array};
    std::vector<FieldPtr> array_fields{ConstructInt64Field("id")};
    auto array_schema = std::make_shared<Schema>(tensor_fields);

    auto dataset = std::make_shared<Dataset>(std::move(arrays), array_schema,
                                             std::move(tensors), tensor_schema);
    return dataset;
}

DatasetPtr
GenDataset(const int64_t &nb, const int64_t &dim, const float *xb) {
    std::vector<int64_t> shape{nb, dim};
    auto tensor = ConstructFloatTensor((uint8_t *) xb, nb * dim * sizeof(float), shape);
    std::vector<TensorPtr> tensors{tensor};
    std::vector<FieldPtr> tensor_fields{ConstructFloatField("data")};
    auto tensor_schema = std::make_shared<Schema>(tensor_fields);

    auto dataset = std::make_shared<Dataset>(std::move(tensors), tensor_schema);
    return dataset;
}

}
}
}
