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
//
//#include <iostream>
//#include <sstream>
//#include "knowhere/adapter/sptag.h"
//#include "knowhere/adapter/structure.h"
//#include "knowhere/index/vector_index/cpu_kdt_rng.h"
//#include "knowhere/index/vector_index/definitions.h"
//
//
// knowhere::DatasetPtr
// generate_dataset(int64_t n, int64_t d, int64_t base) {
//    auto elems = n * d;
//    auto p_data = (float*)malloc(elems * sizeof(float));
//    auto p_id = (int64_t*)malloc(elems * sizeof(int64_t));
//    assert(p_data != nullptr && p_id != nullptr);
//
//    for (auto i = 0; i < n; ++i) {
//        for (auto j = 0; j < d; ++j) {
//            p_data[i * d + j] = float(base + i);
//        }
//        p_id[i] = i;
//    }
//
//    std::vector<int64_t> shape{n, d};
//    auto tensor = ConstructFloatTensorSmart((uint8_t*)p_data, elems * sizeof(float), shape);
//    std::vector<TensorPtr> tensors{tensor};
//    std::vector<FieldPtr> tensor_fields{ConstructFloatField("data")};
//    auto tensor_schema = std::make_shared<Schema>(tensor_fields);
//
//    auto id_array = ConstructInt64ArraySmart((uint8_t*)p_id, n * sizeof(int64_t));
//    std::vector<ArrayPtr> arrays{id_array};
//    std::vector<FieldPtr> array_fields{ConstructInt64Field("id")};
//    auto array_schema = std::make_shared<Schema>(tensor_fields);
//
//    auto dataset = std::make_shared<Dataset>(std::move(arrays), array_schema, std::move(tensors), tensor_schema);
//
//    return dataset;
//}
//
// knowhere::DatasetPtr
// generate_queries(int64_t n, int64_t d, int64_t k, int64_t base) {
//    size_t size = sizeof(float) * n * d;
//    auto v = (float*)malloc(size);
//    // TODO(lxj): check malloc
//    for (auto i = 0; i < n; ++i) {
//        for (auto j = 0; j < d; ++j) {
//            v[i * d + j] = float(base + i);
//        }
//    }
//
//    std::vector<TensorPtr> data;
//    auto buffer = MakeMutableBufferSmart((uint8_t*)v, size);
//    std::vector<int64_t> shape{n, d};
//    auto float_type = std::make_shared<arrow::FloatType>();
//    auto tensor = std::make_shared<Tensor>(float_type, buffer, shape);
//    data.push_back(tensor);
//
//    Config meta;
//    meta[META_ROWS] = int64_t(n);
//    meta[META_DIM] = int64_t(d);
//    meta[META_K] = int64_t(k);
//
//    auto type = std::make_shared<arrow::FloatType>();
//    auto field = std::make_shared<Field>("data", type);
//    std::vector<FieldPtr> fields{field};
//    auto schema = std::make_shared<Schema>(fields);
//
//    return std::make_shared<knowhere::Dataset>(data, schema);
//}
//
// int
// main(int argc, char* argv[]) {
//    auto kdt_index = std::make_shared<CPUKDTRNG>();
//
//    const auto d = 10;
//    const auto k = 3;
//    const auto nquery = 10;
//
//    // ID [0, 99]
//    auto train = generate_dataset(100, d, 0);
//    // ID [100]
//    auto base = generate_dataset(1, d, 0);
//    auto queries = generate_queries(nquery, d, k, 0);
//
//    // Build Preprocessor
//    auto preprocessor = kdt_index->BuildPreprocessor(train, Config());
//
//    // Set Preprocessor
//    kdt_index->set_preprocessor(preprocessor);
//
//    Config train_config;
//    train_config["TPTNumber"] = "64";
//    // Train
//    kdt_index->Train(train, train_config);
//
//    // Add
//    kdt_index->Add(base, Config());
//
//    auto binary = kdt_index->Serialize();
//    auto new_index = std::make_shared<CPUKDTRNG>();
//    new_index->Load(binary);
//    //    auto new_index = kdt_index;
//
//    Config search_config;
//    search_config[META_K] = int64_t(k);
//
//    // Search
//    auto result = new_index->Search(queries, search_config);
//
//    // Print Result
//    {
//        auto ids = result->array()[0];
//        auto dists = result->array()[1];
//
//        std::stringstream ss_id;
//        std::stringstream ss_dist;
//        for (auto i = 0; i < nquery; i++) {
//            for (auto j = 0; j < k; ++j) {
//                ss_id << *ids->data()->GetValues<int64_t>(1, i * k + j) << " ";
//                ss_dist << *dists->data()->GetValues<float>(1, i * k + j) << " ";
//            }
//            ss_id << std::endl;
//            ss_dist << std::endl;
//        }
//        std::cout << "id\n" << ss_id.str() << std::endl;
//        std::cout << "dist\n" << ss_dist.str() << std::endl;
//    }
//}
