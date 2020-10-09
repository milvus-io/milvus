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

#include <getopt.h>
#include <libgen.h>
#include <cstring>
#include <string>
#include <iostream>

#include "utils/Utils.h"
#include "grpc/ClientProxy.h"
#include "interface/ConnectionImpl.h"
#include "utils/TimeRecorder.h"
#include <random>

const int N = 200000;
const int DIM = 16;
const int LOOP = 10;

const milvus::FieldValue GetData() {
  milvus::FieldValue value_map;

  std::vector<int32_t> int32_data;
  for (int i = 0; i < N; i++) {
    int32_data.push_back(i);
  }
  std::default_random_engine eng(rand() % 20);
  std::normal_distribution<float> dis(0, 1);
  std::vector<milvus::VectorData> vector_data;
  for (int i = 0; i < N; i++) {
    std::vector<float> float_data(DIM);
    for(auto &x: float_data) {
      x = dis(eng);
    }
    milvus::VectorData vectorData;
    vectorData.float_data = float_data;
    vector_data.push_back(vectorData);
  }

  value_map.int32_value["INT32"] = int32_data;
  value_map.vector_value["VECTOR"] = vector_data;
  value_map.row_num = N;
  return value_map;
}

int
main(int argc, char* argv[]) {
  TestParameters parameters = milvus_sdk::Utils::ParseTestParameters(argc, argv);
    if (!parameters.is_valid){
      return 0;
   }

    auto client = milvus::ConnectionImpl();
    milvus::ConnectParam connect_param;
    connect_param.ip_address = parameters.address_.empty() ? "127.0.0.1":parameters.address_;
    connect_param.port = parameters.port_.empty() ? "19530":parameters.port_ ;
    client.Connect(connect_param);
    std::vector<int64_t> ids_array;
    auto data = GetData();
    for (int64_t i = 0; i < N; i++) {
        ids_array.push_back(i);
    }

    milvus_sdk::TimeRecorder insert("insert");
    for (int j = 0; j < LOOP; ++j) {
        auto status = client.Insert("collection0", "tag01", data, ids_array);
        if (!status.ok()){
            return -1;
        }
    }

    return 0;
}
