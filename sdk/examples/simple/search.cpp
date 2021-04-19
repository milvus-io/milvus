

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
#include <thirdparty/nlohmann/json.hpp>
#include "interface/ConnectionImpl.h"
#include "include/MilvusApi.h"
#include "grpc/ClientProxy.h"
#include "interface/ConnectionImpl.h"
#include "utils/TimeRecorder.h"
#include "utils/Utils.h"
#include <random>

const int TOP_K = 10;

int main(int argc , char**argv) {
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
  std::vector<std::string> partition_list;
  partition_list.emplace_back("partition-1");
  partition_list.emplace_back("partition-2");
  partition_list.emplace_back("partition-3");

  milvus::VectorParam vectorParam;
  std::vector<milvus::VectorData> vector_records;

  std::default_random_engine eng(rand() % 20);
  std::normal_distribution<float> dis(0, 1);
 
  for (int j = 0; j < 10; ++j) {
    milvus::VectorData vectorData;
    std::vector<float> float_data;
    for (int i = 0; i < 100; ++i) {
      float_data.emplace_back(dis(eng));
    }

    vectorData.float_data = float_data;
    vector_records.emplace_back(vectorData);
  }

  nlohmann::json vector_param_json;
  vector_param_json["num_queries"] = 1;
  vector_param_json["topK"] = TOP_K;
  vector_param_json["field_name"] = "field_vec";
  std::string vector_param_json_string = vector_param_json.dump();

  vectorParam.json_param = vector_param_json_string;
  vectorParam.vector_records = vector_records;

  milvus::TopKQueryResult result;

  milvus_sdk::TimeRecorder test_search("search");
  auto status = client.Search("collection0", partition_list, "dsl", vectorParam, result);

    return 0;
}

