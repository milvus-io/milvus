

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
#include "interface/ConnectionImpl.h"
#include "include/MilvusApi.h"
#include "grpc/ClientProxy.h"
#include "interface/ConnectionImpl.h"

int main(int argc , char**argv) {
    auto client = milvus::ConnectionImpl();
    milvus::ConnectParam connect_param;
    connect_param.ip_address = "192.168.2.28";
    connect_param.port = "19530";
    client.Connect(connect_param);
    std::vector<int64_t> ids_array;
    std::vector<std::string> partition_list;
    partition_list.emplace_back("partition-1");
    partition_list.emplace_back("partition-2");
    partition_list.emplace_back("partition-3");

    milvus::VectorParam vectorParam;
    milvus::VectorData vectorData;
    std::vector<float> float_data;
    std::vector<uint8_t> binary_data;
    for (int i = 0; i < 100; ++i) {
        float_data.emplace_back(i);
        binary_data.emplace_back(i);
    }

    vectorData.float_data = float_data;
    vectorData.binary_data = binary_data;

    std::vector<milvus::VectorData> vector_records;
    for (int j = 0; j < 10; ++j) {
        vector_records.emplace_back(vectorData);
    }


    vectorParam.json_param = "json_param";
    vectorParam.vector_records = vector_records;

    milvus::TopKQueryResult result;



    auto t1 = std::chrono::high_resolution_clock::now();
//    for (int k = 0; k < 1000; ++k) {
    auto status = client.Search("collection1", partition_list, "dsl", vectorParam, result);

//    }

//    std::cout << "hahaha" << std::endl;
//    if (result.size() > 0){
//        std::cout << result[0].ids[0] << std::endl;
//        std::cout << result[0].distances[0] << std::endl;
//    } else {
//        std::cout << "sheep is a shadiao";
//    }
    auto t2 = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();

    std::cout << "Query run time: " << duration/1000.0 << "ms" << std::endl;
    return 0;
}

