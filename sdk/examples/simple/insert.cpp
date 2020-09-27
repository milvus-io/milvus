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
const int DIM = 128;
const int LOOP = 100;
int ID_START = 0;

int generate_ids(std::vector<int64_t> & ids_array, int count);

int generate_ids(std::vector<int64_t>& ids_array, int count) {
  for (int i = 0; i < count; i++) {
    ids_array.push_back(ID_START++);
  }
  return 0;
}

const milvus::FieldValue GetData(int count) {
  milvus::FieldValue value_map;

  std::vector<int32_t> int32_data;

  for (int i = 0; i < count; i++) {
    int32_data.push_back(ID_START++);
  }

  std::default_random_engine eng(42);
  std::normal_distribution<float> dis(0, 1);
  std::vector<milvus::VectorData> vector_data;
  for (int i = 0; i < count; i++) {
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

bool checkSchema(){
    // Get Collection info
    bool ret = false;

    milvus::FieldPtr field_ptr1 = std::make_shared<milvus::Field>();
    milvus::FieldPtr field_ptr2 = std::make_shared<milvus::Field>();

    field_ptr1->field_name = "age";
    field_ptr1->field_type = milvus::DataType::INT32;
    field_ptr1->dim = 1;

    field_ptr2->field_name = "field_vec";
    field_ptr2->field_type = milvus::DataType::VECTOR_FLOAT;
    field_ptr2->dim = DIM;

    std::vector<milvus::FieldPtr> fields{field_ptr1, field_ptr2};

    milvus::Mapping map;
    //client.GetCollectionInfo(collection_name, map);

    for (auto &f : map.fields) {
      ///std::cout << f->field_name << ":" << int(f->field_type) << ":" << f->dim << "DIM" << std::endl;
    }

    return true;
}

int
main(int argc, char* argv[]) {
  TestParameters parameters = milvus_sdk::Utils::ParseTestParameters(argc, argv);
    if (!parameters.is_valid){
      return 0;
   }

  if (parameters.collection_name_.empty()){
	std::cout<< "should specify collection name!" << std::endl;
	milvus_sdk::Utils::PrintHelp(argc, argv);
	return 0;
  }

    const std::string collection_name = parameters.collection_name_;
    auto client = milvus::ConnectionImpl();
    milvus::ConnectParam connect_param;
    connect_param.ip_address = parameters.address_.empty() ? "127.0.0.1":parameters.address_;
    connect_param.port = parameters.port_.empty() ? "19530":parameters.port_ ;
    client.Connect(connect_param);


    int per_count = N / LOOP;
    int failed_count = 0;

    milvus_sdk::TimeRecorder insert_timer("insert");
    for (int64_t i = 0; i < LOOP; i++) {
	    std::vector<int64_t> ids_array;
	    generate_ids(ids_array, per_count);
	    auto data = GetData(per_count);
	    insert_timer.Start();
	    auto status = client.Insert(collection_name, "default", data, ids_array);
	    if (!status.ok()){
		failed_count += 1;
	    }
	    insert_timer.End();
    }
    if (failed_count > 0) {
	    std::cout <<" test done, failed_count is :" << failed_count<< std::endl;
    }
    insert_timer.Print(LOOP);
    return 0;
}
