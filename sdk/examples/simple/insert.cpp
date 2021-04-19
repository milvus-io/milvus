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

int N = 6000000;
int DIM = 128;
int LOOP = 2000;

int ID_START = 0;
std::default_random_engine eng(42);

void generate_ids(std::vector<int64_t> & ids_array, int count);

void generate_ids(std::vector<int64_t>& ids_array, int count) {
  for (int i = 0; i < count; i++) {
    ids_array.push_back(ID_START++);
  }
}

const milvus::FieldValue GetData(int count) {
  milvus::FieldValue value_map;

  std::vector<int32_t> int32_data;

  for (int i = 0; i < count; i++) {
    int32_data.push_back(ID_START++);
  }

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
  value_map.row_num = count;
  return value_map;
}

bool check_field(milvus::FieldPtr left, milvus::FieldPtr right){

    if (left->field_name != right->field_name){
	std::cout<<"filed_name not match! want "<< left->field_name << " but get "<<right->field_name << std::endl;
	return false;
    }

    if (left->field_type != right->field_type){
	std::cout<<"filed_type not match! want "<< int(left->field_type) << " but get "<< int(right->field_type) << std::endl;
	return false;
    }


    if (left->dim != right->dim){
	std::cout<<"dim not match! want "<< left->dim << " but get "<<right->dim << std::endl;
	return false;
    }

    return true;	
}

bool check_schema(const milvus::Mapping & map){
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

    auto size_ = map.fields.size();
    for ( int i =0; i != size_; ++ i){
	auto ret = check_field(fields[i], map.fields[i]);
	if (!ret){
		return false;
	}
    }

    for (auto &f : map.fields) {
      std::cout << f->field_name << ":" << int(f->field_type) << ":" << f->dim << "DIM" << std::endl;
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


  if (parameters.id_start_ < 0){
	std::cout<< "id_start should >= 0 !" << std::endl;
	milvus_sdk::Utils::PrintHelp(argc, argv);
	return 0;
  }

  if (parameters.id_count_ <= 0){
	std::cout<< "id_count should > 0 !" << std::endl;
	milvus_sdk::Utils::PrintHelp(argc, argv);
	return 0;
  }

  if (parameters.loop_ <= 0){
	std::cout<< "loop should > 0 !" << std::endl;
	milvus_sdk::Utils::PrintHelp(argc, argv);
	return 0;
  }

  N = parameters.id_count_;
  ID_START = parameters.id_start_;
  LOOP = parameters.loop_;

  std::cout<<"N: " << N << std::endl;
  std::cout<<"ID_START: " << ID_START << std::endl;
  std::cout<<"LOOP: " << LOOP << std::endl;

    const std::string collection_name = parameters.collection_name_;
    auto client = milvus::ConnectionImpl();

    milvus::ConnectParam connect_param;
    connect_param.ip_address = parameters.address_.empty() ? "127.0.0.1":parameters.address_;
    connect_param.port = parameters.port_.empty() ? "19530":parameters.port_ ;
    client.Connect(connect_param);


    milvus::Mapping map;
    client.GetCollectionInfo(collection_name, map);

    auto check_ret = check_schema(map);
    if (!check_ret){
	std::cout<<" Schema is not right!"<< std::endl;
	return 0;
    }

    int per_count = N / LOOP;
    int failed_count = 0;

    std::cout<<"PER_COUNT: " << per_count << std::endl;

    milvus_sdk::TimeRecorder insert_timer("insert");
    for (int64_t i = 0, j=0; j < N;) {
	    i=j;
	    j += per_count;
	    if( j > N ) j = N;

            std::vector<int64_t> ids_array;
	    generate_ids(ids_array, j - i);
	    auto data = GetData(j - i);
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
