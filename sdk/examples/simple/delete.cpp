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
#include "interface/ConnectionImpl.h"
#include "utils/Utils.h"

int ID_START = 0;

void generate_ids(std::vector<int64_t> & ids_array, int count);

void generate_ids(std::vector<int64_t>& ids_array, int count) {
  for (int i = 0; i < count; i++) {
    ids_array.push_back(ID_START++);
  }
}

int
main(int argc, char *argv[]) {
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

  std::vector<int64_t> delete_ids;
  generate_ids(delete_ids, 3);
  client.DeleteEntityByID(collection_name, delete_ids);

  return 0;
}

