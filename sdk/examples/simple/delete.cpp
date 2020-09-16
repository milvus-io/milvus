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
#include "ip.h"

int
main(int argc, char *argv[]) {
  auto client = milvus::ConnectionImpl();
  milvus::ConnectParam connect_param;
  connect_param.ip_address = IP;
  connect_param.port = "19530";
  client.Connect(connect_param);

  std::vector<int64_t> delete_ids;
  delete_ids.push_back(1);
  delete_ids.push_back(2);
  delete_ids.push_back(3);
  client.DeleteEntityByID("collection0", delete_ids);

  return 0;
}

