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

#pragma once

#include "server/delivery/request/BaseRequest.h"

#include <memory>
#include <string>
#include <vector>

namespace milvus {
namespace server {

class SearchRequest : public BaseRequest {
 public:
    static BaseRequestPtr
    Create(const std::shared_ptr<Context>& context, const std::string& table_name, int64_t record_size,
           const std::vector<float>& data_list, const std::vector<Range>& range_list, int64_t topk, int64_t nprobe,
           const std::vector<std::string>& partition_list, const std::vector<std::string>& file_id_list,
           TopKQueryResult& result);

 protected:
    SearchRequest(const std::shared_ptr<Context>& context, const std::string& table_name, int64_t record_size,
                  const std::vector<float>& data_list, const std::vector<Range>& range_list, int64_t topk,
                  int64_t nprobe, const std::vector<std::string>& partition_list,
                  const std::vector<std::string>& file_id_list, TopKQueryResult& result);

    Status
    OnExecute() override;

 private:
    const std::string table_name_;
    int64_t record_size_;
    const std::vector<float>& data_list_;
    const std::vector<Range> range_list_;
    int64_t topk_;
    int64_t nprobe_;
    const std::vector<std::string> partition_list_;
    const std::vector<std::string> file_id_list_;

    TopKQueryResult& result_;
};

}  // namespace server
}  // namespace milvus
