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

namespace milvus {
namespace server {

class CompactRequest : public BaseRequest {
 public:
    static BaseRequestPtr
    Create(const std::shared_ptr<milvus::server::Context>& context, const std::string& collection_name,
           double compact_threshold);

 protected:
    CompactRequest(const std::shared_ptr<milvus::server::Context>& context, const std::string& collection_name,
                   double compact_threshold);

    Status
    OnExecute() override;

 private:
    const std::string collection_name_;
    double compact_threshold_ = 0.0;
};

}  // namespace server
}  // namespace milvus
