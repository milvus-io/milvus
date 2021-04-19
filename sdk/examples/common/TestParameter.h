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

#pragma once

// #include "include/MilvusApi.h"

#include <string>
#include <list>
#include <memory>
#include <vector>
#include <future>

struct TestParameters {
    // specify this will ignore index_type/index_file_size/nlist/metric_type/dimension/dow_count
    std::string address_;
    std::string port_;
    std::string collection_name_;

    int64_t id_start_ = -1;
    int64_t id_count_ = 0;
    int64_t loop_ = 0;

    // query parameters
    int64_t topk_ = 10;
    bool is_valid = true;
};

