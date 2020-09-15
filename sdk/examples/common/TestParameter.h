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

    // collection parameters, only works when collection_name_ is empty
    int64_t index_type_ = (int64_t)milvus::IndexType::IVFSQ8; // sq8
    int64_t index_file_size_ = 1024; // 1024 MB
    int64_t nlist_ = 16384;
    int64_t metric_type_ = (int64_t)milvus::MetricType::L2; // L2
    int64_t dimensions_ = 128;
    int64_t row_count_ = 1; // 1 million

    // query parameters
    int64_t concurrency_ = 20; // 20 connections
    int64_t query_count_ = 1000;
    int64_t nq_ = 1;
    int64_t topk_ = 10;
    int64_t nprobe_ = 16;
    bool print_result_ = false;
    bool is_valid = true;
};

