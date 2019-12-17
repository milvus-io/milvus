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

#include "examples/utils/TimeRecorder.h"

#include <iostream>

namespace milvus_sdk {

TimeRecorder::TimeRecorder(const std::string& title) : title_(title) {
    start_ = std::chrono::system_clock::now();
    std::cout << title_ << " begin..." << std::endl;
}

TimeRecorder::~TimeRecorder() {
    std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
    int64_t span = (std::chrono::duration_cast<std::chrono::milliseconds>(end - start_)).count();
    std::cout << title_ << " totally cost: " << span << " ms" << std::endl;
}

}  // namespace milvus_sdk
