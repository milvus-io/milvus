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

#include <memory>

#include "SearchTask.h"

namespace milvus {
namespace scheduler {

class TestTask : public XSearchTask {
 public:
    explicit TestTask(const std::shared_ptr<server::Context>& context, TableFileSchemaPtr& file, TaskLabelPtr label);

 public:
    void
    Load(LoadType type, uint8_t device_id) override;

    void
    Execute() override;

    void
    Wait();

 public:
    uint64_t load_count_ = 0;
    uint64_t exec_count_ = 0;

    bool done_ = false;
    std::mutex mutex_;
    std::condition_variable cv_;
};

}  // namespace scheduler
}  // namespace milvus
