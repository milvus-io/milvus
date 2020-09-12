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

#include "TaskLabel.h"
#include "scheduler/ResourceMgr.h"

#include <memory>
#include <string>

// class Resource;
//
// using ResourceWPtr = std::weak_ptr<Resource>;

namespace milvus {
namespace scheduler {

class SpecResLabel : public TaskLabel {
 public:
    explicit SpecResLabel(const ResourceWPtr& resource)
        : TaskLabel(TaskLabelType::SPECIFIED_RESOURCE), resource_(resource) {
    }

    inline ResourceWPtr&
    resource() {
        return resource_;
    }

    inline std::string
    name() const override {
        return resource_.lock()->name();
    }

 private:
    ResourceWPtr resource_;
};

using SpecResLabelPtr = std::shared_ptr<SpecResLabel>();

}  // namespace scheduler
}  // namespace milvus
