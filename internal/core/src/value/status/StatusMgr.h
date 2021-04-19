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

#include <string>
#include <unordered_map>

#include "value/ValueMgr.h"
#include "value/ValueType.h"

namespace milvus {

class StatusMgr : public ValueMgr {
 public:
    static ValueMgr&
    GetInstance() {
        return instance;
    }

 private:
    static StatusMgr instance;

 public:
    StatusMgr();

    /* throws std::exception only */
    void
    Set(const std::string& name, const std::string& value, bool update) override;

    /* throws std::exception only */
    std::string
    Get(const std::string& name) const override;

 private:
    const std::unordered_map<std::string, BaseValuePtr>& status_list_ = value_list_;
};

}  // namespace milvus
