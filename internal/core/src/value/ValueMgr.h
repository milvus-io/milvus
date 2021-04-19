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

#include <list>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>

#include "value/ValueType.h"

namespace milvus {
class ValueObserver {
 public:
    virtual ~ValueObserver() = default;

    virtual void
    ValueUpdate(const std::string& name) = 0;
};

class BaseValueMgr {
 protected:
    BaseValueMgr() = default;

 public:
    // Shared pointer should not be used here
    void
    Attach(const std::string& name, ValueObserver* observer);

    void
    Detach(const std::string& name, ValueObserver* observer);

 protected:
    virtual void
    Notify(const std::string& name);

 private:
    std::unordered_map<std::string, std::list<ValueObserver*>> observers_;
    std::mutex observer_mutex_;
};

class ValueMgr : public BaseValueMgr {
 public:
    explicit ValueMgr(std::unordered_map<std::string, BaseValuePtr> init_list) : value_list_(std::move(init_list)) {
    }

    ValueMgr(const ValueMgr&) = delete;
    ValueMgr&
    operator=(const ValueMgr&) = delete;

    ValueMgr(ValueMgr&&) = delete;
    ValueMgr&
    operator=(ValueMgr&&) = delete;

 public:
    void
    Init();

    virtual void
    Set(const std::string& name, const std::string& value, bool update) = 0;

    virtual std::string
    Get(const std::string& name) const = 0;

    std::string
    Dump() const;

    std::string
    JsonDump() const;

 protected:
    const std::unordered_map<std::string, BaseValuePtr> value_list_;
};
}  // namespace milvus
