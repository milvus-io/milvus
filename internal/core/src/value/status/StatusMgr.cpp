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

#include "value/status/StatusMgr.h"

namespace milvus {

extern std::unordered_map<std::string, BaseValuePtr>
InitStatus();

StatusMgr StatusMgr::instance;

StatusMgr::StatusMgr() : ValueMgr(InitStatus()) {
}

void
StatusMgr::Set(const std::string& name, const std::string& value, bool update) {
    /* Check if existed */
    if (status_list_.find(name) == status_list_.end()) {
        throw std::runtime_error("Status " + name + " not found.");
    }

    try {
        /* Set value, throws ValueError only. */
        status_list_.at(name)->Set(value, update);
    } catch (ValueError& e) {
        /* Convert to std::runtime_error. */
        throw std::runtime_error(e.message());
    } catch (...) {
        /* Unexpected exception, output status and value. */
        throw std::runtime_error("Unexpected exception happened when setting " + value + " to " + name + ".");
    }
}

std::string
StatusMgr::Get(const std::string& name) const {
    try {
        auto& status = status_list_.at(name);
        return status->Get();
    } catch (std::out_of_range& ex) {
        throw std::runtime_error("Status " + name + " not found.");
    } catch (...) {
        throw std::runtime_error("Unexpected exception happened when getting status " + name + ".");
    }
}

}  // namespace milvus
