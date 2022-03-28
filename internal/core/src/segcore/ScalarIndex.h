// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/Types.h"
#include "pb/schema.pb.h"

namespace milvus::segcore {

class ScalarIndexBase {
 public:
    virtual std::pair<std::unique_ptr<IdArray>, std::vector<SegOffset>>
    do_search_ids(const IdArray& ids) const = 0;
    virtual std::pair<std::vector<idx_t>, std::vector<SegOffset>>
    do_search_ids(const std::vector<idx_t>& ids) const = 0;
    virtual ~ScalarIndexBase() = default;
    virtual std::string
    debug() const = 0;
};

class ScalarIndexVector : public ScalarIndexBase {
    using T = int64_t;

 public:
    // TODO: use proto::schema::ids
    void
    append_data(const T* ids, int64_t count, SegOffset base);

    void
    build();

    std::pair<std::unique_ptr<IdArray>, std::vector<SegOffset>>
    do_search_ids(const IdArray& ids) const override;

    std::pair<std::vector<idx_t>, std::vector<SegOffset>>
    do_search_ids(const std::vector<idx_t>& ids) const override;

    std::string
    debug() const override {
        std::string dbg_str;
        for (auto pr : mapping_) {
            dbg_str += "<" + std::to_string(pr.first) + "->" + std::to_string(pr.second.get()) + ">";
        }
        return dbg_str;
    }

 private:
    std::vector<std::pair<T, SegOffset>> mapping_;
};

}  // namespace milvus::segcore
