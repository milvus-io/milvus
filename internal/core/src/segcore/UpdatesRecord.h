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

#include <functional>
#include <memory>
#include <optional>
#include <variant>
#include <vector>

#include "common/Schema.h"
#include "pb/schema.pb.h"

namespace milvus::segcore {

struct UpdateAssignment {
    FieldId field;
    proto::schema::UpdateLiteral literal;
};

using UpdateString = std::shared_ptr<const std::string>;
using UpdateArray = std::shared_ptr<const proto::schema::ScalarField>;
using UpdateValue = std::variant<std::monostate,
                                 bool,
                                 int8_t,
                                 int16_t,
                                 int32_t,
                                 int64_t,
                                 UpdateString,
                                 UpdateArray>;

// One store per physical segment. Absence means native base fallback;
// monostate is a visible SET NULL. Variable-length lookup results own their data.
class UpdatesRecord {
 public:
    UpdatesRecord();
    ~UpdatesRecord();
    void
    Grow(size_t rows);
    void
    Apply(const Schema& schema,
          size_t row_count,
          const std::vector<int64_t>& offsets,
          Timestamp timestamp,
          uint32_t ordinal,
          const std::vector<UpdateAssignment>& assignments,
          const std::function<void()>& before_publish = {});
    std::optional<UpdateValue>
    Lookup(FieldId field, int64_t offset, Timestamp timestamp) const;

    struct FieldStats {
        size_t entries = 0;
        size_t distinct_offsets = 0;
        size_t membership_size = 0;
    };
    FieldStats
    Stats(FieldId field) const;

 private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace milvus::segcore
