// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "common/Array.h"
#include "common/ArrayValue.h"
#include "common/Types.h"

namespace milvus {

// One document per element in array offset order. Own the strings for the
// duration of a batch passed to the 3.0 Tantivy writer.
struct ArrayTextValues {
    std::vector<std::string> values;
    FixedVector<bool> valid;

    const bool*
    Validity() const {
        return valid.empty() ? nullptr : valid.data();
    }

    void
    AppendValue(std::string_view value, bool present) {
        if (!present && valid.empty()) {
            valid.resize(values.size(), true);
        }
        if (!present || !valid.empty()) {
            valid.push_back(present);
        }
        values.emplace_back(value);
    }

    void
    Append(const ArrayValueView& row) {
        for (size_t i = 0; i < row.size(); ++i) {
            const bool present = row.is_valid(i);
            AppendValue(present ? row.get_data_unchecked<std::string_view>(i)
                                : std::string_view{},
                        present);
        }
    }

    template <typename Row>
    void
    Append(const Row& row) {
        for (int i = 0; i < row.length(); ++i) {
            const bool present = row.is_element_valid(i);
            AppendValue(
                present ? row.template get_data_unchecked<std::string_view>(i)
                        : std::string_view{},
                present);
        }
    }
};

}  // namespace milvus
