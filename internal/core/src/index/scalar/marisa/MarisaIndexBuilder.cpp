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

#include "index/scalar/marisa/MarisaIndexBuilder.h"
#include "index/scalar/marisa/MarisaIndexParams.h"

#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <vector>

#include <marisa.h>

#include "index/ParamUtils.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "index/Families.h"
#include "index/Utils.h"
#include "index/contracts/Registry.h"
#include "index/scalar/marisa/MarisaIndexArtifact.h"

namespace milvus::index {
namespace {

using marisa_params::ParseNested;
using marisa_params::ParseValueType;

std::string_view
LegacyCStringValue(std::string_view value) {
    return value.substr(0, value.find('\0'));
}

size_t
LookupKeyId(const marisa::Trie& trie, std::string_view value) {
    marisa::Agent agent;
    agent.set_query(value.data(), value.size());
    if (!trie.lookup(agent)) {
        return MARISA_INVALID_KEY_ID;
    }
    return agent.key().id();
}

void
BuildCsr(const marisa::Trie& trie,
         const std::vector<int64_t>& str_ids,
         std::vector<uint32_t>& csr_index,
         std::vector<uint32_t>& csr_offsets) {
    const auto num_keys = trie.num_keys();
    AssertInfo(str_ids.size() <= std::numeric_limits<uint32_t>::max(),
               "segment row count {} exceeds uint32_t capacity for marisa CSR",
               str_ids.size());
    AssertInfo(num_keys < std::numeric_limits<uint32_t>::max(),
               "marisa trie key count {} exceeds uint32_t capacity for CSR",
               num_keys);

    csr_index.assign(num_keys + 1, 0);
    for (auto str_id : str_ids) {
        if (str_id == static_cast<int64_t>(MARISA_NULL_KEY_ID)) {
            continue;
        }
        AssertInfo(str_id >= 0 && static_cast<size_t>(str_id) < num_keys,
                   "invalid marisa key id {} while building CSR",
                   str_id);
        ++csr_index[static_cast<size_t>(str_id) + 1];
    }
    for (size_t i = 1; i < csr_index.size(); ++i) {
        csr_index[i] += csr_index[i - 1];
    }

    csr_offsets.resize(csr_index.back());
    std::vector<uint32_t> write_pos(csr_index.begin(), csr_index.end() - 1);
    for (size_t row = 0; row < str_ids.size(); ++row) {
        const auto str_id = str_ids[row];
        if (str_id == static_cast<int64_t>(MARISA_NULL_KEY_ID)) {
            continue;
        }
        csr_offsets[write_pos[static_cast<size_t>(str_id)]++] =
            static_cast<uint32_t>(row);
    }
}

}  // namespace

MarisaIndexBuilder::MarisaIndexBuilder(DataType value_type)
    : value_type_(value_type) {
    AssertInfo(IsStringDataType(value_type_),
               "marisa builder requires STRING, VARCHAR, or TEXT");
}

MarisaIndexBuilder::~MarisaIndexBuilder() = default;

storage::ArtifactPtr
MarisaIndexBuilder::Build(const ScalarBuildInput<std::string_view>& input) && {
    size_t row_count = 0;
    auto trie = std::make_shared<marisa::Trie>();
    {
        marisa::Keyset keyset;
        for (const auto& batch : input.batches) {
            AssertInfo(batch.values.size() <=
                           std::numeric_limits<uint32_t>::max() - row_count,
                       "marisa row count exceeds uint32_t CSR capacity");
            row_count += batch.values.size();
            for (size_t i = 0; i < batch.values.size(); ++i) {
                if (!batch.validity || batch.validity[i]) {
                    const auto value = LegacyCStringValue(batch.values[i]);
                    AssertInfo(
                        value.size() <= std::numeric_limits<uint32_t>::max(),
                        "marisa key at row {} exceeds uint32_t length capacity",
                        row_count - batch.values.size() + i);
                    keyset.push_back(value.data(), value.size());
                }
            }
        }
        trie->build(keyset, MARISA_LABEL_ORDER);
    }

    std::vector<int64_t> str_ids(row_count,
                                 static_cast<int64_t>(MARISA_NULL_KEY_ID));
    size_t row = 0;
    for (const auto& batch : input.batches) {
        for (size_t i = 0; i < batch.values.size(); ++i, ++row) {
            if (!batch.validity || batch.validity[i]) {
                const auto value = LegacyCStringValue(batch.values[i]);
                const auto key_id = LookupKeyId(*trie, value);
                AssertInfo(key_id != MARISA_INVALID_KEY_ID &&
                               key_id < trie->num_keys(),
                           "failed to resolve a key inserted into marisa trie");
                str_ids[row] = static_cast<int64_t>(key_id);
            }
        }
    }

    std::vector<uint32_t> csr_index;
    std::vector<uint32_t> csr_offsets;
    BuildCsr(*trie, str_ids, csr_index, csr_offsets);
    return std::make_unique<MarisaIndexArtifact>(std::move(trie),
                                                 std::move(str_ids),
                                                 std::move(csr_index),
                                                 std::move(csr_offsets),
                                                 value_type_);
}

namespace {

const bool kMarisaBuilderRegistered = [] {
    BuilderRegistry<ScalarBuildInput<std::string_view>>::Instance().Register(
        families::kMarisa, [](const BuildParams& params) {
            if (ParseNested(params)) {
                ThrowInfo(DataTypeInvalid,
                          "marisa indexes support row-domain strings only");
            }
            return std::make_unique<MarisaIndexBuilder>(ParseValueType(params));
        });
    return true;
}();

}  // namespace

}  // namespace milvus::index
