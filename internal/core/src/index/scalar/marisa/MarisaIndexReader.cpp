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

#include "index/scalar/marisa/MarisaIndexReader.h"

#include <limits>
#include <sys/mman.h>
#include <utility>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/RegexQuery.h"
#include "common/Utils.h"

namespace milvus::index {
namespace {

bool
IsNullKeyId(int64_t key_id) {
    return key_id == static_cast<int64_t>(MARISA_NULL_KEY_ID);
}

void
SetRowsForKey(const MarisaIndexStorage& data,
              size_t key_id,
              bool value,
              TargetBitmap& result) {
    AssertInfo(key_id < data.csr_num_keys,
               "marisa key id {} exceeds key count {}",
               key_id,
               data.csr_num_keys);
    for (size_t i = data.csr_index[key_id]; i < data.csr_index[key_id + 1];
         ++i) {
        result[data.csr_offsets[i]] = value;
    }
}

std::string_view
ReverseKey(const marisa::Trie& trie, size_t key_id, marisa::Agent& agent) {
    agent.set_query(key_id);
    trie.reverse_lookup(agent);
    return {agent.key().ptr(), agent.key().length()};
}

std::string_view
LegacyCStringValue(std::string_view value) {
    return value.substr(0, value.find('\0'));
}

void
AddUsageBytes(size_t& total, size_t value) {
    AssertInfo(value <= std::numeric_limits<size_t>::max() - total,
               "marisa reader resource usage overflows size_t");
    total += value;
}

int64_t
ToUsageBytes(size_t value) {
    AssertInfo(
        value <= static_cast<size_t>(std::numeric_limits<int64_t>::max()),
        "marisa reader resource usage exceeds int64 capacity");
    return static_cast<int64_t>(value);
}

}  // namespace

MarisaMmapOwner::MarisaMmapOwner(char* str_ids_data,
                                 size_t str_ids_bytes,
                                 char* csr_data,
                                 size_t csr_bytes,
                                 std::shared_ptr<storage::LocalDirectory> directory)
    : str_ids_data_(str_ids_data),
      str_ids_bytes_(str_ids_bytes),
      csr_data_(csr_data),
      csr_bytes_(csr_bytes),
      directory_(std::move(directory)) {
}

MarisaMmapOwner::~MarisaMmapOwner() {
    if (csr_data_ != nullptr) {
        munmap(csr_data_, csr_bytes_);
    }
    if (str_ids_data_ != nullptr) {
        munmap(str_ids_data_, str_ids_bytes_);
    }
}

const int64_t*
MarisaMmapOwner::StrIds() const {
    return reinterpret_cast<const int64_t*>(str_ids_data_);
}

const uint32_t*
MarisaMmapOwner::Csr() const {
    return reinterpret_cast<const uint32_t*>(csr_data_);
}

MarisaIndexReader::MarisaIndexReader(
    std::shared_ptr<const MarisaIndexStorage> storage)
    : storage_(std::move(storage)) {
    AssertInfo(storage_ != nullptr, "marisa reader requires shared storage");
    AssertInfo(storage_->trie != nullptr, "marisa reader requires a trie");
    AssertInfo(storage_->csr_num_keys == storage_->trie->num_keys(),
               "marisa reader CSR key count mismatch");
    AssertInfo(storage_->str_ids_size == 0 || storage_->str_ids != nullptr,
               "marisa reader is missing row ids");
    AssertInfo(storage_->csr_index != nullptr,
               "marisa reader is missing CSR index");
    AssertInfo(storage_->csr_index[storage_->csr_num_keys] == 0 ||
                   storage_->csr_offsets != nullptr,
               "marisa reader is missing CSR offsets");
    AssertInfo(storage_->value_type == DataType::STRING ||
                   storage_->value_type == DataType::VARCHAR ||
                   storage_->value_type == DataType::TEXT,
               "marisa reader requires STRING, VARCHAR, or TEXT");
}

MarisaIndexReader::~MarisaIndexReader() = default;

ReaderCaps
MarisaIndexReader::Caps() const {
    return ReaderCaps{.predicate = true,
                      .pattern_match = true,
                      .value_lookup = true,
                      .cheap_value_lookup = true};
}

Domain
MarisaIndexReader::CoordDomain() const {
    return Domain::Row;
}

int64_t
MarisaIndexReader::Count() const {
    return static_cast<int64_t>(storage_->str_ids_size);
}

DataType
MarisaIndexReader::ValueType() const {
    return storage_->value_type;
}

int64_t
MarisaIndexReader::MemoryUsage() const {
    size_t bytes = sizeof(MarisaIndexStorage);
    if (!storage_->mmap_owner) {
        AddUsageBytes(bytes, storage_->trie->total_size());
    }
    if (storage_->str_ids_owner) {
        AddUsageBytes(bytes,
                      storage_->str_ids_owner->capacity() * sizeof(int64_t));
    }
    if (storage_->csr_index_owner) {
        AddUsageBytes(bytes,
                      storage_->csr_index_owner->capacity() * sizeof(uint32_t));
    }
    if (storage_->csr_offsets_owner) {
        AddUsageBytes(
            bytes, storage_->csr_offsets_owner->capacity() * sizeof(uint32_t));
    }
    return ToUsageBytes(bytes);
}

cachinglayer::ResourceUsage
MarisaIndexReader::CellByteSize() const {
    return {MemoryUsage(), ToUsageBytes(storage_->file_backed_bytes)};
}

TargetBitmap
MarisaIndexReader::In(size_t n, const std::string_view* values) const {
    AssertInfo(n == 0 || values != nullptr,
               "marisa In received a null value array");
    TargetBitmap result(storage_->str_ids_size);
    for (size_t i = 0; i < n; ++i) {
        const auto key_id = LookupKeyId(values[i]);
        if (key_id != MARISA_INVALID_KEY_ID) {
            SetRowsForKey(*storage_, key_id, true, result);
        }
    }
    return result;
}

TargetBitmap
MarisaIndexReader::NotIn(size_t n, const std::string_view* values) const {
    AssertInfo(n == 0 || values != nullptr,
               "marisa NotIn received a null value array");
    TargetBitmap result(storage_->str_ids_size, true);
    for (size_t i = 0; i < n; ++i) {
        const auto key_id = LookupKeyId(values[i]);
        if (key_id != MARISA_INVALID_KEY_ID) {
            SetRowsForKey(*storage_, key_id, false, result);
        }
    }
    for (size_t row = 0; row < storage_->str_ids_size; ++row) {
        if (IsNullKeyId(storage_->str_ids[row])) {
            result.reset(row);
        }
    }
    return result;
}

TargetBitmap
MarisaIndexReader::Range(const std::string_view& value, CompareOp op) const {
    if (op == CompareOp::Equal) {
        return In(1, &value);
    }
    if (op == CompareOp::NotEqual) {
        return NotIn(1, &value);
    }

    TargetBitmap result(storage_->str_ids_size);
    const bool ordered = InLexicographicOrder();
    marisa::Agent agent;
    while (storage_->trie->predictive_search(agent)) {
        const std::string_view key(agent.key().ptr(), agent.key().length());
        bool matches = false;
        switch (op) {
            case CompareOp::GreaterThan:
                matches = key > value;
                break;
            case CompareOp::GreaterEqual:
                matches = key >= value;
                break;
            case CompareOp::LessThan:
                matches = key < value;
                break;
            case CompareOp::LessEqual:
                matches = key <= value;
                break;
            case CompareOp::Equal:
            case CompareOp::NotEqual:
                break;
            default:
                ThrowInfo(OpTypeInvalid,
                          "invalid comparison operator for marisa range");
        }
        if (matches) {
            SetRowsForKey(*storage_, agent.key().id(), true, result);
        } else if (ordered &&
                   (op == CompareOp::LessThan || op == CompareOp::LessEqual) &&
                   key >= value) {
            break;
        }
    }
    return result;
}

TargetBitmap
MarisaIndexReader::Range(const std::string_view& lo,
                         bool lo_inc,
                         const std::string_view& hi,
                         bool hi_inc) const {
    TargetBitmap result(storage_->str_ids_size);
    if (lo > hi || (lo == hi && !(lo_inc && hi_inc))) {
        return result;
    }

    size_t common_size = 0;
    while (common_size < lo.size() && common_size < hi.size() &&
           lo[common_size] == hi[common_size]) {
        ++common_size;
    }

    marisa::Agent agent;
    agent.set_query(lo.data(), common_size);
    const bool ordered = InLexicographicOrder();
    while (storage_->trie->predictive_search(agent)) {
        const std::string_view key(agent.key().ptr(), agent.key().length());
        const bool above_lo = lo_inc ? key >= lo : key > lo;
        const bool below_hi = hi_inc ? key <= hi : key < hi;
        if (above_lo && below_hi) {
            SetRowsForKey(*storage_, agent.key().id(), true, result);
        } else if (ordered && !below_hi) {
            break;
        }
    }
    return result;
}

std::optional<std::string>
MarisaIndexReader::Lookup(int64_t offset) const {
    AssertInfo(
        offset >= 0 && static_cast<size_t>(offset) < storage_->str_ids_size,
        "marisa lookup offset {} is out of range [0, {})",
        offset,
        storage_->str_ids_size);
    const auto key_id = storage_->str_ids[offset];
    if (IsNullKeyId(key_id)) {
        return std::nullopt;
    }
    marisa::Agent agent;
    const auto value =
        ReverseKey(*storage_->trie, static_cast<size_t>(key_id), agent);
    return std::string(value);
}

void
MarisaIndexReader::Gather(
    const int64_t* offsets,
    int64_t count,
    const std::function<void(int64_t i, const std::string_view*, bool valid)>&
        out) const {
    AssertInfo(count >= 0, "marisa gather count must not be negative");
    AssertInfo(count == 0 || offsets != nullptr,
               "marisa Gather received a null offset array");
    marisa::Agent agent;
    for (int64_t i = 0; i < count; ++i) {
        const auto offset = offsets[i];
        AssertInfo(
            offset >= 0 && static_cast<size_t>(offset) < storage_->str_ids_size,
            "marisa gather offset {} is out of range [0, {})",
            offset,
            storage_->str_ids_size);
        const auto key_id = storage_->str_ids[offset];
        if (IsNullKeyId(key_id)) {
            out(i, nullptr, false);
            continue;
        }
        const auto value =
            ReverseKey(*storage_->trie, static_cast<size_t>(key_id), agent);
        out(i, &value, true);
    }
}

TargetBitmap
MarisaIndexReader::PatternMatch(std::string_view pattern, PatternOp op) const {
    TargetBitmap result(storage_->str_ids_size);
    if (op == PatternOp::PrefixMatch) {
        for (const auto key_id : PrefixMatchKeyIds(pattern)) {
            SetRowsForKey(*storage_, key_id, true, result);
        }
        return result;
    }

    std::optional<LikePatternMatcher> like;
    std::optional<PartialRegexMatcher> regex;
    if (op == PatternOp::Match) {
        like.emplace(std::string(pattern));
    } else if (op == PatternOp::RegexMatch) {
        regex.emplace(std::string(pattern));
    }

    marisa::Agent agent;
    for (size_t key_id = 0; key_id < storage_->csr_num_keys; ++key_id) {
        if (storage_->csr_index[key_id] == storage_->csr_index[key_id + 1]) {
            continue;
        }
        const auto value = ReverseKey(*storage_->trie, key_id, agent);
        bool matches = false;
        switch (op) {
            case PatternOp::Match:
                matches = (*like)(value);
                break;
            case PatternOp::PostfixMatch:
                matches = milvus::PostfixMatch(value, pattern);
                break;
            case PatternOp::InnerMatch:
                matches = milvus::InnerMatch(value, pattern);
                break;
            case PatternOp::RegexMatch:
                matches = (*regex)(value);
                break;
            case PatternOp::PrefixMatch:
                break;
            default:
                ThrowInfo(OpTypeInvalid,
                          "invalid pattern operator for marisa index");
        }
        if (matches) {
            SetRowsForKey(*storage_, key_id, true, result);
        }
    }
    return result;
}

TargetBitmap
MarisaIndexReader::IsNull() const {
    TargetBitmap result(storage_->str_ids_size);
    for (size_t row = 0; row < storage_->str_ids_size; ++row) {
        if (IsNullKeyId(storage_->str_ids[row])) {
            result.set(row);
        }
    }
    return result;
}

TargetBitmap
MarisaIndexReader::IsNotNull() const {
    TargetBitmap result(storage_->str_ids_size);
    for (size_t row = 0; row < storage_->str_ids_size; ++row) {
        if (!IsNullKeyId(storage_->str_ids[row])) {
            result.set(row);
        }
    }
    return result;
}

size_t
MarisaIndexReader::LookupKeyId(std::string_view value) const {
    value = LegacyCStringValue(value);
    marisa::Agent agent;
    agent.set_query(value.data(), value.size());
    if (!storage_->trie->lookup(agent)) {
        return MARISA_INVALID_KEY_ID;
    }
    return agent.key().id();
}

std::vector<size_t>
MarisaIndexReader::PrefixMatchKeyIds(std::string_view prefix) const {
    prefix = LegacyCStringValue(prefix);
    std::vector<size_t> result;
    marisa::Agent agent;
    agent.set_query(prefix.data(), prefix.size());
    while (storage_->trie->predictive_search(agent)) {
        result.push_back(agent.key().id());
    }
    return result;
}

bool
MarisaIndexReader::InLexicographicOrder() const {
    return storage_->trie->node_order() == MARISA_LABEL_ORDER;
}

}  // namespace milvus::index
