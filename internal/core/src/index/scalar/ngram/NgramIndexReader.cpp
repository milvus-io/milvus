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

#include "index/scalar/ngram/NgramIndexReader.h"

#include <algorithm>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "common/RegexQuery.h"
#include "storage/artifact/LocalDirectory.h"
#include "index/scalar/ngram/NgramRegex.h"
#include "tantivy-wrapper.h"

namespace milvus::index {
namespace {

constexpr size_t kLargeRowThreshold = 5000;
constexpr size_t kMediumRowThreshold = 1000;
constexpr size_t kSmallRowThreshold = 100;
constexpr double kPreFilterHitRateThreshold = 0.20;
constexpr size_t kMaxIterations = 5;
constexpr double kBreakThreshold = 0.002;
constexpr size_t kMaxIterationsForMediumRow = 3;
constexpr double kBreakThresholdForSmallRow = 0.01;
constexpr size_t kMaxIterationsForSmallRow = 2;

size_t
Utf8LiteralLength(const std::string& literal) {
    return Utf8CharCount(literal.data(), literal.size());
}

std::vector<std::string>
split_by_wildcard(const std::string& literal) {
    std::vector<std::string> result;
    std::string r;
    r.reserve(literal.size());
    bool escape_mode = false;
    for (char c : literal) {
        if (escape_mode) {
            r += c;
            escape_mode = false;
        } else {
            if (c == '\\') {
                // consider case "\\%", we should reserve %
                escape_mode = true;
            } else if (c == '%' || c == '_') {
                if (r.length() > 0) {
                    result.push_back(std::move(r));
                    r.clear();
                }
            } else {
                r += c;
            }
        }
    }
    if (r.length() > 0) {
        result.push_back(std::move(r));
    }
    return result;
}

int64_t
ToUsageBytes(size_t bytes) {
    if (bytes > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
        ThrowInfo(DataFormatBroken,
                  "NGRAM reader resource size exceeds int64 domain");
    }
    return static_cast<int64_t>(bytes);
}

std::string
OwnString(std::string_view value) {
    return value.empty() ? std::string{}
                         : std::string(value.data(), value.size());
}

}  // namespace

NgramIndexReader::NgramIndexReader(
    std::shared_ptr<storage::LocalDirectory> directory,
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
    std::shared_ptr<const std::vector<size_t>> null_offsets,
    DataType value_type,
    uintptr_t min_gram,
    uintptr_t max_gram,
    size_t avg_row_size,
    bool mmap,
    size_t engine_bytes)
    : directory_(std::move(directory)),
      engine_(std::move(engine)),
      value_type_(value_type),
      min_gram_(min_gram),
      max_gram_(max_gram),
      avg_row_size_(avg_row_size),
      mmap_(mmap),
      engine_bytes_(engine_bytes) {
    AssertInfo(engine_ != nullptr, "NGRAM reader requires an engine");
    AssertInfo(null_offsets != nullptr,
               "NGRAM reader requires immutable null offsets");
    AssertInfo(IsStringDataType(value_type_),
               "NGRAM reader requires a string value type, got {}",
               static_cast<int>(value_type_));
    AssertInfo(min_gram_ > 0 && min_gram_ <= max_gram_,
               "NGRAM reader has invalid gram range {}..{}",
               min_gram_,
               max_gram_);
    AssertInfo(!mmap_ || directory_ != nullptr,
               "mmap NGRAM reader requires a directory owner");
    count_ = engine_->count();

    size_t previous = 0;
    bool first = true;
    for (const auto offset : *null_offsets) {
        if ((!first && offset <= previous) || offset >= count_) {
            ThrowInfo(DataFormatBroken,
                      "invalid NGRAM null offset {} for count {}",
                      offset,
                      count_);
        }
        previous = offset;
        first = false;
    }

    // See the valid_bitmap_ declaration: materialize validity once here so no
    // query replays the offsets, and keep the all-valid case allocation-free.
    if (null_offsets->empty()) {
        all_valid_ = true;
    } else {
        valid_bitmap_ = TargetBitmap(count_, true);
        for (const auto offset : *null_offsets) {
            valid_bitmap_.reset(offset);
        }
    }
}

NgramIndexReader::~NgramIndexReader() = default;

ReaderCaps
NgramIndexReader::Caps() const {
    return ReaderCaps{.ngram_candidates = true, .exact = false};
}

Domain
NgramIndexReader::CoordDomain() const {
    return Domain::Row;
}

int64_t
NgramIndexReader::Count() const {
    return static_cast<int64_t>(count_);
}

DataType
NgramIndexReader::ValueType() const {
    return value_type_;
}

int64_t
NgramIndexReader::MemoryUsage() const {
    constexpr size_t kKnownMetadataBytes =
        sizeof(NgramIndexReader) + sizeof(milvus::tantivy::TantivyIndexWrapper);
    const auto validity_bytes = valid_bitmap_.size_in_bytes();
    if (validity_bytes >
        std::numeric_limits<size_t>::max() - kKnownMetadataBytes) {
        ThrowInfo(DataFormatBroken, "NGRAM reader memory size overflows");
    }
    auto total = kKnownMetadataBytes + validity_bytes;
    if (directory_ != nullptr) {
        const auto directory_bytes = directory_->HeapBytes();
        if (directory_bytes > std::numeric_limits<size_t>::max() - total) {
            ThrowInfo(DataFormatBroken, "NGRAM reader memory size overflows");
        }
        total += directory_bytes;
    }
    if (!mmap_) {
        if (engine_bytes_ > std::numeric_limits<size_t>::max() - total) {
            ThrowInfo(DataFormatBroken, "NGRAM reader memory size overflows");
        }
        total += engine_bytes_;
    }
    return ToUsageBytes(total);
}

cachinglayer::ResourceUsage
NgramIndexReader::CellByteSize() const {
    return {MemoryUsage(), mmap_ ? ToUsageBytes(engine_bytes_) : 0};
}

bool
NgramIndexReader::CanHandle(std::string_view literal, PatternOp op) const {
    const auto owned = OwnString(literal);
    switch (op) {
        case PatternOp::Match: {
            const auto literals = split_by_wildcard(owned);
            if (literals.empty()) {
                return false;
            }
            return std::all_of(
                literals.begin(), literals.end(), [this](const auto& part) {
                    return Utf8LiteralLength(part) >= min_gram_;
                });
        }
        case PatternOp::RegexMatch:
            // The plan is TRUE when the pattern, or any OR branch of it, has
            // no literal of min_gram characters (including empty-match and
            // over-budget patterns); the executor then scans instead.
            return !PlanNgramRegex(owned, min_gram_).IsTrue();
        case PatternOp::InnerMatch:
        case PatternOp::PrefixMatch:
        case PatternOp::PostfixMatch:
            return Utf8LiteralLength(owned) >= min_gram_;
    }
    return false;
}

void
NgramIndexReader::Candidates(std::string_view literal,
                             PatternOp op,
                             TargetBitmap& candidates) const {
    AssertInfo(candidates.size() == count_,
               "NGRAM candidates size {} disagrees with count {}",
               candidates.size(),
               count_);
    if (count_ == 0 || candidates.none()) {
        return;
    }

    const auto owned = OwnString(literal);
    if (op == PatternOp::RegexMatch) {
        // AND the literals inside each branch, union the OR branches. Every
        // candidate is rechecked with the canonical RE2 matcher in phase 2.
        const auto plan = PlanNgramRegex(owned, min_gram_);
        AssertInfo(!plan.IsTrue(),
                   "NGRAM cannot handle the requested literal and operation");
        ApplyNgramRegexPlan(
            plan,
            candidates,
            [this](const std::vector<std::string>& literals,
                   TargetBitmap& bitmap) { ApplyLiterals(literals, bitmap); });
        return;
    }

    std::vector<std::string> literals;
    switch (op) {
        case PatternOp::Match:
            literals = split_by_wildcard(owned);
            if (std::any_of(
                    literals.begin(), literals.end(), [this](const auto& part) {
                        return Utf8LiteralLength(part) < min_gram_;
                    })) {
                literals.clear();
            }
            break;
        case PatternOp::RegexMatch:
            break;
        case PatternOp::InnerMatch:
        case PatternOp::PrefixMatch:
        case PatternOp::PostfixMatch:
            if (Utf8LiteralLength(owned) >= min_gram_) {
                literals.push_back(owned);
            }
            break;
    }
    AssertInfo(!literals.empty(),
               "NGRAM cannot handle the requested literal and operation");
    ApplyLiterals(literals, candidates);
}

void
NgramIndexReader::ApplyLiterals(const std::vector<std::string>& literals,
                                TargetBitmap& candidates) const {
    if (candidates.none()) {
        return;
    }
    const auto pre_count = candidates.count();
    const auto pre_filter_hit_rate =
        static_cast<double>(pre_count) / static_cast<double>(count_);
    if (ShouldUseBatchStrategy(pre_filter_hit_rate)) {
        for (const auto& part : literals) {
            TargetBitmap hits(count_);
            engine_->ngram_match_query(part, min_gram_, max_gram_, &hits);
            candidates &= hits;
            if (candidates.none()) {
                return;
            }
        }
        return;
    }

    const auto terms = engine_->ngram_tokenize(literals, min_gram_, max_gram_);
    AssertInfo(!terms.empty(),
               "NGRAM tokenizer returned no terms for a usable literal");
    ApplyIterativeNgramFilter(terms, count_, candidates);
}

TargetBitmap
NgramIndexReader::IsNull() const {
    if (all_valid_) {
        return TargetBitmap(count_);
    }
    auto result = valid_bitmap_.clone();
    result.flip();
    return result;
}

TargetBitmap
NgramIndexReader::IsNotNull() const {
    if (all_valid_) {
        return TargetBitmap(count_, true);
    }
    return valid_bitmap_.clone();
}

bool
NgramIndexReader::ShouldUseBatchStrategy(double pre_filter_hit_rate) const {
    return avg_row_size_ >= kLargeRowThreshold ||
           (avg_row_size_ >= kMediumRowThreshold &&
            pre_filter_hit_rate > kPreFilterHitRateThreshold);
}

void
NgramIndexReader::ApplyIterativeNgramFilter(
    const std::vector<std::string>& sorted_terms,
    size_t total_count,
    TargetBitmap& bitset) const {
    auto max_iterations = kMaxIterations;
    if (avg_row_size_ < kSmallRowThreshold) {
        max_iterations = kMaxIterationsForSmallRow;
    } else if (avg_row_size_ < kMediumRowThreshold) {
        max_iterations = kMaxIterationsForMediumRow;
    }

    const auto iterations = std::min(sorted_terms.size(), max_iterations);
    for (size_t i = 0; i < iterations; ++i) {
        TargetBitmap term_bitset(total_count);
        engine_->ngram_term_posting_list(sorted_terms[i], &term_bitset);
        bitset &= term_bitset;

        const auto current_hit_rate = static_cast<double>(bitset.count()) /
                                      static_cast<double>(total_count);
        if (current_hit_rate < kBreakThreshold ||
            (avg_row_size_ < kSmallRowThreshold &&
             current_hit_rate < kBreakThresholdForSmallRow)) {
            break;
        }
    }
}

}  // namespace milvus::index
