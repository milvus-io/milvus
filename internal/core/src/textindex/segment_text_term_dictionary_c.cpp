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

#include "textindex/segment_text_term_dictionary_c.h"

#include <limits>
#include <new>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/EasyAssert.h"
#include "monitor/scope_metric.h"
#include "segcore/SegmentInterface.h"
#include "textindex/fst/text_fst.h"
#include "textindex/segment_text_term_dictionary.h"

namespace {

uint64_t
ReadEncodedTextTermU64(std::span<const uint8_t> input, size_t* offset) {
    if (*offset > input.size() || input.size() - *offset < sizeof(uint64_t)) {
        throw std::invalid_argument("truncated encoded segment text terms");
    }
    uint64_t value = 0;
    for (size_t i = 0; i < sizeof(uint64_t); ++i) {
        value |= static_cast<uint64_t>(input[*offset + i]) << (i * 8);
    }
    *offset += sizeof(uint64_t);
    return value;
}

std::vector<std::string>
DecodeSegmentTextTerms(const uint8_t* encoded_terms, int64_t encoded_size) {
    if (encoded_size < 0 ||
        static_cast<uint64_t>(encoded_size) >
            static_cast<uint64_t>(std::numeric_limits<size_t>::max()) ||
        (encoded_size > 0 && encoded_terms == nullptr)) {
        throw std::invalid_argument("invalid encoded segment text terms");
    }
    const auto input = std::span<const uint8_t>(
        encoded_terms, static_cast<size_t>(encoded_size));
    size_t offset = 0;
    const auto count = ReadEncodedTextTermU64(input, &offset);
    if (count > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
        throw std::invalid_argument("too many encoded segment text terms");
    }
    std::vector<std::string> terms;
    terms.reserve(static_cast<size_t>(count));
    for (uint64_t i = 0; i < count; ++i) {
        const auto length = ReadEncodedTextTermU64(input, &offset);
        if (length > input.size() - offset) {
            throw std::invalid_argument("truncated encoded segment text term");
        }
        terms.emplace_back(reinterpret_cast<const char*>(input.data() + offset),
                           static_cast<size_t>(length));
        offset += static_cast<size_t>(length);
    }
    if (offset != input.size()) {
        throw std::invalid_argument(
            "trailing bytes in encoded segment text terms");
    }
    return terms;
}

void
SetTextTermTrieStats(
    CTextTermTrieUpdateResult* result,
    const milvus::textindex::SegmentTextTermDictionary& dictionary) {
    const auto stats = dictionary.TrieStats();
    if (stats.term_count >
            static_cast<size_t>(std::numeric_limits<int64_t>::max()) ||
        stats.memory_bytes >
            static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
        throw std::overflow_error(
            "segment text term Trie stats overflow int64");
    }
    result->term_count = static_cast<int64_t>(stats.term_count);
    result->memory_size = static_cast<int64_t>(stats.memory_bytes);
}

void
TrySetTextTermTrieStats(
    CTextTermTrieUpdateResult* result,
    const milvus::textindex::SegmentTextTermDictionary& dictionary) noexcept {
    try {
        SetTextTermTrieStats(result, dictionary);
    } catch (...) {
        result->term_count = 0;
        result->memory_size = 0;
    }
}

}  // namespace

CTextTermTrieUpdateResult
UpdateSegmentTextTermTrie(CSegmentInterface c_segment,
                          int64_t field_id,
                          const uint8_t* encoded_terms,
                          int64_t encoded_size) {
    SCOPE_CGO_CALL_METRIC();

    CTextTermTrieUpdateResult result{};
    milvus::textindex::SegmentTextTermDictionary* dictionary = nullptr;
    try {
        auto* segment =
            static_cast<milvus::segcore::SegmentInternalInterface*>(c_segment);
        AssertInfo(segment != nullptr, "segment conversion failed");
        AssertInfo(segment->type() == ::SegmentType::Growing,
                   "cannot update text term Trie for non-growing segment");
        dictionary = &segment->GetTextTermDictionary();
        auto terms = DecodeSegmentTextTerms(encoded_terms, encoded_size);
        dictionary->AddTerms(field_id, terms);
        SetTextTermTrieStats(&result, *dictionary);
        result.status = milvus::SuccessCStatus();
    } catch (const std::bad_alloc& e) {
        if (dictionary != nullptr) {
            TrySetTextTermTrieStats(&result, *dictionary);
        }
        result.status =
            milvus::FailureCStatus(milvus::MemAllocateFailed, e.what());
    } catch (const std::exception& e) {
        if (dictionary != nullptr) {
            TrySetTextTermTrieStats(&result, *dictionary);
        }
        result.status = milvus::FailureCStatus(&e);
    } catch (...) {
        result.status = milvus::FailureCStatus(
            milvus::UnexpectedError,
            "unknown segment text term Trie update failure");
    }
    return result;
}

CTextTermTrieUpdateResult
AddSegmentTextTermFstsToTrie(CSegmentInterface c_segment,
                             int64_t field_id,
                             const CTextFstHandle* fst_handles,
                             int64_t fst_count) {
    SCOPE_CGO_CALL_METRIC();

    CTextTermTrieUpdateResult result{};
    milvus::textindex::SegmentTextTermDictionary* dictionary = nullptr;
    bool importing_persisted_terms = false;
    try {
        if (c_segment == nullptr || fst_count < 0 ||
            (fst_count > 0 && fst_handles == nullptr)) {
            result.status = milvus::FailureCStatus(
                milvus::UnexpectedError,
                "invalid segment text term FST import request");
            return result;
        }

        auto* segment =
            static_cast<milvus::segcore::SegmentInternalInterface*>(c_segment);
        AssertInfo(segment->type() == ::SegmentType::Growing,
                   "cannot import text term FSTs into non-growing segment");
        dictionary = &segment->GetTextTermDictionary();
        std::vector<const milvus::textindex::TextFst*> fsts;
        fsts.reserve(static_cast<size_t>(fst_count));
        for (int64_t i = 0; i < fst_count; ++i) {
            if (fst_handles[i] == nullptr) {
                throw std::invalid_argument("text term FST handle is null");
            }
            fsts.push_back(
                static_cast<const milvus::textindex::TextFst*>(fst_handles[i]));
        }

        importing_persisted_terms = true;
        dictionary->AddFstTerms(field_id, fsts);
        importing_persisted_terms = false;
        SetTextTermTrieStats(&result, *dictionary);
        result.status = milvus::SuccessCStatus();
    } catch (const std::bad_alloc& e) {
        if (dictionary != nullptr) {
            TrySetTextTermTrieStats(&result, *dictionary);
        }
        result.status =
            milvus::FailureCStatus(milvus::MemAllocateFailed, e.what());
    } catch (const std::exception& e) {
        if (dictionary != nullptr) {
            TrySetTextTermTrieStats(&result, *dictionary);
        }
        if (importing_persisted_terms) {
            result.status =
                milvus::FailureCStatus(milvus::DataFormatBroken, e.what());
            result.is_data_integrity_error = true;
        } else {
            result.status = milvus::FailureCStatus(&e);
        }
    } catch (...) {
        if (dictionary != nullptr) {
            TrySetTextTermTrieStats(&result, *dictionary);
        }
        if (importing_persisted_terms) {
            result.status = milvus::FailureCStatus(
                milvus::DataFormatBroken,
                "unknown persisted text term FST import failure");
            result.is_data_integrity_error = true;
        } else {
            result.status = milvus::FailureCStatus(
                milvus::UnexpectedError,
                "unknown segment text term FST import failure");
        }
    }
    return result;
}
