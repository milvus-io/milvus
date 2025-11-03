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

#include <string>

#include "unicode/brkiter.h"
#include "unicode/unistr.h"
#include "unicode/utypes.h"
#include "ankerl/unordered_dense.h"
#include "common/Types.h"
#include "arrow/api.h"
#include "log/Log.h"
#include <cstring>
namespace milvus::index {

inline bool
SupportsSkipIndex(arrow::Type::type type) {
    switch (type) {
        case arrow::Type::BOOL:
        case arrow::Type::INT8:
        case arrow::Type::INT16:
        case arrow::Type::INT32:
        case arrow::Type::INT64:
        case arrow::Type::FLOAT:
        case arrow::Type::DOUBLE:
        case arrow::Type::STRING:
            return true;
        default:
            return false;
    }
}

inline bool
SupportsSkipIndex(DataType type) {
    switch (type) {
        case DataType::BOOL:
        case DataType::INT8:
        case DataType::INT16:
        case DataType::INT32:
        case DataType::INT64:
        case DataType::FLOAT:
        case DataType::DOUBLE:
        case DataType::VARCHAR:
        case DataType::STRING:
        case DataType::TIMESTAMPTZ:
            return true;
        default:
            return false;
    }
}

inline void
ExtractNgrams(ankerl::unordered_dense::set<std::string>& ngrams_set,
              const std::string_view& text,
              size_t n) {
    if (n == 0 || text.size() < n) {
        return;
    }
    UErrorCode status = U_ZERO_ERROR;
    std::unique_ptr<icu::BreakIterator> bi(
        icu::BreakIterator::createCharacterInstance(icu::Locale::getUS(),
                                                    status));
    if (U_FAILURE(status)) {
        LOG_WARN("Failed to create ICU BreakIterator: {}", u_errorName(status));
        return;
    }

    icu::UnicodeString ustr = icu::UnicodeString::fromUTF8(
        icu::StringPiece(text.data(), text.size()));
    bi->setText(ustr);

    std::vector<int32_t> boundaries;
    boundaries.reserve(ustr.length() + 1);
    int32_t pos = bi->first();
    while (pos != icu::BreakIterator::DONE) {
        boundaries.push_back(pos);
        pos = bi->next();
    }

    size_t char_count = boundaries.size() > 0 ? boundaries.size() - 1 : 0;
    if (char_count < n) {
        return;
    }

    for (size_t i = 0; i + n < boundaries.size(); ++i) {
        int32_t start_pos = boundaries[i];
        int32_t end_pos = boundaries[i + n];
        int32_t length = end_pos - start_pos;
        if (length <= 0) {
            continue;
        }
        icu::UnicodeString ngram_ustr(ustr, start_pos, length);
        std::string ngram_utf8;
        ngram_ustr.toUTF8String(ngram_utf8);
        if (!ngram_utf8.empty()) {
            ngrams_set.insert(std::move(ngram_utf8));
        }
    }
}

}  // namespace milvus::index