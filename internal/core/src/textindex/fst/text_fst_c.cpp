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

#include "textindex/fst/text_fst_c.h"

#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <system_error>

#include "common/EasyAssert.h"
#include "textindex/fst/text_fst.h"

namespace {

using milvus::textindex::TextFst;

uint64_t
ReadU64(std::span<const uint8_t> input, size_t* offset) {
    if (*offset > input.size() || input.size() - *offset < sizeof(uint64_t)) {
        throw std::invalid_argument("truncated encoded text FST terms");
    }
    uint64_t value = 0;
    for (size_t i = 0; i < sizeof(uint64_t); ++i) {
        value |= static_cast<uint64_t>(input[*offset + i]) << (i * 8);
    }
    *offset += sizeof(uint64_t);
    return value;
}

class EncodedTermReader {
 public:
    EncodedTermReader(const uint8_t* encoded_terms, int64_t encoded_size) {
        if (encoded_size < 0 ||
            static_cast<uint64_t>(encoded_size) >
                static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
            throw std::invalid_argument("invalid encoded text FST size");
        }
        if (encoded_size > 0 && encoded_terms == nullptr) {
            throw std::invalid_argument("encoded text FST terms are null");
        }

        input_ = std::span<const uint8_t>(encoded_terms,
                                          static_cast<size_t>(encoded_size));
        count_ = ReadU64(input_, &offset_);
        if (count_ >
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            throw std::invalid_argument("too many encoded text FST terms");
        }
        if (count_ > (input_.size() - offset_) / sizeof(uint64_t)) {
            throw std::invalid_argument("truncated encoded text FST terms");
        }
        remaining_ = count_;
    }

    std::optional<std::string_view>
    Next() {
        if (remaining_ == 0) {
            if (offset_ != input_.size()) {
                throw std::invalid_argument(
                    "trailing bytes in encoded text FST terms");
            }
            return std::nullopt;
        }

        const auto length = ReadU64(input_, &offset_);
        if (length > input_.size() - offset_) {
            throw std::invalid_argument("truncated encoded text FST term");
        }
        const auto term = std::string_view(
            reinterpret_cast<const char*>(input_.data() + offset_),
            static_cast<size_t>(length));
        offset_ += static_cast<size_t>(length);
        --remaining_;
        return term;
    }

    int64_t
    TermCount() const {
        return static_cast<int64_t>(count_);
    }

 private:
    std::span<const uint8_t> input_;
    size_t offset_ = 0;
    uint64_t count_ = 0;
    uint64_t remaining_ = 0;
};

}  // namespace

CTextFstBuildResult
BuildTextFst(const uint8_t* encoded_terms, int64_t encoded_size) {
    CTextFstBuildResult result{};
    try {
        EncodedTermReader terms(encoded_terms, encoded_size);
        auto fst = std::make_unique<TextFst>();
        fst->Build([&]() { return terms.Next(); });
        if (!fst->VerifyChecksum()) {
            throw std::runtime_error(
                "built text FST failed checksum verification");
        }
        const auto bytes = fst->SerializedBytes();
        if (bytes.size() >
            static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
            throw std::runtime_error("built text FST is too large");
        }
        result.data = bytes.data();
        result.data_size = static_cast<int64_t>(bytes.size());
        result.term_count = terms.TermCount();
        result.status = milvus::SuccessCStatus();
        result.handle = fst.release();
    } catch (const std::bad_alloc& e) {
        result.data = nullptr;
        result.data_size = 0;
        result.term_count = 0;
        result.handle = nullptr;
        result.status =
            milvus::FailureCStatus(milvus::MemAllocateFailed, e.what());
    } catch (const std::exception& e) {
        result.data = nullptr;
        result.data_size = 0;
        result.term_count = 0;
        result.handle = nullptr;
        result.status = milvus::FailureCStatus(&e);
    } catch (...) {
        result.data = nullptr;
        result.data_size = 0;
        result.term_count = 0;
        result.handle = nullptr;
        result.status = milvus::FailureCStatus(
            milvus::UnexpectedError, "unknown text FST build failure");
    }
    return result;
}

namespace {

CTextFstLoadResult
MakeTextFstLoadResult(std::unique_ptr<TextFst> fst) {
    CTextFstLoadResult result{};
    if (fst->DataSize() >
            static_cast<size_t>(std::numeric_limits<int64_t>::max()) ||
        fst->TermCount() >
            static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
        throw std::runtime_error("loaded text FST metadata overflows int64");
    }
    result.data_size = static_cast<int64_t>(fst->DataSize());
    result.term_count = static_cast<int64_t>(fst->TermCount());
    result.is_memory_mapped = fst->IsMemoryMapped();
    result.handle = fst.release();
    result.status = milvus::SuccessCStatus();
    return result;
}

CTextFstLoadResult
TextFstLoadFailure(milvus::ErrorCode code,
                   const std::exception& e,
                   bool is_data_integrity_error = false) {
    CTextFstLoadResult result{};
    result.status = milvus::FailureCStatus(code, e.what());
    result.is_data_integrity_error = is_data_integrity_error;
    return result;
}

CTextFstLoadResult
TextFstUnknownFailure(const char* message) {
    CTextFstLoadResult result{};
    result.status = milvus::FailureCStatus(milvus::UnexpectedError, message);
    return result;
}

}  // namespace

CTextFstLoadResult
LoadTextFstBytes(const uint8_t* data, int64_t data_size) {
    try {
        if (data_size < 0 ||
            static_cast<uint64_t>(data_size) >
                static_cast<uint64_t>(std::numeric_limits<size_t>::max()) ||
            (data_size > 0 && data == nullptr)) {
            return TextFstUnknownFailure("invalid text FST byte buffer");
        }
        auto fst = std::make_unique<TextFst>();
        fst->LoadBytes(
            std::span<const uint8_t>(data, static_cast<size_t>(data_size)));
        return MakeTextFstLoadResult(std::move(fst));
    } catch (const std::bad_alloc& e) {
        return TextFstLoadFailure(milvus::MemAllocateFailed, e);
    } catch (const std::exception& e) {
        // Bytes have already crossed the object-storage boundary. Any format,
        // checksum or metadata failure below this point is persisted-data
        // corruption, not a transient loader failure.
        return TextFstLoadFailure(milvus::DataFormatBroken, e, true);
    } catch (...) {
        return TextFstUnknownFailure("unknown text FST byte load failure");
    }
}

CTextFstLoadResult
LoadTextFstFile(const char* path, bool memory_mapped) {
    try {
        if (path == nullptr || path[0] == '\0') {
            return TextFstUnknownFailure("text FST path is empty");
        }
        auto fst = std::make_unique<TextFst>();
        fst->LoadFile(path, memory_mapped);
        return MakeTextFstLoadResult(std::move(fst));
    } catch (const std::bad_alloc& e) {
        return TextFstLoadFailure(milvus::MemAllocateFailed, e);
    } catch (const std::system_error& e) {
        return TextFstLoadFailure(
            memory_mapped ? milvus::MmapError : milvus::FileReadFailed, e);
    } catch (const std::exception& e) {
        return TextFstLoadFailure(milvus::DataFormatBroken, e, true);
    } catch (...) {
        return TextFstUnknownFailure("unknown text FST file load failure");
    }
}

void
DeleteTextFst(CTextFstHandle handle) {
    delete static_cast<TextFst*>(handle);
}
