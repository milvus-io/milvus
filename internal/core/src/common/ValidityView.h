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

#include <cstddef>
#include <cstdint>

namespace milvus {

// Non-owning view over either one-byte-per-row bool data or an LSB-first
// packed bitmap. The underlying buffer must outlive the view.
class ValidityView {
 public:
    ValidityView() = default;
    ValidityView(std::nullptr_t) {
    }

    static ValidityView
    FromExpanded(const bool* data) {
        return ValidityView(data, Encoding::Expanded, 0);
    }

    static ValidityView
    FromPacked(const uint8_t* data) {
        return ValidityView(data, Encoding::Packed, 0);
    }

    explicit operator bool() const {
        return data_ != nullptr;
    }

    bool
    operator[](int64_t index) const {
        const auto offset = offset_ + index;
        if (encoding_ == Encoding::Expanded) {
            return static_cast<const bool*>(data_)[offset];
        }
        return (static_cast<const uint8_t*>(data_)[offset >> 3] >>
                (offset & 0x07)) &
               1;
    }

    bool
    is_packed() const {
        return encoding_ == Encoding::Packed;
    }

    ValidityView
    Subview(int64_t offset) const {
        auto result = *this;
        result.offset_ += offset;
        return result;
    }

    const bool*
    expanded_data() const {
        if (encoding_ != Encoding::Expanded) {
            return nullptr;
        }
        return static_cast<const bool*>(data_) + offset_;
    }

    const uint8_t*
    packed_data() const {
        if (encoding_ != Encoding::Packed) {
            return nullptr;
        }
        return static_cast<const uint8_t*>(data_);
    }

    int64_t
    bit_offset() const {
        return offset_;
    }

 private:
    enum class Encoding : uint8_t { None, Expanded, Packed };

    ValidityView(const void* data, Encoding encoding, int64_t offset)
        : data_(data),
          encoding_(data == nullptr ? Encoding::None : encoding),
          offset_(offset) {
    }

    const void* data_{nullptr};
    Encoding encoding_{Encoding::None};
    int64_t offset_{0};
};

}  // namespace milvus
