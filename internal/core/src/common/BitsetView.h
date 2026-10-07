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

#include <fmt/core.h>

#include <boost_ext/dynamic_bitset_ext.hpp>
#include <deque>
#include <optional>

#include "bitset/detail/element_wise.h"
#include "common/Types.h"
#include "common/EasyAssert.h"
#include "knowhere/bitsetview.h"

namespace milvus {

class BitsetView : public knowhere::BitsetView {
 public:
    BitsetView() = default;
    ~BitsetView() = default;

    BitsetView(const std::nullptr_t value)  // NOLINT
        : knowhere::BitsetView(value) {     // NOLINT
    }

    BitsetView(const uint8_t* data, size_t num_bits)
        : knowhere::BitsetView(data, num_bits) {  // NOLINT
    }

    BitsetView(const BitsetTypeView& bitset)  // NOLINT
        : knowhere::BitsetView(
              bitset.empty() || bitset.offset() == 0
                  ? reinterpret_cast<const uint8_t*>(bitset.data())
                  : reinterpret_cast<const uint8_t*>(bitset.data()) +
                        bitset.offset() / 8,
              bitset.size()),
          dense_source_(bitset) {
        AssertInfo(bitset.empty() || (bitset.offset() & 7) == 0,
                   "search bitset offset {} must be byte aligned",
                   bitset.offset());
    }

    BitsetView(const BitsetTypePtr& bitset_ptr) {  // NOLINT
        if (bitset_ptr) {
            *this = BitsetView(*bitset_ptr);
        }
    }

    // Predicates describe test(id) over the backend id domain. In particular,
    // Knowhere empty() can mean a nonempty all-allowed bitmap.
    bool
    all() const {
        if (size() == 0)
            return true;
        if (!has_out_ids() && id_offset() == 0 && size() == num_bits()) {
            if (dense_source_)
                return dense_source_->all();
            return bitset::detail::ElementWiseBitsetPolicy<uint8_t>::op_all(
                data(), 0, size());
        }
        for (size_t i = 0; i < size(); ++i)
            if (!test(i))
                return false;
        return true;
    }

    bool
    none() const {
        if (size() == 0)
            return true;
        if (!has_out_ids() && id_offset() == 0 && size() == num_bits()) {
            if (dense_source_)
                return dense_source_->none();
            return bitset::detail::ElementWiseBitsetPolicy<uint8_t>::op_none(
                data(), 0, size());
        }
        for (size_t i = 0; i < size(); ++i)
            if (test(i))
                return false;
        return true;
    }

    BitsetView
    subview(size_t offset, size_t length) const {
        AssertInfo(offset <= size() && length <= size() - offset,
                   "index out of range, offset={}, size={}, bitset.size={}",
                   offset,
                   length,
                   size());
        if (num_bits() == 0)
            return {};
        if (has_out_ids() || id_offset() != 0 || size() != num_bits()) {
            // Preserve the public bitmap and translate the backend window.
            // A prepared count from the parent is not the window's count.
            BitsetView result(data(), num_bits());
            result.dense_source_ = dense_source_;
            if (has_out_ids())
                result.set_out_ids(get_out_ids(), out_ids_count());
            result.set_id_offset(id_offset() + offset);
            result.set_vector_count(length);
            return result;
        }

        AssertInfo(
            (offset & 7) == 0, "offset {} is not divisible by 8", offset);
        if (dense_source_)
            return BitsetView(dense_source_->view(offset, length));
        return {data() + (offset >> 3), length};
    }

 private:
    std::optional<BitsetTypeView> dense_source_;
};

}  // namespace milvus
