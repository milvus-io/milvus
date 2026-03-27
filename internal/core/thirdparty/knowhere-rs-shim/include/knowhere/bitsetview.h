#pragma once

#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>

namespace knowhere {

class BitsetView {
 public:
    BitsetView() = default;

    BitsetView(std::nullptr_t) {
    }

    BitsetView(const uint8_t* bits, size_t num_bits)
        : bits_(bits), num_bits_(num_bits) {
    }

    bool
    empty() const {
        return num_bits_ == 0;
    }

    size_t
    size() const {
        return num_bits_;
    }

    size_t
    byte_size() const {
        return (num_bits_ + 7) >> 3;
    }

    const uint8_t*
    data() const {
        return bits_;
    }

    bool
    test(int64_t index) const {
        return bits_[index >> 3] & (0x1 << (index & 0x7));
    }

    std::string
    to_string(size_t from, size_t to) const {
        if (empty()) {
            return {};
        }
        std::string value;
        value.reserve(to - from);
        to = std::min(to, num_bits_);
        for (size_t i = from; i < to; ++i) {
            value.push_back(test(i) ? '1' : '0');
        }
        return value;
    }

 private:
    const uint8_t* bits_ = nullptr;
    size_t num_bits_ = 0;
};

}  // namespace knowhere
