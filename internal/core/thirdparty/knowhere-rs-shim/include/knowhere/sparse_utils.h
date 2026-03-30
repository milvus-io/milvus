#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

namespace knowhere::sparse {

using label_t = int64_t;

template <typename T>
struct SparseRowElement {
    uint32_t id{};
    T val{};
};

template <typename T>
class SparseRow {
 public:
    using ValueType = T;
    using Element = SparseRowElement<T>;

    SparseRow() = default;

    explicit SparseRow(size_t size)
        : size_(size),
          owned_(size == 0 ? std::shared_ptr<Element[]>{}
                           : std::shared_ptr<Element[]>(new Element[size](),
                                                        std::default_delete<Element[]>())),
          data_(owned_.get()) {
    }

    SparseRow(size_t size, void* raw_data, bool own_data)
        : size_(size) {
        if (own_data) {
            owned_ = std::shared_ptr<Element[]>(
                static_cast<Element*>(raw_data), std::default_delete<Element[]>());
            data_ = owned_.get();
        } else {
            data_ = static_cast<Element*>(raw_data);
        }
    }

    void*
    data() {
        return data_;
    }

    const void*
    data() const {
        return data_;
    }

    size_t
    size() const {
        return size_;
    }

    bool
    empty() const {
        return size_ == 0;
    }

    size_t
    data_byte_size() const {
        return size_ * sizeof(Element);
    }

    int64_t
    dim() const {
        if (size_ == 0 || data_ == nullptr) {
            return 0;
        }
        return static_cast<int64_t>(data_[size_ - 1].id) + 1;
    }

    Element&
    operator[](size_t offset) {
        return data_[offset];
    }

    const Element&
    operator[](size_t offset) const {
        return data_[offset];
    }

    void
    set_at(size_t offset, uint32_t id, T value) {
        data_[offset].id = id;
        data_[offset].val = value;
    }

    T
    dot(const SparseRow& other) const {
        if (data_ == nullptr || other.data_ == nullptr || empty() || other.empty()) {
            return T{};
        }

        T score{};
        for (size_t left = 0; left < size_; ++left) {
            for (size_t right = 0; right < other.size_; ++right) {
                if (data_[left].id != other.data_[right].id) {
                    continue;
                }
                score += data_[left].val * other.data_[right].val;
            }
        }
        return score;
    }

    static constexpr size_t
    element_size() {
        return sizeof(Element);
    }

 private:
    size_t size_ = 0;
    std::shared_ptr<Element[]> owned_{};
    Element* data_ = nullptr;
};

}  // namespace knowhere::sparse
