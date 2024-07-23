#pragma once
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <utility>
#include <vector>
namespace milvus {

class NullBitmapConcatenater {
 public:
    NullBitmapConcatenater() = default;

    void
    AddBitmap(uint8_t* bitmap, size_t size);

    // Do not call this function multiple times
    void
    ConcatenateBitmaps();

    void
    Clear() {
        bitmaps_.clear();
        if (concatenated_bitmap_.first) {
            delete[] concatenated_bitmap_.first;
            concatenated_bitmap_.first = nullptr;
        }
    }

    std::pair<uint8_t*, size_t>
    GetConcatenatedBitmap() {
        return concatenated_bitmap_;
    }

    ~NullBitmapConcatenater() {
        if (concatenated_bitmap_.first) {
            delete[] concatenated_bitmap_.first;
        }
    }

 private:
    std::vector<std::pair<uint8_t*, size_t>> bitmaps_;
    std::pair<uint8_t*, size_t> concatenated_bitmap_;
};
}  // namespace milvus