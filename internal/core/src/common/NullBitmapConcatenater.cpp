#include "common/NullBitmapConcatenater.h"
#include <cstdint>
namespace milvus {
void
NullBitmapConcatenater::AddBitmap(uint8_t* bitmap, size_t size) {
    bitmaps_.emplace_back(bitmap, size);
}

void
NullBitmapConcatenater::ConcatenateBitmaps() {
    size_t total_size = 0;
    for (const auto& bitmap : bitmaps_) {
        total_size += bitmap.second;
    }
    concatenated_bitmap_.first = new uint8_t[(total_size + 7) / 8];
    concatenated_bitmap_.second = total_size;

    size_t offset = 0;
    for (const auto& bitmap : bitmaps_) {
        if (bitmap.first == nullptr) {
            // nullptr means nullable is false or no null value
            // set offset to offset + bitmap.second bits are 0
            offset += bitmap.second;
            continue;
        }
        // copy bitmap.second bits from bitmap.first to concatenated_bitmap_.first
        uint8_t dst_byte = offset / 8;
        uint8_t dst_offset = offset % 8;
        if (dst_offset) {
            // example:
            //  mask:                         | 1 1  0 0 0 0 0 0  |
            //                                +-----+-------------+
            //  bitmap                   ...  |     |             | ...
            //                                +-----+-------------+
            //                  |  dst_offset |
            //                  +-------------+-----+-------------+-----+
            // concatenated ... |             |     |             |     | ...
            //                  +-------------+-----+-------------+-----+
            //                  |     dst_byte      |   dst_byte + 1    |
            uint8_t mask = 0xff << dst_offset;
            auto src_byte_cnt =
                (bitmap.second + 7) / 8 -
                1;  // always treat the last byte as trailing bits
            auto trailing_bits = bitmap.second % 8 == 0 ? 8 : bitmap.second % 8;
            for (size_t i = 0; i < src_byte_cnt; i++, dst_byte++) {
                uint8_t data_byte = bitmap.first[i];
                concatenated_bitmap_.first[dst_byte] |=
                    (data_byte & mask) >> dst_offset;
                concatenated_bitmap_.first[dst_byte + 1] = (data_byte & ~mask)
                                                           << (8 - dst_offset);
            }
            // process trailing bits
            // trailing bits may can be wrapped by remaing bits
            auto masked_trailing =
                bitmap.first[src_byte_cnt] & (0xff << (8 - trailing_bits));
            concatenated_bitmap_.first[dst_byte] |=
                masked_trailing >> dst_offset;
            if (trailing_bits > 8 - dst_offset) {
                concatenated_bitmap_.first[dst_byte + 1] = masked_trailing
                                                           << (8 - dst_offset);
            }
        } else {
            auto src_byte_cnt = (bitmap.second + 7) / 8;
            auto trailing_bits = bitmap.second % 8 == 0 ? 8 : bitmap.second % 8;
            memcpy(concatenated_bitmap_.first + dst_byte,
                   bitmap.first,
                   src_byte_cnt);
            concatenated_bitmap_.first[dst_byte + src_byte_cnt - 1] &=
                (0xff << (8 - trailing_bits));
        }
        offset += bitmap.second;
    }
}
}  // namespace milvus