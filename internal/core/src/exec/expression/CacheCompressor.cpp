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

#include "exec/expression/CacheCompressor.h"

#include <algorithm>
#include <bit>
#include <cstring>
#include <memory>
#include <vector>

#include <roaring/roaring.h>
#include <roaring/roaring.hh>
#include <roaring/bitset_util.h>
#include <roaring/containers/bitset.h>
#include <roaring/containers/containers.h>
#include <roaring/roaring_array.h>

#include "log/Log.h"

namespace milvus {
namespace exec {

// ---- Payload header ----
// [result_bit_count (4B)] [valid_bit_count (4B)] [payload...]
// valid_bit_count high bit = kValidAllOnesMask means valid is all-ones (not in payload).
//
// Raw: [raw result][raw valid, unless all-ones].
// Independent: [result codec (1B)][valid codec (1B)][result size (4B)]
//              [encoded result][encoded valid, unless all-ones].

constexpr size_t kHeaderSize = sizeof(uint32_t) * 2;
constexpr size_t kEncodingHeaderSize = 2 + sizeof(uint32_t);

// Density thresholds for auto-selection
constexpr double kRoaringDensityMax = 0.03;     // <= 3%  → Roaring
constexpr double kRoaringInvDensityMin = 0.97;  // >= 97% → invert + Roaring

// ---- Portable Roaring encode ----

std::vector<char>
CacheCompressor::CompressRoaring(const TargetBitmap& bset, bool inverted) {
    using namespace roaring::internal;

    const uint64_t* words = reinterpret_cast<const uint64_t*>(bset.data());
    size_t total_bits = bset.size();
    size_t total_words = bset.size_in_bytes() / 8;

    // Invert while encoding instead of allocating another full bitmap. The
    // final word may contain padding bits, which must never enter Roaring.
    auto load_word = [&](size_t w) {
        uint64_t word = inverted ? ~words[w] : words[w];
        if (w + 1 == total_words && total_bits % 64 != 0) {
            word &= (uint64_t{1} << (total_bits % 64)) - 1;
        }
        return word;
    };

    size_t num_containers = (total_bits + 65535) / 65536;
    constexpr size_t WORDS_PER_CONTAINER = 1024;
    constexpr int32_t ARRAY_THRESHOLD = 4096;

    std::unique_ptr<roaring_bitmap_t, decltype(&roaring_bitmap_free)> r(
        roaring_bitmap_create_with_capacity(
            static_cast<uint32_t>(num_containers)),
        roaring_bitmap_free);

    for (size_t c = 0; c < num_containers; ++c) {
        uint16_t key = static_cast<uint16_t>(c);
        size_t word_start = c * WORDS_PER_CONTAINER;
        size_t word_end =
            std::min(word_start + WORDS_PER_CONTAINER, total_words);
        size_t chunk_words = word_end - word_start;

        int32_t popcount = 0;
        for (size_t w = word_start; w < word_end; ++w) {
            popcount += __builtin_popcountll(load_word(w));
        }

        if (popcount == 0) {
            continue;
        }

        // Roaring infers array versus bitset from cardinality when reading:
        // exactly 4096 positions must use an array container.
        if (popcount > ARRAY_THRESHOLD) {
            bitset_container_t* bc = bitset_container_create();
            for (size_t w = word_start; w < word_end; ++w) {
                bc->words[w - word_start] = load_word(w);
            }
            if (chunk_words < WORDS_PER_CONTAINER) {
                std::memset(bc->words + chunk_words,
                            0,
                            (WORDS_PER_CONTAINER - chunk_words) * 8);
            }
            bc->cardinality = popcount;
            ra_append(&r->high_low_container,
                      key,
                      static_cast<container_t*>(bc),
                      BITSET_CONTAINER_TYPE);
        } else {
            array_container_t* ac =
                array_container_create_given_capacity(popcount);
            for (size_t w = word_start; w < word_end; ++w) {
                uint64_t word = load_word(w);
                while (word != 0) {
                    ac->array[ac->cardinality++] = static_cast<uint16_t>(
                        (w - word_start) * 64 + __builtin_ctzll(word));
                    word &= word - 1;
                }
            }
            ra_append(&r->high_low_container,
                      key,
                      static_cast<container_t*>(ac),
                      ARRAY_CONTAINER_TYPE);
        }
    }

    // runOptimize: convert bitmap/array → run containers where smaller
    roaring_bitmap_run_optimize(r.get());

    size_t ser_size = roaring_bitmap_portable_size_in_bytes(r.get());
    std::vector<char> buf(ser_size);
    roaring_bitmap_portable_serialize(r.get(), buf.data());
    return buf;
}

// ---- Public API ----

CompressedData
CacheCompressor::Compress(const TargetBitmap& result,
                          const TargetBitmap& valid,
                          bool compression_enabled) {
    CompressedData out;
    const uint32_t result_bits = static_cast<uint32_t>(result.size());
    const uint32_t valid_bits = static_cast<uint32_t>(valid.size());

    // Detect valid all-ones: skip storing valid bytes if all set.
    // Uses all() which does word-level comparison with short-circuit (~5μs),
    // much faster than count() == size() which does full popcount (~15μs).
    bool valid_all_ones = valid.all();
    const uint32_t valid_bits_header =
        valid_all_ones ? (valid_bits | kValidAllOnesMask) : valid_bits;

    // Pre-fill header (used by all paths)
    std::memcpy(out.header, &result_bits, 4);
    std::memcpy(out.header + 4, &valid_bits_header, 4);

    auto select_encoding = [compression_enabled](const TargetBitmap& bitmap) {
        if (!compression_enabled || bitmap.size() == 0) {
            return kCompTypeRaw;
        }
        const double density =
            static_cast<double>(bitmap.count()) / bitmap.size();
        if (density <= kRoaringDensityMax) {
            return kCompTypeRoaring;
        }
        if (density >= kRoaringInvDensityMin) {
            return kCompTypeRoaringInv;
        }
        return kCompTypeRaw;
    };
    out.result_comp_type = select_encoding(result);
    out.valid_comp_type =
        valid_all_ones ? kCompTypeRaw : select_encoding(valid);
    out.comp_type = out.result_comp_type == kCompTypeRaw &&
                            out.valid_comp_type == kCompTypeRaw
                        ? kCompTypeRaw
                        : kCompTypeIndependent;

    if (out.result_comp_type == kCompTypeRaw) {
        out.raw_result_ptr = reinterpret_cast<const char*>(result.data());
        out.raw_result_size = result.size_in_bytes();
    } else {
        out.result_payload = CompressRoaring(
            result, out.result_comp_type == kCompTypeRoaringInv);
    }
    if (!valid_all_ones) {
        if (out.valid_comp_type == kCompTypeRaw) {
            out.raw_valid_ptr = reinterpret_cast<const char*>(valid.data());
            out.raw_valid_size = valid.size_in_bytes();
        } else {
            out.valid_payload = CompressRoaring(
                valid, out.valid_comp_type == kCompTypeRoaringInv);
        }
    }
    return out;
}

// Flatten both bitmap representations without copying Raw bytes into scratch.
std::vector<char>
CacheCompressor::Compress(const TargetBitmap& result,
                          const TargetBitmap& valid,
                          bool compression_enabled,
                          uint8_t& out_comp_type) {
    auto cd = Compress(result, valid, compression_enabled);
    out_comp_type = cd.comp_type;
    std::vector<char> buf(cd.total_size());
    std::memcpy(buf.data(), cd.header, 8);
    char* p = buf.data() + kHeaderSize;
    if (cd.comp_type == kCompTypeIndependent) {
        p[0] = static_cast<char>(cd.result_comp_type);
        p[1] = static_cast<char>(cd.valid_comp_type);
        const auto result_size = static_cast<uint32_t>(cd.result_size());
        std::memcpy(p + 2, &result_size, sizeof(result_size));
        p += kEncodingHeaderSize;
    }
    if (cd.result_size() > 0) {
        std::memcpy(p, cd.result_data(), cd.result_size());
        p += cd.result_size();
    }
    if (cd.valid_size() > 0) {
        std::memcpy(p, cd.valid_data(), cd.valid_size());
    }
    return buf;
}

namespace {
// Portable Roaring is little-endian. memcpy avoids alignment assumptions for
// containers embedded after the cache's 14-byte header or another container.
template <typename T>
T
ReadLE(const char* bytes) {
    T value;
    if constexpr (std::endian::native == std::endian::little) {
        std::memcpy(&value, bytes, sizeof(T));
    } else {
        value = 0;
        for (size_t i = 0; i < sizeof(T); ++i) {
            value |= T{static_cast<uint8_t>(bytes[i])} << (8 * i);
        }
    }
    return value;
}
}  // namespace

bool
CacheCompressor::ReadView::BitmapView::Init(const char* data,
                                            size_t size,
                                            uint8_t codec,
                                            uint32_t rows) {
    encoding = codec;
    if (codec == kCompTypeRaw) {
        if (size != ((size_t{rows} + 63) / 64) * 8) {
            return false;
        }
        raw = data;
        return true;
    }
    if ((codec != kCompTypeRoaring && codec != kCompTypeRoaringInv) ||
        size < 4) {
        return false;
    }
    // https://github.com/RoaringBitmap/RoaringFormatSpec
    constexpr uint32_t no_run_cookie = 12346;
    constexpr uint16_t run_cookie = 12347;
    const auto cookie = ReadLE<uint32_t>(data);
    const bool has_runs = (cookie & 0xffff) == run_cookie;
    size_t cursor = 4;
    uint32_t count;
    const char* run_flags = nullptr;
    if (has_runs) {
        count = (cookie >> 16) + 1;
        const size_t flags_size = (count + 7) / 8;
        if (flags_size > size - cursor) {
            return false;
        }
        run_flags = data + cursor;
        cursor += flags_size;
    } else {
        if (cookie != no_run_cookie || size < 8) {
            return false;
        }
        count = ReadLE<uint32_t>(data + cursor);
        cursor += 4;
    }
    if (count > 65536 || count > (uint64_t{rows} + 65535) / 65536 ||
        size_t{count} * 4 > size - cursor) {
        return false;
    }
    const char* descriptors = data + cursor;
    cursor += size_t{count} * 4;
    if (!has_runs || count >= 4) {
        // Like CRoaring's safe reader, derive locations from the container
        // lengths, not untrusted offsets in the redundant offset table.
        if (size_t{count} * 4 > size - cursor) {
            return false;
        }
        cursor += size_t{count} * 4;
    }
    containers.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        const auto key = ReadLE<uint16_t>(descriptors + 4 * i);
        const uint32_t cardinality =
            ReadLE<uint16_t>(descriptors + 4 * i + 2) + 1;
        const uint64_t base = uint64_t{key} << 16;
        if (base >= rows ||
            (!containers.empty() && key <= containers.back().key)) {
            return false;
        }
        const uint32_t bits = std::min<uint64_t>(65536, rows - base);
        const bool run = has_runs && (static_cast<uint8_t>(run_flags[i / 8]) &
                                      (1u << (i % 8)));
        Container container{
            data + cursor, cardinality, key, ContainerType::Array};
        if (run) {
            if (size - cursor < 2) {
                return false;
            }
            const uint32_t runs = ReadLE<uint16_t>(data + cursor);
            cursor += 2;
            if (runs == 0 || size_t{runs} * 4 > size - cursor) {
                return false;
            }
            container = {data + cursor, runs, key, ContainerType::Run};
            uint32_t actual = 0, previous_end = 0;
            for (uint32_t j = 0; j < runs; ++j) {
                const uint32_t start = ReadLE<uint16_t>(data + cursor + j * 4);
                const uint32_t length =
                    ReadLE<uint16_t>(data + cursor + j * 4 + 2);
                const uint32_t end = start + length;
                if (end >= bits || (j != 0 && start <= previous_end)) {
                    return false;
                }
                actual += length + 1;
                previous_end = end;
            }
            if (actual != cardinality) {
                return false;
            }
            cursor += size_t{runs} * 4;
        } else if (cardinality > 4096) {
            if (size - cursor < 8192) {
                return false;
            }
            container.type = ContainerType::Bitset;
            uint32_t actual = 0;
            for (uint32_t word = 0; word < 1024; ++word) {
                const auto value = ReadLE<uint64_t>(data + cursor + word * 8);
                actual += std::popcount(value);
                if (value != 0 && (word * 64 >= bits ||
                                   (bits - word * 64 < 64 &&
                                    (value >> (bits - word * 64)) != 0))) {
                    return false;
                }
            }
            if (actual != cardinality) {
                return false;
            }
            cursor += 8192;
        } else {
            if (size_t{cardinality} * 2 > size - cursor) {
                return false;
            }
            uint32_t previous = 0;
            for (uint32_t j = 0; j < cardinality; ++j) {
                const uint32_t value = ReadLE<uint16_t>(data + cursor + j * 2);
                if (value >= bits || (j != 0 && value <= previous)) {
                    return false;
                }
                previous = value;
            }
            cursor += size_t{cardinality} * 2;
        }
        containers.push_back(container);
    }
    return cursor == size;
}

TargetBitmap
CacheCompressor::ReadView::BitmapView::Decode(uint32_t rows) const {
    TargetBitmap result(rows, false);
    if (encoding == kCompTypeRaw) {
        if (rows != 0) {
            std::memcpy(result.data(), raw, result.size_in_bytes());
        }
        return result;
    }
    auto* words = reinterpret_cast<uint64_t*>(result.data());
    for (const auto& container : containers) {
        const size_t base = size_t{container.key} << 16;
        if (container.type == ContainerType::Array) {
            for (uint32_t j = 0; j < container.count; ++j) {
                result[base + ReadLE<uint16_t>(container.data + j * 2)] = true;
            }
        } else if (container.type == ContainerType::Run) {
            for (uint32_t j = 0; j < container.count; ++j) {
                roaring::internal::bitset_set_lenrange(
                    words + base / 64,
                    ReadLE<uint16_t>(container.data + j * 4),
                    ReadLE<uint16_t>(container.data + j * 4 + 2));
            }
        } else {
            const size_t count =
                (std::min<size_t>(65536, rows - base) + 63) / 64;
            for (size_t j = 0; j < count; ++j) {
                words[base / 64 + j] = ReadLE<uint64_t>(container.data + j * 8);
            }
        }
    }
    if (encoding == kCompTypeRoaringInv) {
        result.flip();
    }
    return result;
}

CacheCompressor::ReadView::ReadView(const char* data,
                                    size_t size,
                                    uint8_t encoding,
                                    int64_t rows) {
    valid_ = Init(data, size, encoding, rows);
}

bool
CacheCompressor::ReadView::Init(const char* data,
                                size_t size,
                                uint8_t encoding,
                                int64_t rows) {
    if (size < kHeaderSize || rows < 0) {
        return false;
    }
    uint32_t result_bits, valid_header;
    std::memcpy(&result_bits, data, 4);
    std::memcpy(&valid_header, data + 4, 4);
    if (result_bits != rows || (valid_header & ~kValidAllOnesMask) != rows) {
        return false;
    }
    rows_ = result_bits;
    all_valid_ = valid_header & kValidAllOnesMask;
    data += kHeaderSize;
    size -= kHeaderSize;
    uint8_t result_type = kCompTypeRaw, valid_type = kCompTypeRaw;
    size_t result_size = ((size_t{rows_} + 63) / 64) * 8;
    if (encoding == kCompTypeIndependent) {
        if (size < kEncodingHeaderSize) {
            return false;
        }
        result_type = static_cast<uint8_t>(data[0]);
        valid_type = static_cast<uint8_t>(data[1]);
        uint32_t length;
        std::memcpy(&length, data + 2, 4);
        result_size = length;
        data += kEncodingHeaderSize;
        size -= kEncodingHeaderSize;
    } else if (encoding != kCompTypeRaw) {
        return false;
    }
    if (result_size > size ||
        (all_valid_ && (valid_type != kCompTypeRaw || size != result_size))) {
        return false;
    }
    if (!result_.Init(data, result_size, result_type, rows_)) {
        return false;
    }
    return all_valid_ ||
           validity_.Init(
               data + result_size, size - result_size, valid_type, rows_);
}

size_t
CacheCompressor::ReadView::HeapBytes() const {
    return (result_.containers.capacity() + validity_.containers.capacity()) *
           sizeof(Container);
}

bool
CacheCompressor::ReadView::Decode(TargetBitmap& result,
                                  TargetBitmap& valid) const {
    if (!valid_) {
        return false;
    }
    auto decoded = result_.Decode(rows_);
    auto decoded_valid =
        all_valid_ ? TargetBitmap(rows_, true) : validity_.Decode(rows_);
    result = std::move(decoded);
    valid = std::move(decoded_valid);
    return true;
}

bool
CacheCompressor::Decompress(const char* data,
                            uint32_t data_len,
                            uint8_t comp_type,
                            TargetBitmap& out_result,
                            TargetBitmap& out_valid) {
    if (data_len < kHeaderSize) {
        return false;
    }
    uint32_t rows;
    std::memcpy(&rows, data, 4);
    return ReadView(data, data_len, comp_type, rows)
        .Decode(out_result, out_valid);
}

}  // namespace exec
}  // namespace milvus
