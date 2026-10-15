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

// ---- Roaring V2 zero-copy encode ----

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

    size_t ser_size = roaring_bitmap_size_in_bytes(r.get());
    std::vector<char> buf(ser_size);
    roaring_bitmap_serialize(r.get(), buf.data());
    return buf;
}

// ---- Roaring decode ----

bool
CacheCompressor::DecompressRoaring(const char* data,
                                   uint32_t data_len,
                                   uint32_t num_bits,
                                   TargetBitmap& out) {
    using namespace roaring::internal;

    std::unique_ptr<roaring_bitmap_t, decltype(&roaring_bitmap_free)> r(
        roaring_bitmap_deserialize_safe(data, data_len), roaring_bitmap_free);
    if (!r) {
        LOG_WARN("CacheCompressor::DecompressRoaring: deserialize failed");
        return false;
    }

    TargetBitmap result(num_bits, false);
    auto* words = reinterpret_cast<uint64_t*>(result.data());
    const auto& containers = r->high_low_container;
    // Write containers directly into the output. Expanding all set positions
    // to uint32_t would require up to 32 times the bitmap's memory as scratch.
    for (int32_t i = 0; i < containers.size; ++i) {
        const size_t bit_start = size_t{containers.keys[i]} << 16;
        if (bit_start >= num_bits) {
            continue;
        }
        const uint32_t bits = std::min<size_t>(65536, num_bits - bit_start);
        auto* dst = words + bit_start / 64;
        uint8_t type = containers.typecodes[i];
        const auto* container =
            container_unwrap_shared(containers.containers[i], &type);
        switch (type) {
            case BITSET_CONTAINER_TYPE: {
                const auto* src = const_CAST_bitset(container);
                std::memcpy(dst, src->words, ((bits + 63) / 64) * 8);
                break;
            }
            case ARRAY_CONTAINER_TYPE: {
                const auto* src = const_CAST_array(container);
                for (int32_t j = 0; j < src->cardinality; ++j) {
                    const uint32_t pos = src->array[j];
                    if (pos < bits) {
                        dst[pos / 64] |= uint64_t{1} << (pos % 64);
                    }
                }
                break;
            }
            case RUN_CONTAINER_TYPE: {
                const auto* src = const_CAST_run(container);
                for (int32_t j = 0; j < src->n_runs; ++j) {
                    const auto& run = src->runs[j];
                    if (run.value < bits) {
                        bitset_set_lenrange(
                            dst,
                            run.value,
                            std::min<uint32_t>(run.length,
                                               bits - run.value - 1));
                    }
                }
                break;
            }
            default:
                return false;
        }
    }
    if (num_bits % 64 != 0) {
        words[num_bits / 64] &= (uint64_t{1} << (num_bits % 64)) - 1;
    }
    out = std::move(result);
    return true;
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

bool
CacheCompressor::DecompressBitmap(const char* data,
                                  uint32_t data_len,
                                  uint32_t num_bits,
                                  uint8_t comp_type,
                                  TargetBitmap& out) {
    if (comp_type == kCompTypeRaw) {
        const size_t bytes = ((size_t{num_bits} + 63) / 64) * 8;
        if (data_len != bytes) {
            return false;
        }
        TargetBitmap result(num_bits, false);
        if (bytes > 0) {
            std::memcpy(result.data(), data, bytes);
        }
        out = std::move(result);
        return true;
    }
    if (comp_type != kCompTypeRoaring && comp_type != kCompTypeRoaringInv) {
        return false;
    }
    if (!DecompressRoaring(data, data_len, num_bits, out)) {
        return false;
    }
    if (comp_type == kCompTypeRoaringInv) {
        out.flip();
    }
    return true;
}

bool
CacheCompressor::Decompress(const char* data,
                            uint32_t data_len,
                            uint8_t comp_type,
                            TargetBitmap& out_result,
                            TargetBitmap& out_valid) {
    if (data_len < kHeaderSize) {
        LOG_WARN("CacheCompressor::Decompress: data_len ({}) < header size",
                 data_len);
        return false;
    }

    uint32_t result_bits = 0;
    uint32_t valid_bits_raw = 0;
    std::memcpy(&result_bits, data, 4);
    std::memcpy(&valid_bits_raw, data + 4, 4);

    bool valid_all_ones = (valid_bits_raw & kValidAllOnesMask) != 0;
    uint32_t valid_bits = valid_bits_raw & ~kValidAllOnesMask;

    const char* payload = data + kHeaderSize;
    const uint32_t payload_len = data_len - kHeaderSize;

    if (comp_type == kCompTypeIndependent) {
        if (payload_len < kEncodingHeaderSize) {
            return false;
        }
        const auto result_type = static_cast<uint8_t>(payload[0]);
        const auto valid_type = static_cast<uint8_t>(payload[1]);
        auto valid_encoding = [](uint8_t encoding) {
            return encoding == kCompTypeRaw || encoding == kCompTypeRoaring ||
                   encoding == kCompTypeRoaringInv;
        };
        if (!valid_encoding(result_type) || !valid_encoding(valid_type)) {
            return false;
        }
        uint32_t result_size = 0;
        std::memcpy(&result_size, payload + 2, sizeof(result_size));
        if (result_size > payload_len - kEncodingHeaderSize) {
            return false;
        }
        const uint32_t valid_size =
            payload_len - kEncodingHeaderSize - result_size;
        if (valid_all_ones && (valid_type != kCompTypeRaw || valid_size != 0)) {
            return false;
        }
        const char* result_data = payload + kEncodingHeaderSize;
        if (!DecompressBitmap(result_data,
                              result_size,
                              result_bits,
                              result_type,
                              out_result)) {
            return false;
        }
        if (valid_all_ones) {
            out_valid = TargetBitmap(valid_bits, true);
            return true;
        }
        return DecompressBitmap(result_data + result_size,
                                valid_size,
                                valid_bits,
                                valid_type,
                                out_valid);
    }

    // --- Raw path ---
    if (comp_type != kCompTypeRaw) {
        LOG_WARN("CacheCompressor::Decompress: unknown comp_type={}",
                 comp_type);
        return false;
    }

    auto bits_to_bytes = [](uint32_t bits) -> uint32_t {
        return static_cast<uint32_t>(((bits + 63) / 64) * 8);
    };
    const uint32_t result_bytes = bits_to_bytes(result_bits);
    const uint32_t valid_bytes = valid_all_ones ? 0 : bits_to_bytes(valid_bits);
    const uint32_t raw_total = result_bytes + valid_bytes;

    if (payload_len < raw_total) {
        LOG_WARN(
            "CacheCompressor::Decompress: raw payload too short "
            "(payload_len={}, expected={})",
            payload_len,
            raw_total);
        return false;
    }
    const char* raw = payload;

    out_result = TargetBitmap(result_bits, false);
    if (result_bytes > 0) {
        std::memcpy(
            reinterpret_cast<char*>(out_result.data()), raw, result_bytes);
    }

    if (valid_all_ones) {
        // Construct minimal bitmap and set all bits.
        // Note: TargetBitmap(n, true) does one write pass (init to all-1s),
        // vs TargetBitmap(n) + set() which does two (zero-init then set).
        out_valid = TargetBitmap(valid_bits, true);
    } else {
        out_valid = TargetBitmap(valid_bits, false);
        if (valid_bytes > 0) {
            std::memcpy(reinterpret_cast<char*>(out_valid.data()),
                        raw + result_bytes,
                        valid_bytes);
        }
    }
    return true;
}

}  // namespace exec
}  // namespace milvus
