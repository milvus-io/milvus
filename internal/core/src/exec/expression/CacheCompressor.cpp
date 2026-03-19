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
#include <vector>

#include <roaring/roaring.h>
#include <roaring/roaring.hh>
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
// For LZ4/Raw: payload = LZ4-compressed or raw bytes of result (+ valid if not all-ones).
// For Roaring/RoaringInv: payload = [result_roaring_size (4B)][result_roaring][valid_roaring]
//   (valid_roaring is absent if valid is all-ones)

constexpr size_t kHeaderSize = sizeof(uint32_t) * 2;

// Density thresholds for auto-selection
constexpr double kRoaringDensityMax = 0.03;     // <= 3%  → Roaring
constexpr double kRoaringInvDensityMin = 0.97;  // >= 97% → invert + Roaring
constexpr double kRawDensityMin = 0.30;         // 30%-70% → Raw
constexpr double kRawDensityMax = 0.70;

// ---- Roaring V2 zero-copy encode ----

std::vector<char>
CacheCompressor::CompressRoaring(const TargetBitmap& bset) {
    using namespace roaring::internal;

    const uint64_t* words = reinterpret_cast<const uint64_t*>(bset.data());
    size_t total_bits = bset.size();
    size_t total_words = bset.size_in_bytes() / 8;

    size_t num_containers = (total_bits + 65535) / 65536;
    constexpr size_t WORDS_PER_CONTAINER = 1024;
    constexpr int32_t ARRAY_THRESHOLD = 4096;

    roaring_bitmap_t* r = roaring_bitmap_create_with_capacity(
        static_cast<uint32_t>(num_containers));

    for (size_t c = 0; c < num_containers; ++c) {
        uint16_t key = static_cast<uint16_t>(c);
        size_t word_start = c * WORDS_PER_CONTAINER;
        size_t word_end =
            std::min(word_start + WORDS_PER_CONTAINER, total_words);
        size_t chunk_words = word_end - word_start;

        int32_t popcount = 0;
        for (size_t w = word_start; w < word_end; ++w) {
            popcount += __builtin_popcountll(words[w]);
        }

        if (popcount == 0) {
            continue;
        }

        if (popcount >= ARRAY_THRESHOLD) {
            bitset_container_t* bc = bitset_container_create();
            std::memcpy(bc->words, words + word_start, chunk_words * 8);
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
                uint64_t word = words[w];
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
    roaring_bitmap_run_optimize(r);

    size_t ser_size = roaring_bitmap_size_in_bytes(r);
    std::vector<char> buf(ser_size);
    roaring_bitmap_serialize(r, buf.data());
    roaring_bitmap_free(r);
    return buf;
}

// ---- Roaring decode ----

TargetBitmap
CacheCompressor::DecompressRoaring(const char* data,
                                   uint32_t data_len,
                                   uint32_t num_bits) {
    roaring_bitmap_t* r = roaring_bitmap_deserialize_safe(data, data_len);
    if (!r) {
        LOG_WARN("CacheCompressor::DecompressRoaring: deserialize failed");
        return TargetBitmap(num_bits, false);
    }

    TargetBitmap result(num_bits, false);
    uint64_t card = roaring_bitmap_get_cardinality(r);
    if (card > 0) {
        // Extract all set-bit positions and set them in dense bitset
        std::vector<uint32_t> positions(card);
        roaring_bitmap_to_uint32_array(r, positions.data());

        uint64_t* words = reinterpret_cast<uint64_t*>(result.data());
        for (uint32_t pos : positions) {
            if (pos < num_bits) {
                words[pos / 64] |= (uint64_t{1} << (pos % 64));
            }
        }
    }
    roaring_bitmap_free(r);
    return result;
}

// ---- Public API ----

CompressedData
CacheCompressor::Compress(const TargetBitmap& result,
                          const TargetBitmap& valid,
                          bool compression_enabled) {
    CompressedData out;
    const uint32_t result_bits = static_cast<uint32_t>(result.size());
    const uint32_t valid_bits = static_cast<uint32_t>(valid.size());
    const uint32_t result_bytes = static_cast<uint32_t>(result.size_in_bytes());

    // Single popcount for result (used for density-based compression selection)
    size_t result_count =
        (compression_enabled && result.size() > 0) ? result.count() : 0;

    // Detect valid all-ones: skip storing valid bytes if all set.
    // Uses all() which does word-level comparison with short-circuit (~5μs),
    // much faster than count() == size() which does full popcount (~15μs).
    bool valid_all_ones = valid.all();
    const uint32_t valid_bytes =
        valid_all_ones ? 0 : static_cast<uint32_t>(valid.size_in_bytes());
    const uint32_t valid_bits_header =
        valid_all_ones ? (valid_bits | kValidAllOnesMask) : valid_bits;

    // Pre-fill header (used by all paths)
    std::memcpy(out.header, &result_bits, 4);
    std::memcpy(out.header + 4, &valid_bits_header, 4);

    // Auto-select compression based on result density
    if (compression_enabled && result.size() > 0) {
        double density = static_cast<double>(result_count) / result.size();

        // --- Roaring path (sparse ≤3%) ---
        if (density <= kRoaringDensityMax) {
            out.comp_type = kCompTypeRoaring;
            auto result_roaring = CompressRoaring(result);
            uint32_t rr_sz = static_cast<uint32_t>(result_roaring.size());

            std::vector<char> valid_roaring;
            uint32_t vr_sz = 0;
            if (!valid_all_ones && valid.size() > 0) {
                valid_roaring = CompressRoaring(valid);
                vr_sz = static_cast<uint32_t>(valid_roaring.size());
            }
            out.payload.resize(4 + rr_sz + vr_sz);
            std::memcpy(out.payload.data(), &rr_sz, 4);
            std::memcpy(out.payload.data() + 4, result_roaring.data(), rr_sz);
            if (vr_sz > 0) {
                std::memcpy(out.payload.data() + 4 + rr_sz,
                            valid_roaring.data(),
                            vr_sz);
            }
            return out;
        }

        // --- Inverted Roaring path (dense ≥97%) ---
        if (density >= kRoaringInvDensityMin) {
            out.comp_type = kCompTypeRoaringInv;
            TargetBitmap inverted(result.size());
            inverted.set();
            inverted -= result;
            auto result_roaring = CompressRoaring(inverted);
            uint32_t rr_sz = static_cast<uint32_t>(result_roaring.size());

            std::vector<char> valid_roaring;
            uint32_t vr_sz = 0;
            if (!valid_all_ones && valid.size() > 0) {
                valid_roaring = CompressRoaring(valid);
                vr_sz = static_cast<uint32_t>(valid_roaring.size());
            }
            out.payload.resize(4 + rr_sz + vr_sz);
            std::memcpy(out.payload.data(), &rr_sz, 4);
            std::memcpy(out.payload.data() + 4, result_roaring.data(), rr_sz);
            if (vr_sz > 0) {
                std::memcpy(out.payload.data() + 4 + rr_sz,
                            valid_roaring.data(),
                            vr_sz);
            }
            return out;
        }

        // Mid-density (3%-97%): fall through to zero-copy Raw
    }

    // --- Raw path: zero-copy via pointers ---
    out.comp_type = kCompTypeRaw;
    out.raw_result_ptr = reinterpret_cast<const char*>(result.data());
    out.raw_result_size = result_bytes;
    if (!valid_all_ones && valid.size() > 0) {
        out.raw_valid_ptr = reinterpret_cast<const char*>(valid.data());
        out.raw_valid_size = valid_bytes;
    }
    return out;
}

// Backward-compat wrapper: flatten CompressedData into a single buffer
std::vector<char>
CacheCompressor::Compress(const TargetBitmap& result,
                          const TargetBitmap& valid,
                          bool compression_enabled,
                          uint8_t& out_comp_type) {
    auto cd = Compress(result, valid, compression_enabled);
    out_comp_type = cd.comp_type;
    std::vector<char> buf(cd.total_size());
    std::memcpy(buf.data(), cd.header, 8);
    if (cd.comp_type == kCompTypeRaw) {
        char* p = buf.data() + 8;
        if (cd.raw_result_size > 0) {
            std::memcpy(p, cd.raw_result_ptr, cd.raw_result_size);
            p += cd.raw_result_size;
        }
        if (cd.raw_valid_size > 0) {
            std::memcpy(p, cd.raw_valid_ptr, cd.raw_valid_size);
        }
    } else {
        std::memcpy(buf.data() + 8, cd.payload.data(), cd.payload.size());
    }
    return buf;
}

void
CacheCompressor::Decompress(const char* data,
                            uint32_t data_len,
                            uint8_t comp_type,
                            TargetBitmap& out_result,
                            TargetBitmap& out_valid) {
    if (data_len < kHeaderSize) {
        LOG_WARN("CacheCompressor::Decompress: data_len ({}) < header size",
                 data_len);
        return;
    }

    uint32_t result_bits = 0;
    uint32_t valid_bits_raw = 0;
    std::memcpy(&result_bits, data, 4);
    std::memcpy(&valid_bits_raw, data + 4, 4);

    bool valid_all_ones = (valid_bits_raw & kValidAllOnesMask) != 0;
    uint32_t valid_bits = valid_bits_raw & ~kValidAllOnesMask;

    if (result_bits == 0 && valid_bits == 0) {
        out_result = TargetBitmap(0);
        out_valid = TargetBitmap(0);
        return;
    }

    const char* payload = data + kHeaderSize;
    const uint32_t payload_len = data_len - kHeaderSize;

    // --- Roaring / RoaringInv path ---
    if (comp_type == kCompTypeRoaring || comp_type == kCompTypeRoaringInv) {
        if (payload_len < 4) {
            LOG_WARN("CacheCompressor::Decompress: roaring payload too short");
            return;
        }
        uint32_t rr_sz = 0;
        std::memcpy(&rr_sz, payload, 4);

        out_result = DecompressRoaring(payload + 4, rr_sz, result_bits);

        if (comp_type == kCompTypeRoaringInv) {
            out_result.flip();
        }

        if (valid_all_ones) {
            out_valid = TargetBitmap(valid_bits);
            out_valid.set();
        } else if (payload_len > 4 + rr_sz) {
            out_valid = DecompressRoaring(
                payload + 4 + rr_sz, payload_len - 4 - rr_sz, valid_bits);
        } else {
            out_valid = TargetBitmap(valid_bits, false);
        }
        return;
    }

    // --- Raw path ---
    if (comp_type != kCompTypeRaw) {
        LOG_WARN("CacheCompressor::Decompress: unknown comp_type={}",
                 comp_type);
        return;
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
        return;
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
}

}  // namespace exec
}  // namespace milvus
