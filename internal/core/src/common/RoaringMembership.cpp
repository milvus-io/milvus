// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "common/RoaringMembership.h"

#include <new>

#include <algorithm>
#include <bit>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string_view>
#include <utility>

#include "common/EasyAssert.h"

namespace milvus {
namespace {

// These are RoaringFormatSpec values, not a Milvus contract, so unlike the
// limits in RoaringMembership.h they are not generated from a Go counterpart:
// both sides implement a published format, and two of them are derived rather
// than chosen (kPortableBitmapBytes is 65536/8, kPortableRoaring64MinEntryBytes
// is the 4-byte high key plus the 8-byte minimum child).
//
// Each side is instead held to the format behaviourally, at the boundary, by
// fixtures written independently on each side -- the Go validator accepts
// CRoaring-written bytes (TestValidateAcceptsCRoaringGeneratedFixtures) and
// this file accepts Go-written ones (ParsesGoGeneratedBitmapFixture and its
// siblings), across every container type and both sides of the offset-table
// threshold. That catches something a value check cannot: flipping a `>` to a
// `>=` here diverges the two sides while every declaration still reads the
// same. It is not strictly stronger, though. Tightening
// kPortableRoaring64MinEntryBytes 12 -> 16 fails on both sides; loosening it
// 12 -> 8 fails on neither, which is benign -- the bound is a pre-filter and
// the full scan behind it rejects the same input -- but the two sides really
// can drift in that direction unnoticed.
constexpr uint32_t kPortableCookieNoRun = 12346;
constexpr uint16_t kPortableCookieRun = 12347;
constexpr uint32_t kPortableArrayMaxCardinality = 4096;
constexpr size_t kPortableBitmapBytes = 8192;
constexpr uint32_t kPortableNoOffsetThreshold = 4;
constexpr size_t kPortableRoaring64PrefixBytes = 8;
// 4-byte high key plus the smallest portable Roaring32 child: the no-run
// cookie followed by a container count of zero. CRoaring emits that for an
// empty child and both CRoaring and roaring/v2 consume it.
constexpr size_t kPortableRoaring64MinEntryBytes = 12;

struct PortableRoaringSummary {
    size_t consumed;
    uint64_t cardinality;
    uint64_t container_count;
};

struct PortableRoaring64Summary {
    size_t consumed;
    uint64_t cardinality;
    uint64_t high_container_count;
    uint64_t low_container_count;
    uint64_t estimated_decoded_bytes;
};

static_assert(sizeof(std::pair<uint32_t, roaring::Roaring>) <=
              RoaringMembership::kEstimatedHighContainerOverheadBytes);

uint16_t
ReadLittleEndianU16(const char* data) {
    const auto* bytes = reinterpret_cast<const uint8_t*>(data);
    return static_cast<uint16_t>(bytes[0]) |
           (static_cast<uint16_t>(bytes[1]) << 8);
}

uint32_t
ReadLittleEndianU32(const char* data) {
    const auto* bytes = reinterpret_cast<const uint8_t*>(data);
    uint32_t value = 0;
    for (size_t i = 0; i < sizeof(value); ++i) {
        value |= static_cast<uint32_t>(bytes[i]) << (8 * i);
    }
    return value;
}

uint64_t
ReadLittleEndianU64(const char* data) {
    const auto* bytes = reinterpret_cast<const uint8_t*>(data);
    uint64_t value = 0;
    for (size_t i = 0; i < sizeof(value); ++i) {
        value |= static_cast<uint64_t>(bytes[i]) << (8 * i);
    }
    return value;
}

void
RequirePortableBytes(std::string_view data,
                     size_t cursor,
                     size_t size,
                     std::string_view what) {
    if (cursor > data.size() || size > data.size() - cursor) {
        ThrowInfo(
            ExprInvalid, "invalid portable roaring64 body: truncated {}", what);
    }
}

size_t
CheckedTableBytes(size_t count, size_t element_size, std::string_view what) {
    if (element_size != 0 &&
        count > std::numeric_limits<size_t>::max() / element_size) {
        ThrowInfo(ExprInvalid,
                  "invalid portable roaring64 body: {} size overflows",
                  what);
    }
    return count * element_size;
}

void
CheckedAddCardinality(uint64_t& total,
                      uint64_t cardinality,
                      std::string_view what) {
    if (cardinality > std::numeric_limits<uint64_t>::max() - total) {
        ThrowInfo(ExprInvalid,
                  "invalid portable roaring64 body: {} cardinality overflows",
                  what);
    }
    total += cardinality;
}

PortableRoaringSummary
ValidatePortableArrayContainer(std::string_view data,
                               size_t cursor,
                               uint32_t cardinality) {
    const auto bytes_needed =
        CheckedTableBytes(cardinality, sizeof(uint16_t), "array container");
    RequirePortableBytes(data, cursor, bytes_needed, "array container");

    uint16_t previous = 0;
    for (uint32_t i = 0; i < cardinality; ++i) {
        const auto value = ReadLittleEndianU16(
            data.data() + cursor + static_cast<size_t>(i) * sizeof(uint16_t));
        if (i > 0 && value <= previous) {
            ThrowInfo(ExprInvalid,
                      "invalid portable roaring64 body: array values must be "
                      "strictly increasing");
        }
        previous = value;
    }
    return {cursor + bytes_needed, cardinality, 0};
}

PortableRoaringSummary
ValidatePortableBitmapContainer(std::string_view data,
                                size_t cursor,
                                uint32_t cardinality) {
    RequirePortableBytes(
        data, cursor, kPortableBitmapBytes, "bitmap container");

    uint32_t actual_cardinality = 0;
    for (size_t offset = 0; offset < kPortableBitmapBytes;
         offset += sizeof(uint64_t)) {
        actual_cardinality +=
            std::popcount(ReadLittleEndianU64(data.data() + cursor + offset));
    }
    if (actual_cardinality != cardinality) {
        ThrowInfo(ExprInvalid,
                  "invalid portable roaring64 body: bitmap cardinality {} does "
                  "not match descriptor {}",
                  actual_cardinality,
                  cardinality);
    }
    return {cursor + kPortableBitmapBytes, actual_cardinality, 0};
}

PortableRoaringSummary
ValidatePortableRunContainer(std::string_view data,
                             size_t cursor,
                             uint32_t cardinality) {
    RequirePortableBytes(data, cursor, sizeof(uint16_t), "run count");
    const auto run_count = ReadLittleEndianU16(data.data() + cursor);
    // A run count of zero cannot be consistent with any descriptor: the
    // descriptor stores cardinality - 1, so it cannot express an empty
    // container, and a zero-run body is rejected downstream on the cardinality
    // mismatch regardless. Checking here only makes the error say what is
    // actually wrong.
    if (run_count == 0) {
        ThrowInfo(ExprInvalid,
                  "invalid portable roaring64 body: invalid run count 0");
    }
    cursor += sizeof(uint16_t);

    const auto run_bytes =
        CheckedTableBytes(run_count, 2 * sizeof(uint16_t), "run intervals");
    RequirePortableBytes(data, cursor, run_bytes, "run intervals");

    uint32_t previous_end = 0;
    uint64_t cardinality_sum = 0;
    for (uint32_t i = 0; i < run_count; ++i) {
        const auto offset = cursor + static_cast<size_t>(i) * 4;
        const uint32_t start = ReadLittleEndianU16(data.data() + offset);
        const uint32_t length =
            ReadLittleEndianU16(data.data() + offset + sizeof(uint16_t));
        const uint32_t end = start + length;
        if (end > std::numeric_limits<uint16_t>::max()) {
            ThrowInfo(ExprInvalid,
                      "invalid portable roaring64 body: run interval exceeds "
                      "uint16 range");
        }
        // The portable format requires runs to be sorted and non-overlapping,
        // NOT merged: [0,0] followed by [1,1] is a legal encoding of {0,1} and
        // both roaring/v2 and CRoaring decode it that way. Reject only actual
        // overlap or descending order.
        if (i > 0 && start <= previous_end) {
            ThrowInfo(ExprInvalid,
                      "invalid portable roaring64 body: run intervals overlap "
                      "or are out of order");
        }
        previous_end = end;

        const uint64_t run_cardinality = static_cast<uint64_t>(length) + 1;
        if (run_cardinality > (uint64_t{1} << 16) - cardinality_sum) {
            ThrowInfo(
                ExprInvalid,
                "invalid portable roaring64 body: run cardinality exceeds "
                "container capacity");
        }
        cardinality_sum += run_cardinality;
    }
    if (cardinality_sum != cardinality) {
        ThrowInfo(
            ExprInvalid,
            "invalid portable roaring64 body: run cardinality {} does not "
            "match descriptor {}",
            cardinality_sum,
            cardinality);
    }
    return {cursor + run_bytes, cardinality_sum, 0};
}

PortableRoaringSummary
ValidatePortableRoaring32(std::string_view data) {
    RequirePortableBytes(data, 0, sizeof(uint32_t), "Roaring32 cookie");
    const auto cookie = ReadLittleEndianU32(data.data());
    size_t cursor = sizeof(uint32_t);
    uint32_t container_count;
    size_t run_bitmap_start = 0;
    size_t run_bitmap_bytes = 0;
    const bool has_run_bitmap =
        static_cast<uint16_t>(cookie) == kPortableCookieRun;

    if (has_run_bitmap) {
        container_count = (cookie >> 16) + 1;
        run_bitmap_bytes = (static_cast<size_t>(container_count) + 7) / 8;
        RequirePortableBytes(data, cursor, run_bitmap_bytes, "run bitmap");
        run_bitmap_start = cursor;
        cursor += run_bitmap_bytes;

        // The portable format sizes the run bitmap at (container_count + 7) / 8
        // bytes and assigns meaning only to the first container_count bits. The
        // remaining bits are unspecified padding: roaring/v2 and CRoaring both
        // ignore them, so requiring them to be zero would reject bodies those
        // writers can produce.
    } else if (cookie == kPortableCookieNoRun) {
        RequirePortableBytes(
            data, cursor, sizeof(uint32_t), "Roaring32 container count");
        container_count = ReadLittleEndianU32(data.data() + cursor);
        cursor += sizeof(uint32_t);
        // A zero-container child is legal; see kPortableRoaring64MinEntryBytes.
        if (container_count > (uint32_t{1} << 16)) {
            ThrowInfo(ExprInvalid,
                      "invalid portable roaring64 body: invalid Roaring32 "
                      "container count {}",
                      container_count);
        }
    } else {
        ThrowInfo(ExprInvalid,
                  "invalid portable roaring64 body: unsupported Roaring32 "
                  "cookie {}",
                  cookie);
    }

    const auto descriptor_bytes = CheckedTableBytes(
        container_count, 2 * sizeof(uint16_t), "container descriptors");
    RequirePortableBytes(
        data, cursor, descriptor_bytes, "container descriptors");
    const auto descriptor_start = cursor;
    uint16_t previous_key = 0;
    for (uint32_t i = 0; i < container_count; ++i) {
        const auto offset =
            descriptor_start + static_cast<size_t>(i) * 2 * sizeof(uint16_t);
        const auto key = ReadLittleEndianU16(data.data() + offset);
        if (i > 0 && key <= previous_key) {
            ThrowInfo(ExprInvalid,
                      "invalid portable roaring64 body: Roaring32 container {} "
                      "has a non-increasing key",
                      i);
        }
        previous_key = key;
    }
    cursor += descriptor_bytes;

    // The offset table is skipped, not verified. It is redundant with the
    // container layout, and both roaring/v2 and CRoaring ignore its contents on
    // read, so requiring each entry to equal the position we computed would
    // reject bodies the reference readers accept. Its SIZE still has to be
    // present, which is what keeps the remaining bounds checks honest.
    const bool has_offsets =
        !has_run_bitmap || container_count >= kPortableNoOffsetThreshold;
    if (has_offsets) {
        const auto offset_bytes = CheckedTableBytes(
            container_count, sizeof(uint32_t), "container offsets");
        RequirePortableBytes(data, cursor, offset_bytes, "container offsets");
        cursor += offset_bytes;
    }

    uint64_t cardinality_sum = 0;
    for (uint32_t i = 0; i < container_count; ++i) {
        const auto descriptor_offset =
            descriptor_start + static_cast<size_t>(i) * 2 * sizeof(uint16_t);
        const uint32_t cardinality =
            static_cast<uint32_t>(ReadLittleEndianU16(
                data.data() + descriptor_offset + sizeof(uint16_t))) +
            1;
        const bool is_run =
            has_run_bitmap &&
            (static_cast<uint8_t>(data[run_bitmap_start + i / 8]) &
             static_cast<uint8_t>(uint8_t{1} << (i % 8))) != 0;

        PortableRoaringSummary container_summary;
        if (is_run) {
            container_summary =
                ValidatePortableRunContainer(data, cursor, cardinality);
        } else if (cardinality > kPortableArrayMaxCardinality) {
            container_summary =
                ValidatePortableBitmapContainer(data, cursor, cardinality);
        } else {
            container_summary =
                ValidatePortableArrayContainer(data, cursor, cardinality);
        }
        cursor = container_summary.consumed;
        CheckedAddCardinality(
            cardinality_sum, container_summary.cardinality, "Roaring32");
    }
    return {cursor, cardinality_sum, container_count};
}

uint64_t
EstimateDecodedBytes(size_t body_size,
                     uint64_t high_container_count,
                     uint64_t low_container_count) {
    uint64_t estimated = body_size;
    const auto add_scaled = [&estimated](uint64_t count,
                                         uint64_t bytes_per_item,
                                         std::string_view what) {
        if (count != 0 &&
            bytes_per_item >
                (std::numeric_limits<uint64_t>::max() - estimated) / count) {
            ThrowInfo(ExprInvalid,
                      "roaring membership estimated decoded {} bytes overflow",
                      what);
        }
        estimated += count * bytes_per_item;
    };
    add_scaled(high_container_count,
               RoaringMembership::kEstimatedHighContainerOverheadBytes,
               "high-container overhead");
    add_scaled(low_container_count,
               RoaringMembership::kEstimatedLowContainerOverheadBytes,
               "low-container overhead");
    return estimated;
}

PortableRoaring64Summary
ValidatePortableRoaring64(std::string_view body) {
    RequirePortableBytes(
        body, 0, kPortableRoaring64PrefixBytes, "high-container count");
    const auto high_container_count = ReadLittleEndianU64(body.data());
    const auto max_count_from_body =
        (body.size() - kPortableRoaring64PrefixBytes) /
        kPortableRoaring64MinEntryBytes;
    if (high_container_count > max_count_from_body) {
        ThrowInfo(ExprInvalid,
                  "invalid portable roaring64 body: high-container count {} "
                  "exceeds body-derived maximum {}",
                  high_container_count,
                  max_count_from_body);
    }
    if (high_container_count > RoaringMembership::kMaxHighContainerCount) {
        ThrowInfo(ExprInvalid,
                  "roaring membership high-container count {} exceeds maximum "
                  "{}",
                  high_container_count,
                  RoaringMembership::kMaxHighContainerCount);
    }

    size_t cursor = kPortableRoaring64PrefixBytes;
    uint32_t previous_high = 0;
    uint64_t cardinality_sum = 0;
    uint64_t low_container_count = 0;
    for (uint64_t i = 0; i < high_container_count; ++i) {
        RequirePortableBytes(body, cursor, sizeof(uint32_t), "high key");
        const auto high = ReadLittleEndianU32(body.data() + cursor);
        if (i > 0 && high <= previous_high) {
            ThrowInfo(ExprInvalid,
                      "invalid portable roaring64 body: high container {} has "
                      "a non-increasing key",
                      i);
        }
        previous_high = high;
        cursor += sizeof(uint32_t);

        const auto child_summary =
            ValidatePortableRoaring32(body.substr(cursor));
        if (child_summary.consumed > body.size() - cursor) {
            ThrowInfo(ExprInvalid,
                      "invalid portable roaring64 body: child consumption "
                      "exceeds body");
        }
        CheckedAddCardinality(
            cardinality_sum, child_summary.cardinality, "Roaring64");
        CheckedAddCardinality(low_container_count,
                              child_summary.container_count,
                              "low-container count");
        cursor += child_summary.consumed;
    }
    if (cursor != body.size()) {
        ThrowInfo(ExprInvalid,
                  "invalid portable roaring64 body: consumed {} bytes, "
                  "expected {}",
                  cursor,
                  body.size());
    }
    const auto estimated_decoded_bytes = EstimateDecodedBytes(
        body.size(), high_container_count, low_container_count);
    if (estimated_decoded_bytes >
        RoaringMembership::kMaxEstimatedDecodedBytes) {
        ThrowInfo(ExprInvalid,
                  "roaring membership estimated decoded size {} bytes exceeds "
                  "maximum {}",
                  estimated_decoded_bytes,
                  RoaringMembership::kMaxEstimatedDecodedBytes);
    }
    return {cursor,
            cardinality_sum,
            high_container_count,
            low_container_count,
            estimated_decoded_bytes};
}

struct ValidatedMrb1 {
    std::string_view body;
    RoaringMembership::ValidationSummary summary;
};

ValidatedMrb1
ValidateMrb1(std::string_view blob) {
    if (blob.size() < RoaringMembership::kHeaderSize) {
        ThrowInfo(ExprInvalid,
                  "roaring membership blob is too short: {} bytes, expected "
                  "at least {}",
                  blob.size(),
                  RoaringMembership::kHeaderSize);
    }
    if (std::memcmp(blob.data(),
                    RoaringMembership::kMagic.data(),
                    RoaringMembership::kMagic.size()) != 0) {
        ThrowInfo(ExprInvalid, "roaring membership blob has invalid magic");
    }

    const auto version = ReadLittleEndianU16(blob.data() + 4);
    if (version != RoaringMembership::kVersion) {
        ThrowInfo(ExprInvalid,
                  "unsupported roaring membership version {}, expected {}",
                  version,
                  RoaringMembership::kVersion);
    }

    const auto format = ReadLittleEndianU16(blob.data() + 6);
    if (format != RoaringMembership::kFormatPortableRoaring64) {
        ThrowInfo(ExprInvalid,
                  "unsupported roaring membership format {}, expected {}",
                  format,
                  RoaringMembership::kFormatPortableRoaring64);
    }

    const auto declared_cardinality = ReadLittleEndianU64(blob.data() + 8);
    const auto declared_body_size = ReadLittleEndianU64(blob.data() + 16);
    const auto reserved = ReadLittleEndianU64(blob.data() + 24);
    if (reserved != 0) {
        ThrowInfo(ExprInvalid,
                  "roaring membership reserved field must be zero, got {}",
                  reserved);
    }
    if (declared_body_size > RoaringMembership::kMaxBodySize) {
        ThrowInfo(ExprInvalid,
                  "roaring membership body is too large: {} bytes, maximum "
                  "is {}",
                  declared_body_size,
                  RoaringMembership::kMaxBodySize);
    }

    const size_t body_size = blob.size() - RoaringMembership::kHeaderSize;
    if (body_size > RoaringMembership::kMaxBodySize) {
        ThrowInfo(ExprInvalid,
                  "roaring membership body is too large: {} bytes, maximum "
                  "is {}",
                  body_size,
                  RoaringMembership::kMaxBodySize);
    }
    if (declared_body_size != body_size) {
        ThrowInfo(ExprInvalid,
                  "roaring membership body length {} does not match declared "
                  "length {}",
                  body_size,
                  declared_body_size);
    }

    const std::string_view body(blob.data() + RoaringMembership::kHeaderSize,
                                body_size);
    const auto validated = ValidatePortableRoaring64(body);
    if (validated.consumed != body_size) {
        ThrowInfo(ExprInvalid,
                  "invalid portable roaring64 body: consumed {} bytes, "
                  "expected {}",
                  validated.consumed,
                  body_size);
    }
    if (validated.cardinality != declared_cardinality) {
        ThrowInfo(ExprInvalid,
                  "roaring membership wire cardinality {} does not match "
                  "declared cardinality {}",
                  validated.cardinality,
                  declared_cardinality);
    }

    return {body,
            {validated.cardinality,
             body_size,
             validated.high_container_count,
             validated.low_container_count,
             validated.estimated_decoded_bytes}};
}

struct DecodedPortableRoaring64 {
    std::vector<std::pair<uint32_t, roaring::Roaring>> bitmaps;
    PortableRoaringSummary wire_summary;
};

DecodedPortableRoaring64
DecodePortableRoaring64(std::string_view body, uint64_t high_container_count) {
    std::vector<std::pair<uint32_t, roaring::Roaring>> bitmaps;
    try {
        bitmaps.reserve(static_cast<size_t>(high_container_count));
    } catch (const std::bad_alloc&) {
        // high_container_count already passed the envelope bounds check, so a
        // failure here is the allocator refusing, not a bad request.
        ThrowInfo(MemAllocateFailed,
                  "failed to allocate {} roaring64 high containers",
                  high_container_count);
    } catch (const std::length_error&) {
        // Not an allocator refusal: length_error means the request exceeded
        // vector::max_size(). high_container_count is already capped at
        // kMaxHighContainerCount (2^18), far below max_size() on any supported
        // platform, so reaching this is an internal or platform invariant
        // violation that would reproduce identically on every replica.
        // UnexpectedError makes the final client-facing status non-retriable.
        // It is not an InputError, so the proxy LB still fails over to the
        // remaining shard leaders and blacklists this one before giving up;
        // that costs a few doomed attempts but is acceptable for a branch that
        // is unreachable in practice, and it avoids blaming the client for a
        // fault we cannot attribute to the request.
        ThrowInfo(UnexpectedError,
                  "roaring64 high-container count {} exceeds vector capacity",
                  high_container_count);
    }
    size_t cursor = kPortableRoaring64PrefixBytes;
    uint64_t cardinality_sum = 0;
    uint64_t low_container_count = 0;
    for (uint64_t i = 0; i < high_container_count; ++i) {
        RequirePortableBytes(body, cursor, sizeof(uint32_t), "high key");
        const auto high = ReadLittleEndianU32(body.data() + cursor);
        cursor += sizeof(uint32_t);

        const auto child_summary =
            ValidatePortableRoaring32(body.substr(cursor));
        CheckedAddCardinality(
            cardinality_sum, child_summary.cardinality, "Roaring64");
        CheckedAddCardinality(low_container_count,
                              child_summary.container_count,
                              "low-container count");

        // Decode only the exact span that the allocation-free validator just
        // accepted. In particular, do not advance by CRoaring's normalized
        // getSizeInBytes(): legal compact run-cookie children can normalize to
        // a different serialized size.
        roaring::Roaring bitmap;
        try {
            bitmap = roaring::Roaring::readSafe(body.data() + cursor,
                                                child_summary.consumed);
        } catch (const std::bad_alloc&) {
            // Unambiguous: the allocator said no. Unlike the CRoaring
            // runtime_error below, no inference is needed, so use the code
            // merr marks retriable.
            ThrowInfo(MemAllocateFailed,
                      "failed to allocate while decoding roaring64 child");
        } catch (const std::length_error& error) {
            ThrowInfo(ExprInvalid,
                      "invalid portable roaring64 body: {}",
                      error.what());
        } catch (const std::runtime_error& error) {
            // ValidatePortableRoaring32 just accepted exactly this span
            // without allocating, so the bytes are already known well-formed.
            // CRoaring funnels malformed input and allocation failure alike
            // through a NULL return reported as runtime_error("failed alloc
            // while reading"), so the cause cannot be established here. What
            // matters is that it is not the caller's fault: ExprInvalid maps
            // to a Go InputError, which makes the proxy blame the client and
            // skip replica failover. UnexpectedError stays a generic system
            // error without asserting a cause we cannot prove.
            ThrowInfo(UnexpectedError,
                      "failed to decode validated roaring64 child: {}",
                      error.what());
        }

        uint64_t decoded_cardinality;
        try {
            decoded_cardinality = bitmap.cardinality();
        } catch (const std::bad_alloc&) {
            ThrowInfo(MemAllocateFailed,
                      "failed to allocate while counting roaring64 child");
        } catch (const std::length_error& error) {
            ThrowInfo(ExprInvalid,
                      "invalid portable roaring64 body: {}",
                      error.what());
        } catch (const std::runtime_error& error) {
            // Same reasoning: the child decoded successfully above, so this
            // is not malformed input.
            ThrowInfo(UnexpectedError,
                      "failed to count validated roaring64 child: {}",
                      error.what());
        }
        if (decoded_cardinality != child_summary.cardinality) {
            ThrowInfo(ExprInvalid,
                      "invalid portable roaring64 body: decoded child "
                      "cardinality {} does not match validated cardinality {}",
                      decoded_cardinality,
                      child_summary.cardinality);
        }

        bitmaps.emplace_back(high, std::move(bitmap));
        cursor += child_summary.consumed;
    }
    return {std::move(bitmaps), {cursor, cardinality_sum, low_container_count}};
}

}  // namespace

RoaringMembership::ValidationSummary
RoaringMembership::Validate(std::string_view blob) {
    return ValidateMrb1(blob).summary;
}

std::shared_ptr<const RoaringMembership>
RoaringMembership::Parse(std::string_view blob) {
    const auto validated = ValidateMrb1(blob);

    auto decoded = DecodePortableRoaring64(
        validated.body, validated.summary.high_container_count);
    if (decoded.wire_summary.consumed != validated.body.size() ||
        decoded.wire_summary.cardinality != validated.summary.cardinality ||
        decoded.wire_summary.container_count !=
            validated.summary.low_container_count) {
        ThrowInfo(ExprInvalid,
                  "invalid portable roaring64 body: decoded structure does not "
                  "match validated structure");
    }

    return std::shared_ptr<const RoaringMembership>(
        new RoaringMembership(std::move(decoded.bitmaps),
                              validated.summary.cardinality,
                              validated.summary.body_size));
}

RoaringMembership::RoaringMembership(
    std::vector<std::pair<uint32_t, roaring::Roaring>> bitmaps,
    uint64_t cardinality,
    size_t serialized_size)
    : bitmaps_(std::move(bitmaps)),
      cardinality_(cardinality),
      serialized_size_(serialized_size) {
}

bool
RoaringMembership::Contains(int64_t value) const {
    const auto unsigned_value = static_cast<uint64_t>(value);
    const auto high = static_cast<uint32_t>(unsigned_value >> 32);
    const auto low = static_cast<uint32_t>(unsigned_value);
    const auto it = std::lower_bound(
        bitmaps_.begin(),
        bitmaps_.end(),
        high,
        [](const auto& entry, uint32_t key) { return entry.first < key; });
    return it != bitmaps_.end() && it->first == high &&
           it->second.contains(low);
}

uint64_t
RoaringMembership::cardinality() const {
    return cardinality_;
}

size_t
RoaringMembership::serialized_size() const {
    return serialized_size_;
}

}  // namespace milvus
