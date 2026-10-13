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

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "index/fmindex/FMIndex.h"

namespace milvus::index::test {
namespace {

TEST(FmIndexLibraryTest, LibraryLoadViewZeroCopyAndLazyExtract) {
    std::vector<std::string> data{"apple", "banana", "grape"};
    std::vector<std::string_view> docs(data.begin(), data.end());
    fmindex::FMIndex built;
    built.Build(docs, /*sa_sample_rate=*/4);
    std::string blob = built.Serialize();

    // LoadView requires 8-byte alignment; std::string does not guarantee it.
    std::vector<uint64_t> aligned((blob.size() + 7) / 8);
    std::memcpy(aligned.data(), blob.data(), blob.size());
    auto viewed = fmindex::FMIndex::LoadView(
        reinterpret_cast<const uint8_t*>(aligned.data()), blob.size());
    ASSERT_TRUE(viewed.valid());
    EXPECT_EQ(viewed.document_count(), docs.size());

    auto p = [](const char* s) { return reinterpret_cast<const uint8_t*>(s); };
    EXPECT_EQ(viewed.MatchingDocs(p("an"), 2), (std::vector<uint64_t>{1}));
    EXPECT_EQ(viewed.CountPrefixDocs(p("gr"), 2), 1u);
    // Extract triggers the lazy ISA build on the viewed index.
    const auto heap_before_extract = viewed.resident_heap_bytes();
    EXPECT_EQ(viewed.Extract(0, 0, 5), "apple");
    EXPECT_EQ(viewed.Extract(1, 2, 4), "nana");
    EXPECT_GT(viewed.resident_heap_bytes(), heap_before_extract);
    // Round-trip: re-serializing the viewed index reproduces the blob.
    EXPECT_EQ(viewed.Serialize(), blob);
}

TEST(FmIndexLibraryTest, LibraryResidentHeapAccountingForLoadView) {
    std::vector<std::string> data;
    data.reserve(512);
    for (size_t i = 0; i < 512; ++i) {
        std::string row(512, static_cast<char>('a' + i % 26));
        for (size_t j = 0; j < row.size(); j += 31) {
            row[j] = static_cast<char>((i + j) & 0xFF);
        }
        data.push_back(std::move(row));
    }
    std::vector<std::string_view> docs(data.begin(), data.end());

    auto load_view = [&](uint32_t block_bytes) {
        fmindex::FMIndex built;
        built.Build(docs,
                    /*sa_sample_rate=*/8,
                    /*case_insensitive=*/false,
                    /*force_wide=*/false,
                    block_bytes);
        std::string blob = built.Serialize();
        std::vector<uint64_t> aligned((blob.size() + 7) / 8);
        std::memcpy(aligned.data(), blob.data(), blob.size());
        auto viewed = fmindex::FMIndex::LoadView(
            reinterpret_cast<const uint8_t*>(aligned.data()), blob.size());
        EXPECT_TRUE(viewed.valid());
        return std::make_tuple(
            std::move(viewed), std::move(blob), std::move(aligned));
    };

    auto [fine, fine_blob, fine_backing] = load_view(/*block_bytes=*/8);
    auto [coarse, coarse_blob, coarse_backing] = load_view(/*block_bytes=*/128);
    ASSERT_TRUE(fine.valid());
    ASSERT_TRUE(coarse.valid());

    EXPECT_GT(fine.rank_directory_bytes(), coarse.rank_directory_bytes());
    EXPECT_GT(fine.resident_heap_bytes(), coarse.resident_heap_bytes());
    // The mapped payload is disk accounting, not heap accounting. With coarse
    // directories, the heap retained by LoadView is only a small fraction of
    // the serialized blob rather than the previous full-blob estimate.
    EXPECT_LT(coarse.resident_heap_bytes(), coarse_blob.size());

    auto owned = fmindex::FMIndex::Deserialize(coarse_blob);
    ASSERT_TRUE(owned.valid());
    EXPECT_GE(owned.resident_heap_bytes(),
              coarse.resident_heap_bytes() + coarse_blob.size());
}

TEST(FmIndexLibraryTest, LibraryDocLocateBoundsOutOfRangePositions) {
    std::vector<std::string> data{"alpha", "beta", "gamma", "delta"};
    std::vector<std::string_view> docs(data.begin(), data.end());
    fmindex::FMIndex built;
    // Sample rate 1 makes every row sampled, so locateRow returns a stored
    // sample value with zero LF steps — the tampered value reaches the call
    // sites verbatim. force_wide pins samples to 8 bytes so the trailing
    // sections are exactly sized with no alignment padding between them.
    built.Build(docs,
                /*sa_sample_rate=*/1,
                /*case_insensitive=*/false,
                /*force_wide=*/true);
    const std::string clean = built.Serialize();

    size_t text_len = docs.size();  // one separator per document
    for (const auto& d : docs) {
        text_len += d.size();
    }
    // Trailing payload sections: sampled-SA values (n_samples * 8) then the
    // document boundaries (n_docs * 8).
    const size_t n_samples = text_len + 1;  // rate 1: text_len/1 + 1
    const size_t n_docs = docs.size() + 1;  // boundaries, not documents
    ASSERT_GT(clean.size(), (n_samples + n_docs) * sizeof(uint64_t));
    const size_t docs_off = clean.size() - n_docs * sizeof(uint64_t);
    const size_t samples_off = docs_off - n_samples * sizeof(uint64_t);

    // Pin the assumed layout before poking at it: the boundary list runs
    // 0 .. text_len. If serialization ever moves these sections, this fails
    // loudly here instead of silently tampering with unrelated bytes.
    auto read_u64 = [&clean](size_t off) {
        uint64_t v = 0;
        std::memcpy(&v, clean.data() + off, sizeof(v));
        return v;
    };
    ASSERT_EQ(read_u64(docs_off), 0u);
    ASSERT_EQ(read_u64(clean.size() - sizeof(uint64_t)), text_len);

    auto p = [](const char* s) { return reinterpret_cast<const uint8_t*>(s); };
    auto load_view = [](const std::string& blob) {
        std::vector<uint64_t> backing((blob.size() + 7) / 8);
        std::memcpy(backing.data(), blob.data(), blob.size());
        auto idx = fmindex::FMIndex::LoadView(
            reinterpret_cast<const uint8_t*>(backing.data()), blob.size());
        return std::make_pair(std::move(idx), std::move(backing));
    };

    // Baseline: the guards must not reject anything on a well-formed index.
    {
        auto [ok, backing] = load_view(clean);
        ASSERT_TRUE(ok.valid());
        EXPECT_EQ(ok.LocatePrefixDocs(p("gam"), 3), (std::vector<uint64_t>{2}));
        EXPECT_EQ(ok.LocateSuffixDocs(p("lta"), 3), (std::vector<uint64_t>{3}));
        EXPECT_EQ(ok.LocatePrefixDocs(p("alp"), 3), (std::vector<uint64_t>{0}));
    }

    // Every sample now claims the sentinel position. text_len is <= text_len and
    // divisible by rate 1, and neither the count nor sampled_bv_ changed, so
    // this passes load validation unchanged — which is the point.
    std::string tampered = clean;
    for (size_t i = 0; i < n_samples; ++i) {
        const uint64_t v = text_len;
        std::memcpy(tampered.data() + samples_off + i * sizeof(uint64_t),
                    &v,
                    sizeof(v));
    }

    auto [bad, bad_backing] = load_view(tampered);
    ASSERT_TRUE(bad.valid()) << "load validation cannot catch this; the bound "
                                "check at the locate call sites is the fix";

    // Suffix hits locate to text_len (== text_len, out of range) and must be
    // dropped entirely rather than mapped to document_count().
    EXPECT_TRUE(bad.LocateSuffixDocs(p("lta"), 3).empty());
    // Prefix adds one before mapping, so it must be checked after the +1.
    for (const char* pat : {"gam", "bet", "del", "alp"}) {
        for (const auto& got : {bad.LocatePrefixDocs(p(pat), 3),
                                bad.LocateSuffixDocs(p(pat), 3)}) {
            for (uint64_t d : got) {
                EXPECT_LT(d, bad.document_count())
                    << "out-of-range document id escaped locate for " << pat;
            }
        }
    }
}

}  // namespace
}  // namespace milvus::index::test
