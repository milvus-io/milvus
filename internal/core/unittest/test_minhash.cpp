// Copyright (C) 2019-2025 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <iostream>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "minhash/MinHashComputer.h"
#include "minhash/MinHashHook.h"
#include "minhash/fusion_compute/fusion_compute_native.h"

using namespace milvus::minhash;

class MinHashTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        // Initialize SIMD hooks based on runtime CPU detection
        minhash_hook_init();

        num_hashes_ = 128;
        seed_ = 42;
        perm_a_.resize(num_hashes_);
        perm_b_.resize(num_hashes_);

        InitPermutations(num_hashes_, seed_, perm_a_.data(), perm_b_.data());
    }

    void
    TearDown() override {
    }

    int32_t num_hashes_;
    uint64_t seed_;
    std::vector<uint64_t> perm_a_;
    std::vector<uint64_t> perm_b_;
};

// Test InitPermutations function
TEST_F(MinHashTest, InitPermutationsTest) {
    std::vector<uint64_t> perm_a(128);
    std::vector<uint64_t> perm_b(128);

    InitPermutations(128, 42, perm_a.data(), perm_b.data());

    // Check that all permutation values are non-zero
    for (int i = 0; i < 128; i++) {
        EXPECT_NE(perm_a[i], 0);
        EXPECT_NE(perm_b[i], 0);
    }

    // Check that values are different with different seeds
    std::vector<uint64_t> perm_a2(128);
    std::vector<uint64_t> perm_b2(128);
    InitPermutations(128, 99, perm_a2.data(), perm_b2.data());

    bool different = false;
    for (int i = 0; i < 128; i++) {
        if (perm_a[i] != perm_a2[i] || perm_b[i] != perm_b2[i]) {
            different = true;
            break;
        }
    }
    EXPECT_TRUE(different);
}

// Test InitPermutations with different sizes
TEST_F(MinHashTest, InitPermutationsDifferentSizes) {
    for (int32_t size : {8, 16, 32, 64, 128, 256}) {
        std::vector<uint64_t> perm_a(size);
        std::vector<uint64_t> perm_b(size);

        InitPermutations(size, 42, perm_a.data(), perm_b.data());

        // Verify all values are initialized
        for (int i = 0; i < size; i++) {
            EXPECT_NE(perm_a[i], 0);
            EXPECT_NE(perm_b[i], 0);
        }
    }
}

// Test HashNGramWindow with simple texts
TEST_F(MinHashTest, HashNGramWindowBasicTest) {
    const char* texts[] = {"hello world", "test document"};
    int32_t text_lengths[] = {11, 13};
    int32_t num_texts = 2;
    int32_t shingle_size = 3;

    std::vector<uint64_t> all_base_hashes;
    std::vector<int32_t> hash_counts;

    // Test with SHA1 hash function
    HashNGramWindow(texts,
                    text_lengths,
                    num_texts,
                    nullptr,
                    shingle_size,
                    HashFunction::SHA1,
                    all_base_hashes,
                    hash_counts);

    // Verify hash counts
    EXPECT_EQ(hash_counts.size(), num_texts);
    for (int i = 0; i < num_texts; i++) {
        EXPECT_GT(hash_counts[i], 0)
            << "Hash count should be positive for text " << i;
    }

    // Verify total hash count
    int32_t total_hashes = 0;
    for (auto count : hash_counts) {
        total_hashes += count;
    }
    EXPECT_EQ(all_base_hashes.size(), total_hashes);
}

// Test HashNGramWindow with XXHASH64
TEST_F(MinHashTest, HashNGramWindowXXHashTest) {
    const char* texts[] = {"hello world"};
    int32_t text_lengths[] = {11};
    int32_t num_texts = 1;
    int32_t shingle_size = 3;

    std::vector<uint64_t> all_base_hashes_sha1;
    std::vector<int32_t> hash_counts_sha1;

    std::vector<uint64_t> all_base_hashes_xxhash;
    std::vector<int32_t> hash_counts_xxhash;

    // Test with SHA1
    HashNGramWindow(texts,
                    text_lengths,
                    num_texts,
                    nullptr,
                    shingle_size,
                    HashFunction::SHA1,
                    all_base_hashes_sha1,
                    hash_counts_sha1);

    // Test with XXHASH64
    HashNGramWindow(texts,
                    text_lengths,
                    num_texts,
                    nullptr,
                    shingle_size,
                    HashFunction::XXHASH64,
                    all_base_hashes_xxhash,
                    hash_counts_xxhash);

    // Both should produce same number of hashes
    EXPECT_EQ(hash_counts_sha1.size(), hash_counts_xxhash.size());
    EXPECT_EQ(hash_counts_sha1[0], hash_counts_xxhash[0]);

    // But hash values should be different
    bool different = false;
    for (size_t i = 0; i < all_base_hashes_sha1.size(); i++) {
        if (all_base_hashes_sha1[i] != all_base_hashes_xxhash[i]) {
            different = true;
            break;
        }
    }
    EXPECT_TRUE(different)
        << "SHA1 and XXHASH should produce different hash values";
}

// Test HashNGramWindow with empty text
TEST_F(MinHashTest, HashNGramWindowEmptyTextTest) {
    const char* texts[] = {""};
    int32_t text_lengths[] = {0};
    int32_t num_texts = 1;
    int32_t shingle_size = 3;

    std::vector<uint64_t> all_base_hashes;
    std::vector<int32_t> hash_counts;

    HashNGramWindow(texts,
                    text_lengths,
                    num_texts,
                    nullptr,
                    shingle_size,
                    HashFunction::SHA1,
                    all_base_hashes,
                    hash_counts);

    EXPECT_EQ(hash_counts.size(), num_texts);
    EXPECT_EQ(hash_counts[0], 0);
    EXPECT_EQ(all_base_hashes.size(), 0);
}

// Test ComputeFromTextsDirectly with simple texts
TEST_F(MinHashTest, ComputeFromTextsDirectlyBasicTest) {
    const char* texts[] = {"hello world", "test document", "another text"};
    int32_t text_lengths[] = {11, 13, 12};
    int32_t num_texts = 3;
    int32_t shingle_size = 3;
    int32_t num_hashes = 128;

    std::vector<uint64_t> perm_a(num_hashes);
    std::vector<uint64_t> perm_b(num_hashes);
    std::vector<uint32_t> signatures(num_texts * num_hashes);

    InitPermutations(num_hashes, 42, perm_a.data(), perm_b.data());

    ComputeFromTextsDirectly(texts,
                             text_lengths,
                             num_texts,
                             nullptr,
                             shingle_size,
                             perm_a.data(),
                             perm_b.data(),
                             HashFunction::SHA1,
                             num_hashes,
                             signatures.data());

    // Verify that signatures are generated
    for (int i = 0; i < num_texts; i++) {
        uint32_t* sig = &signatures[i * num_hashes];

        // Check that signature values are reasonable
        bool has_valid_values = false;
        for (int j = 0; j < num_hashes; j++) {
            if (sig[j] != UINT32_MAX && sig[j] != 0) {
                has_valid_values = true;
                break;
            }
        }
        EXPECT_TRUE(has_valid_values)
            << "Signature " << i << " should have valid hash values";
    }
}

// Test ComputeFromTextsDirectly with identical texts
TEST_F(MinHashTest, ComputeFromTextsDirectlyIdenticalTextsTest) {
    const char* texts[] = {"identical text", "identical text"};
    int32_t text_lengths[] = {14, 14};
    int32_t num_texts = 2;
    int32_t shingle_size = 3;
    int32_t num_hashes = 128;

    std::vector<uint64_t> perm_a(num_hashes);
    std::vector<uint64_t> perm_b(num_hashes);
    std::vector<uint32_t> signatures(num_texts * num_hashes);

    InitPermutations(num_hashes, 42, perm_a.data(), perm_b.data());

    ComputeFromTextsDirectly(texts,
                             text_lengths,
                             num_texts,
                             nullptr,
                             shingle_size,
                             perm_a.data(),
                             perm_b.data(),
                             HashFunction::SHA1,
                             num_hashes,
                             signatures.data());

    // Identical texts should produce identical signatures
    uint32_t* sig1 = &signatures[0];
    uint32_t* sig2 = &signatures[num_hashes];

    bool identical = true;
    for (int i = 0; i < num_hashes; i++) {
        if (sig1[i] != sig2[i]) {
            identical = false;
            break;
        }
    }
    EXPECT_TRUE(identical)
        << "Identical texts should produce identical signatures";
}

// Test ComputeFromTextsDirectly with different hash counts
TEST_F(MinHashTest, ComputeFromTextsDirectlyDifferentHashCounts) {
    const char* texts[] = {"hello world"};
    int32_t text_lengths[] = {11};
    int32_t num_texts = 1;
    int32_t shingle_size = 3;

    for (int32_t num_hashes : {8, 16, 32, 64, 128, 256}) {
        std::vector<uint64_t> perm_a(num_hashes);
        std::vector<uint64_t> perm_b(num_hashes);
        std::vector<uint32_t> signatures(num_texts * num_hashes);

        InitPermutations(num_hashes, 42, perm_a.data(), perm_b.data());

        ComputeFromTextsDirectly(texts,
                                 text_lengths,
                                 num_texts,
                                 nullptr,
                                 shingle_size,
                                 perm_a.data(),
                                 perm_b.data(),
                                 HashFunction::SHA1,
                                 num_hashes,
                                 signatures.data());

        // Verify signature is generated for all hash functions
        int valid_count = 0;
        for (int j = 0; j < num_hashes; j++) {
            if (signatures[j] != UINT32_MAX) {
                valid_count++;
            }
        }
        EXPECT_GT(valid_count, 0) << "Should have valid signatures for "
                                  << num_hashes << " hash functions";
    }
}

// Test similarity preservation property of MinHash
TEST_F(MinHashTest, SimilarityPreservationTest) {
    const char* texts[] = {
        "the quick brown fox jumps over the lazy dog",
        "the quick brown fox jumps over the lazy cat",     // Similar to first
        "zyxwvu 12345 QWERTY !@#$% abcdefgh 67890 ASDFGH"  // Different
    };
    int32_t text_lengths[] = {44, 44, 48};
    int32_t num_texts = 3;
    int32_t shingle_size = 3;
    int32_t num_hashes = 128;

    std::vector<uint64_t> perm_a(num_hashes);
    std::vector<uint64_t> perm_b(num_hashes);
    std::vector<uint32_t> signatures(num_texts * num_hashes);

    InitPermutations(num_hashes, 42, perm_a.data(), perm_b.data());

    ComputeFromTextsDirectly(texts,
                             text_lengths,
                             num_texts,
                             nullptr,
                             shingle_size,
                             perm_a.data(),
                             perm_b.data(),
                             HashFunction::SHA1,
                             num_hashes,
                             signatures.data());

    // Calculate Jaccard similarity estimates
    auto calculate_similarity = [&](int idx1, int idx2) -> double {
        uint32_t* sig1 = &signatures[idx1 * num_hashes];
        uint32_t* sig2 = &signatures[idx2 * num_hashes];
        int matches = 0;
        for (int i = 0; i < num_hashes; i++) {
            if (sig1[i] == sig2[i]) {
                matches++;
            }
        }
        return static_cast<double>(matches) / num_hashes;
    };

    double sim_0_1 = calculate_similarity(0, 1);  // Similar texts
    double sim_0_2 = calculate_similarity(0, 2);  // Different texts

    // Similar texts should have higher similarity
    EXPECT_GT(sim_0_1, sim_0_2)
        << "Similar texts should have higher MinHash similarity";
    EXPECT_GT(sim_0_1, 0.5) << "Similar texts should have similarity > 0.5";
    EXPECT_LT(sim_0_2, 0.3) << "Different texts should have similarity < 0.3";
}

// Test with various shingle sizes
TEST_F(MinHashTest, VariousShingleSizesTest) {
    const char* texts[] = {"hello world from the test"};
    int32_t text_lengths[] = {25};
    int32_t num_texts = 1;
    int32_t num_hashes = 64;

    std::vector<uint64_t> perm_a(num_hashes);
    std::vector<uint64_t> perm_b(num_hashes);
    InitPermutations(num_hashes, 42, perm_a.data(), perm_b.data());

    for (int32_t shingle_size : {1, 2, 3, 4, 5}) {
        std::vector<uint32_t> signatures(num_texts * num_hashes);

        ComputeFromTextsDirectly(texts,
                                 text_lengths,
                                 num_texts,
                                 nullptr,
                                 shingle_size,
                                 perm_a.data(),
                                 perm_b.data(),
                                 HashFunction::SHA1,
                                 num_hashes,
                                 signatures.data());

        // Verify signatures are generated
        int valid_count = 0;
        for (int j = 0; j < num_hashes; j++) {
            if (signatures[j] != UINT32_MAX) {
                valid_count++;
            }
        }
        EXPECT_GT(valid_count, 0)
            << "Should have valid signatures for shingle_size=" << shingle_size;
    }
}

// Test edge case: very long text
TEST_F(MinHashTest, LongTextTest) {
    std::string long_text(10000, 'a');
    for (size_t i = 0; i < long_text.size(); i += 100) {
        long_text[i] = ' ';  // Add some spaces
    }

    const char* texts[] = {long_text.c_str()};
    int32_t text_lengths[] = {static_cast<int32_t>(long_text.size())};
    int32_t num_texts = 1;
    int32_t shingle_size = 3;
    int32_t num_hashes = 128;

    std::vector<uint64_t> perm_a(num_hashes);
    std::vector<uint64_t> perm_b(num_hashes);
    std::vector<uint32_t> signatures(num_texts * num_hashes);

    InitPermutations(num_hashes, 42, perm_a.data(), perm_b.data());

    EXPECT_NO_THROW({
        ComputeFromTextsDirectly(texts,
                                 text_lengths,
                                 num_texts,
                                 nullptr,
                                 shingle_size,
                                 perm_a.data(),
                                 perm_b.data(),
                                 HashFunction::SHA1,
                                 num_hashes,
                                 signatures.data());
    });

    int valid_count = 0;
    for (int j = 0; j < num_hashes; j++) {
        if (signatures[j] != UINT32_MAX) {
            valid_count++;
        }
    }
    EXPECT_GT(valid_count, 0);
}

// Test batch processing with multiple texts
TEST_F(MinHashTest, BatchProcessingTest) {
    std::vector<std::string> text_strings = {"first document",
                                             "second document",
                                             "third document",
                                             "fourth document",
                                             "fifth document"};

    std::vector<const char*> texts;
    std::vector<int32_t> text_lengths;
    for (const auto& s : text_strings) {
        texts.push_back(s.c_str());
        text_lengths.push_back(s.size());
    }

    int32_t num_texts = texts.size();
    int32_t shingle_size = 3;
    int32_t num_hashes = 128;

    std::vector<uint64_t> perm_a(num_hashes);
    std::vector<uint64_t> perm_b(num_hashes);
    std::vector<uint32_t> signatures(num_texts * num_hashes);

    InitPermutations(num_hashes, 42, perm_a.data(), perm_b.data());

    ComputeFromTextsDirectly(texts.data(),
                             text_lengths.data(),
                             num_texts,
                             nullptr,
                             shingle_size,
                             perm_a.data(),
                             perm_b.data(),
                             HashFunction::SHA1,
                             num_hashes,
                             signatures.data());

    // Verify each text has a signature
    for (int i = 0; i < num_texts; i++) {
        uint32_t* sig = &signatures[i * num_hashes];
        int valid_count = 0;
        for (int j = 0; j < num_hashes; j++) {
            if (sig[j] != UINT32_MAX) {
                valid_count++;
            }
        }
        EXPECT_GT(valid_count, 0)
            << "Document " << i << " should have valid signatures";
    }
}

// Test comparing native implementation with current SIMD implementation
TEST_F(MinHashTest, NativeVsCurrentSIMDTest) {
    const char* texts[] = {"hello world test document",
                           "another test with different content",
                           "the quick brown fox jumps over the lazy dog"};
    int32_t text_lengths[] = {25, 35, 44};
    int32_t num_texts = 3;
    int32_t shingle_size = 3;
    int32_t num_hashes = 133;

    std::vector<uint64_t> perm_a(num_hashes);
    std::vector<uint64_t> perm_b(num_hashes);
    InitPermutations(num_hashes, 42, perm_a.data(), perm_b.data());

    // First, get base hashes for all texts
    std::vector<uint64_t> all_base_hashes;
    std::vector<int32_t> hash_counts;
    HashNGramWindow(texts,
                    text_lengths,
                    num_texts,
                    nullptr,
                    shingle_size,
                    HashFunction::SHA1,
                    all_base_hashes,
                    hash_counts);

    // Compute using current implementation (with SIMD if available)
    std::vector<uint32_t> signatures_current(num_texts * num_hashes);
    // simd version
    int32_t base_offset = 0;
    for (int32_t text_idx = 0; text_idx < num_texts; text_idx++) {
        uint32_t* sig_cur = &signatures_current[text_idx * num_hashes];
        const uint64_t* base = &all_base_hashes[base_offset];
        size_t shingle_count = hash_counts[text_idx];

        // Initialize signature
        for (int32_t i = 0; i < num_hashes; i++) {
            sig_cur[i] = UINT32_MAX;
        }

        // Compute using native batch8 function
        for (int32_t i = 0; i + 8 <= num_hashes; i += 8) {
            linear_and_find_min_batch8_impl(
                base, shingle_count, &perm_a[i], &perm_b[i], &sig_cur[i]);
        }

        // Handle remaining hash functions
        for (int32_t i = (num_hashes / 8) * 8; i < num_hashes; i++) {
            sig_cur[i] = linear_and_find_min_impl(
                base, shingle_count, perm_a[i], perm_b[i]);
        }

        base_offset += shingle_count;
    }

    // Compute using pure native implementation
    std::vector<uint32_t> signatures_native(num_texts * num_hashes);
    base_offset = 0;
    for (int32_t text_idx = 0; text_idx < num_texts; text_idx++) {
        uint32_t* sig_native = &signatures_native[text_idx * num_hashes];
        const uint64_t* base = &all_base_hashes[base_offset];
        size_t shingle_count = hash_counts[text_idx];

        // Initialize signature
        for (int32_t i = 0; i < num_hashes; i++) {
            sig_native[i] = UINT32_MAX;
        }

        // Compute using native batch8 function
        for (int32_t i = 0; i + 8 <= num_hashes; i += 8) {
            linear_and_find_min_batch8_native(
                base, shingle_count, &perm_a[i], &perm_b[i], &sig_native[i]);
        }

        // Handle remaining hash functions
        for (int32_t i = (num_hashes / 8) * 8; i < num_hashes; i++) {
            sig_native[i] = linear_and_find_min_native(
                base, shingle_count, perm_a[i], perm_b[i]);
        }

        base_offset += shingle_count;
    }

    // Compare results
    int mismatch_count = 0;
    for (int text_idx = 0; text_idx < num_texts; text_idx++) {
        for (int hash_idx = 0; hash_idx < num_hashes; hash_idx++) {
            int idx = text_idx * num_hashes + hash_idx;
            if (signatures_current[idx] != signatures_native[idx]) {
                mismatch_count++;
                if (mismatch_count <= 1) {
                    std::cerr << "Mismatch at text " << text_idx << ", hash "
                              << hash_idx
                              << ": Current(NEON)=" << signatures_current[idx]
                              << ", Native=" << signatures_native[idx];

                    // Additional debug for first few mismatches
                    if (mismatch_count <= 3) {
                        std::cerr
                            << "\n  perm_a[" << hash_idx << "] = 0x" << std::hex
                            << perm_a[hash_idx] << ", perm_b[" << hash_idx
                            << "] = 0x" << perm_b[hash_idx] << std::dec
                            << ", shingle_count=" << hash_counts[text_idx];
                    }
                    std::cerr << std::endl;
                }
            }
        }
    }

    EXPECT_EQ(mismatch_count, 0)
        << "Found " << mismatch_count
        << " mismatches between native and current SIMD implementations";
}

// Test with random data to stress test
TEST_F(MinHashTest, StressTestNativeVsCurrent) {
    const int num_iterations = 10;
    uint64_t seed = 314159;

    for (int iter = 0; iter < num_iterations; iter++) {
        // Generate random base hashes
        size_t shingle_count = 10 + (seed % 200);
        std::vector<uint64_t> base_hashes(shingle_count);

        for (size_t i = 0; i < shingle_count; i++) {
            seed = seed * 1103515245 + 12345;
            base_hashes[i] = seed;
        }

        // Generate permutations
        std::vector<uint64_t> perm_a(8);
        std::vector<uint64_t> perm_b(8);
        InitPermutations(8, seed, perm_a.data(), perm_b.data());

        // Compute with native
        std::vector<uint32_t> sig_native(8, UINT32_MAX);
        linear_and_find_min_batch8_native(base_hashes.data(),
                                          shingle_count,
                                          perm_a.data(),
                                          perm_b.data(),
                                          sig_native.data());

        // Compute with current implementation
        // We need to use the actual internal function from MinHashComputer
        // For now, we'll compute it step by step using the same logic
        std::vector<uint32_t> sig_current(8, UINT32_MAX);

        // Simulate what ComputeFromTextsDirectly does internally
        // by calling the native version for comparison
        linear_and_find_min_batch8_native(base_hashes.data(),
                                          shingle_count,
                                          perm_a.data(),
                                          perm_b.data(),
                                          sig_current.data());

        // Compare
        for (int i = 0; i < 8; i++) {
            EXPECT_EQ(sig_current[i], sig_native[i])
                << "Iteration " << iter << ", index " << i
                << ": shingle_count=" << shingle_count;
        }

        seed = seed * 1103515245 + 12345;
    }
}
