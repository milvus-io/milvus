// Copyright (C) 2019-2025 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>

#include <cstdint>
#include <random>
#include <vector>

#include "xxhash.h"
#include <openssl/sha.h>

namespace py = pybind11;

// Constants matching MinHashComputer.cpp
constexpr uint64_t MERSENNE_PRIME = 0x1FFFFFFFFFFFFFFFULL;  // 2^61 - 1
constexpr uint64_t MAX_HASH_MASK = 0xFFFFFFFFULL;           // 2^32 - 1
constexpr uint32_t MAX_HASH_32 = 0xFFFFFFFF;

// Hash function implementations
uint32_t hash_xxhash(const char* data, size_t len) {
    return static_cast<uint32_t>(XXH3_64bits(data, len));
}

uint32_t hash_sha1(const char* data, size_t len) {
    unsigned char digest[SHA_DIGEST_LENGTH];
    SHA1(reinterpret_cast<const unsigned char*>(data), len, digest);
    return digest[0] | (digest[1] << 8) | (digest[2] << 16) | (digest[3] << 24);
}

// InitPermutations - exactly matching Milvus implementation
py::tuple init_permutations(int32_t num_hashes, uint64_t seed) {
    std::mt19937_64 rng(seed);

    py::array_t<uint64_t> perm_a(num_hashes);
    py::array_t<uint64_t> perm_b(num_hashes);

    auto a_ptr = perm_a.mutable_data();
    auto b_ptr = perm_b.mutable_data();

    for (int32_t i = 0; i < num_hashes; i++) {
        uint64_t raw_a = rng();
        uint64_t raw_b = rng();
        a_ptr[i] = (raw_a % (MERSENNE_PRIME - 1)) + 1;
        b_ptr[i] = raw_b % MERSENNE_PRIME;
    }

    return py::make_tuple(perm_a, perm_b);
}

// Hash shingles for char-level tokenization
py::array_t<uint64_t> hash_shingles_char(
    const std::string& text,
    int32_t shingle_size,
    bool use_sha1 = false
) {
    auto hash_func = use_sha1 ? hash_sha1 : hash_xxhash;

    std::vector<uint64_t> hashes;

    if (static_cast<int32_t>(text.size()) < shingle_size) {
        hashes.push_back(hash_func(text.data(), text.size()));
    } else {
        int32_t num_shingles = text.size() - shingle_size + 1;
        hashes.reserve(num_shingles);
        for (int32_t i = 0; i < num_shingles; i++) {
            hashes.push_back(hash_func(text.data() + i, shingle_size));
        }
    }

    py::array_t<uint64_t> result(hashes.size());
    std::copy(hashes.begin(), hashes.end(), result.mutable_data());
    return result;
}

// Hash shingles for word-level tokenization (simple whitespace split)
py::array_t<uint64_t> hash_shingles_word(
    const std::string& text,
    int32_t shingle_size,
    bool use_sha1 = false
) {
    auto hash_func = use_sha1 ? hash_sha1 : hash_xxhash;

    // Simple whitespace tokenization
    std::vector<std::string> tokens;
    std::string token;
    for (char c : text) {
        if (std::isspace(c)) {
            if (!token.empty()) {
                tokens.push_back(token);
                token.clear();
            }
        } else {
            token += c;
        }
    }
    if (!token.empty()) {
        tokens.push_back(token);
    }

    std::vector<uint64_t> hashes;

    if (static_cast<int32_t>(tokens.size()) < shingle_size) {
        // Concatenate all tokens
        std::string combined;
        for (const auto& t : tokens) {
            combined += t;
        }
        if (!combined.empty()) {
            hashes.push_back(hash_func(combined.data(), combined.size()));
        }
    } else {
        int32_t num_shingles = tokens.size() - shingle_size + 1;
        hashes.reserve(num_shingles);
        for (int32_t i = 0; i < num_shingles; i++) {
            std::string shingle;
            for (int32_t j = 0; j < shingle_size; j++) {
                shingle += tokens[i + j];
            }
            hashes.push_back(hash_func(shingle.data(), shingle.size()));
        }
    }

    py::array_t<uint64_t> result(hashes.size());
    std::copy(hashes.begin(), hashes.end(), result.mutable_data());
    return result;
}

// Compute MinHash signature from base hashes - exactly matching Milvus
py::array_t<uint32_t> compute_signature(
    py::array_t<uint64_t> base_hashes,
    py::array_t<uint64_t> perm_a,
    py::array_t<uint64_t> perm_b
) {
    auto base_ptr = base_hashes.data();
    auto a_ptr = perm_a.data();
    auto b_ptr = perm_b.data();

    size_t num_base = base_hashes.size();
    size_t num_hashes = perm_a.size();

    py::array_t<uint32_t> signature(num_hashes);
    auto sig_ptr = signature.mutable_data();

    // Initialize with max value
    for (size_t i = 0; i < num_hashes; i++) {
        sig_ptr[i] = MAX_HASH_32;
    }

    // Compute MinHash - exactly matching Milvus linear_and_find_min_native
    for (size_t j = 0; j < num_base; j++) {
        uint64_t base = base_ptr[j];
        for (size_t i = 0; i < num_hashes; i++) {
            // This matches C++ uint64_t overflow behavior
            uint64_t temp = a_ptr[i] * base + b_ptr[i];

            // Fast Mersenne modulo
            uint64_t low = temp & MERSENNE_PRIME;
            uint64_t high = temp >> 61;
            temp = low + high;
            if (temp >= MERSENNE_PRIME) {
                temp -= MERSENNE_PRIME;
            }

            uint32_t permuted_hash = static_cast<uint32_t>(temp & MAX_HASH_MASK);
            if (permuted_hash < sig_ptr[i]) {
                sig_ptr[i] = permuted_hash;
            }
        }
    }

    return signature;
}

// High-level API: compute MinHash signature from text
py::array_t<uint32_t> compute_minhash(
    const std::string& text,
    int32_t num_hashes,
    int32_t shingle_size,
    uint64_t seed,
    bool use_char_level = true,
    bool use_sha1 = false
) {
    // Generate permutations
    auto perms = init_permutations(num_hashes, seed);
    auto perm_a = perms[0].cast<py::array_t<uint64_t>>();
    auto perm_b = perms[1].cast<py::array_t<uint64_t>>();

    // Hash shingles
    py::array_t<uint64_t> base_hashes;
    if (use_char_level) {
        base_hashes = hash_shingles_char(text, shingle_size, use_sha1);
    } else {
        base_hashes = hash_shingles_word(text, shingle_size, use_sha1);
    }

    // Compute signature
    return compute_signature(base_hashes, perm_a, perm_b);
}

PYBIND11_MODULE(milvus_minhash, m) {
    m.doc() = "Milvus MinHash C++ binding for Python testing";

    m.def("init_permutations", &init_permutations,
          "Generate permutation parameters (perm_a, perm_b)",
          py::arg("num_hashes"), py::arg("seed"));

    m.def("hash_shingles_char", &hash_shingles_char,
          "Hash character-level shingles",
          py::arg("text"), py::arg("shingle_size"), py::arg("use_sha1") = false);

    m.def("hash_shingles_word", &hash_shingles_word,
          "Hash word-level shingles (simple whitespace tokenization)",
          py::arg("text"), py::arg("shingle_size"), py::arg("use_sha1") = false);

    m.def("compute_signature", &compute_signature,
          "Compute MinHash signature from base hashes",
          py::arg("base_hashes"), py::arg("perm_a"), py::arg("perm_b"));

    m.def("compute_minhash", &compute_minhash,
          "Compute MinHash signature from text (high-level API)",
          py::arg("text"),
          py::arg("num_hashes"),
          py::arg("shingle_size"),
          py::arg("seed"),
          py::arg("use_char_level") = true,
          py::arg("use_sha1") = false);

    // Export constants
    m.attr("MERSENNE_PRIME") = MERSENNE_PRIME;
    m.attr("MAX_HASH_MASK") = MAX_HASH_MASK;
}
