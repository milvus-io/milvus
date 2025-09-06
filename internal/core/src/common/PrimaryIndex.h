// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include "BooPHF.h"
#include <vector>
#include <string>
#include <memory>
#include <unordered_map>
#include <limits>
#include <cstring>
#include <fstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <cassert>

namespace primaryIndex {

template <typename T, typename Hasher_t>
class PrimaryIndex;

class BitPackedArray {
 private:
    std::vector<uint64_t> data_;
    uint32_t bits_per_value_;
    uint32_t values_per_word_;
    uint32_t size_;
    uint64_t mask_;

 public:
    BitPackedArray()
        : bits_per_value_(0), values_per_word_(0), size_(0), mask_(0) {
    }

    void
    init(uint64_t max_value, uint32_t num_values) {
        size_ = num_values;

        bits_per_value_ = 1;
        uint64_t temp = max_value;
        while (temp >>= 1) bits_per_value_++;

        values_per_word_ = 64 / bits_per_value_;

        mask_ = (1ULL << bits_per_value_) - 1;

        uint32_t num_words =
            (num_values + values_per_word_ - 1) / values_per_word_;
        data_.resize(num_words, 0);
    }

    void
    set(uint32_t index, uint64_t value) {
        if (index >= size_)
            return;

        uint32_t word_index = index / values_per_word_;
        uint32_t bit_offset = (index % values_per_word_) * bits_per_value_;

        data_[word_index] &= ~(mask_ << bit_offset);
        data_[word_index] |= (value & mask_) << bit_offset;
    }

    uint64_t
    get(uint32_t index) const {
        if (index >= size_)
            return 0;

        uint32_t word_index = index / values_per_word_;
        uint32_t bit_offset = (index % values_per_word_) * bits_per_value_;

        return (data_[word_index] >> bit_offset) & mask_;
    }

    uint32_t
    size() const {
        return size_;
    }

    void
    serialize(std::ofstream& out) const {
        out.write(reinterpret_cast<const char*>(&bits_per_value_),
                  sizeof(bits_per_value_));
        out.write(reinterpret_cast<const char*>(&values_per_word_),
                  sizeof(values_per_word_));
        out.write(reinterpret_cast<const char*>(&size_), sizeof(size_));
        out.write(reinterpret_cast<const char*>(&mask_), sizeof(mask_));

        uint32_t data_size = data_.size();
        out.write(reinterpret_cast<const char*>(&data_size), sizeof(data_size));
        out.write(reinterpret_cast<const char*>(data_.data()),
                  data_size * sizeof(uint64_t));
    }

    void
    deserialize(std::ifstream& in) {
        in.read(reinterpret_cast<char*>(&bits_per_value_),
                sizeof(bits_per_value_));
        in.read(reinterpret_cast<char*>(&values_per_word_),
                sizeof(values_per_word_));
        in.read(reinterpret_cast<char*>(&size_), sizeof(size_));
        in.read(reinterpret_cast<char*>(&mask_), sizeof(mask_));

        uint32_t data_size;
        in.read(reinterpret_cast<char*>(&data_size), sizeof(data_size));
        data_.resize(data_size);
        in.read(reinterpret_cast<char*>(data_.data()),
                data_size * sizeof(uint64_t));
    }

    void
    deserialize_from_mmap(const char* data, size_t& offset) {
        memcpy(&bits_per_value_, data + offset, sizeof(bits_per_value_));
        offset += sizeof(bits_per_value_);
        memcpy(&values_per_word_, data + offset, sizeof(values_per_word_));
        offset += sizeof(values_per_word_);
        memcpy(&size_, data + offset, sizeof(size_));
        offset += sizeof(size_);
        memcpy(&mask_, data + offset, sizeof(mask_));
        offset += sizeof(mask_);

        uint32_t data_size;
        memcpy(&data_size, data + offset, sizeof(data_size));
        offset += sizeof(data_size);

        data_.resize(data_size);
        memcpy(data_.data(), data + offset, data_size * sizeof(uint64_t));
        offset += data_size * sizeof(uint64_t);
    }
};

class StringHasher {
 public:
    typedef std::string Item;
    typedef std::pair<uint64_t, uint64_t> hash_pair_t;

 public:
    hash_pair_t
    operator()(const Item& key) const {
        hash_pair_t result;
        result.first = murmurHash3_64(key, 0xAAAAAAAA55555555ULL);
        result.second = murmurHash3_64(key, 0x33333333CCCCCCCCULL);
        return result;
    }

 private:
    uint64_t
    murmurHash3_64(const std::string& key, uint64_t seed) const {
        const uint64_t c1 = 0x87c37b91114253d5ULL;
        const uint64_t c2 = 0x4cf5ad432745937fULL;
        const uint8_t* data = reinterpret_cast<const uint8_t*>(key.c_str());
        const size_t len = key.length();
        const size_t nblocks = len / 16;

        uint64_t h1 = seed;
        uint64_t h2 = seed;

        // Body
        for (size_t i = 0; i < nblocks; i++) {
            uint64_t k1 = *reinterpret_cast<const uint64_t*>(data + i * 16);
            uint64_t k2 = *reinterpret_cast<const uint64_t*>(data + i * 16 + 8);

            k1 *= c1;
            k1 = (k1 << 31) | (k1 >> 33);
            k1 *= c2;
            h1 ^= k1;

            h1 = (h1 << 27) | (h1 >> 37);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;

            k2 *= c2;
            k2 = (k2 << 33) | (k2 >> 31);
            k2 *= c1;
            h2 ^= k2;

            h2 = (h2 << 31) | (h2 >> 33);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }

        // Tail
        const uint8_t* tail = data + nblocks * 16;
        uint64_t k1 = 0;
        uint64_t k2 = 0;

        switch (len & 15) {
            case 15:
                k2 ^= static_cast<uint64_t>(tail[14]) << 48;
            case 14:
                k2 ^= static_cast<uint64_t>(tail[13]) << 40;
            case 13:
                k2 ^= static_cast<uint64_t>(tail[12]) << 32;
            case 12:
                k2 ^= static_cast<uint64_t>(tail[11]) << 24;
            case 11:
                k2 ^= static_cast<uint64_t>(tail[10]) << 16;
            case 10:
                k2 ^= static_cast<uint64_t>(tail[9]) << 8;
            case 9:
                k2 ^= static_cast<uint64_t>(tail[8]);
                k2 *= c2;
                k2 = (k2 << 33) | (k2 >> 31);
                k2 *= c1;
                h2 ^= k2;
            case 8:
                k1 ^= static_cast<uint64_t>(tail[7]) << 56;
            case 7:
                k1 ^= static_cast<uint64_t>(tail[6]) << 48;
            case 6:
                k1 ^= static_cast<uint64_t>(tail[5]) << 40;
            case 5:
                k1 ^= static_cast<uint64_t>(tail[4]) << 32;
            case 4:
                k1 ^= static_cast<uint64_t>(tail[3]) << 24;
            case 3:
                k1 ^= static_cast<uint64_t>(tail[2]) << 16;
            case 2:
                k1 ^= static_cast<uint64_t>(tail[1]) << 8;
            case 1:
                k1 ^= static_cast<uint64_t>(tail[0]);
                k1 *= c1;
                k1 = (k1 << 31) | (k1 >> 33);
                k1 *= c2;
                h1 ^= k1;
        }

        // Finalization
        h1 ^= len;
        h2 ^= len;

        h1 += h2;
        h2 += h1;

        h1 = fmix64(h1);
        h2 = fmix64(h2);

        h1 += h2;
        h2 += h1;

        return h1;
    }

    uint64_t
    fmix64(uint64_t k) const {
        k ^= k >> 33;
        k *= 0xff51afd7ed558ccdULL;
        k ^= k >> 33;
        k *= 0xc4ceb9fe1a85ec53ULL;
        k ^= k >> 33;
        return k;
    }
};

template <typename Item>
class SingleHashFunctor {
    typedef std::pair<uint64_t, uint64_t> hash_pair_t;

 public:
    hash_pair_t
    operator()(const Item& key) const {
        hash_pair_t result;
        result.first = singleHasher(key, 0xAAAAAAAA55555555ULL);
        ;
        result.second = singleHasher(key, 0x33333333CCCCCCCCULL);
        ;

        return result;
    }

    uint64_t
    singleHasher(const Item& key, uint64_t seed) const {
        uint64_t hash = seed;
        hash ^= (hash << 7) ^ key * (hash >> 3) ^
                (~((hash << 11) + (key ^ (hash >> 5))));
        hash = (~hash) + (hash << 21);
        hash = hash ^ (hash >> 24);
        hash = (hash + (hash << 3)) + (hash << 8);
        hash = hash ^ (hash >> 14);
        hash = (hash + (hash << 2)) + (hash << 4);
        hash = hash ^ (hash >> 28);
        hash = hash + (hash << 31);
        return hash;
    }
};

template <typename T>
struct HashSelector {
    using type = StringHasher;
};

template <>
struct HashSelector<int64_t> {
    using type = SingleHashFunctor<int64_t>;
};

template <>
struct HashSelector<std::string> {
    using type = StringHasher;
};

template <typename T>
using SelectedHasher = typename HashSelector<T>::type;

template <typename T, typename Hasher_t = SelectedHasher<T>>
class PrimaryIndex {
 private:
    boomphf::mphf<Hasher_t> bbhash_;

    std::vector<int64_t> segmentids_list_;

    BitPackedArray value_array_;
    std::vector<uint64_t> hash_array_;
    double gamma_factor_;
    int num_threads_;
    bool built_;

 public:
    PrimaryIndex(double gamma = 10.0, int threads = 8)
        : gamma_factor_(gamma), num_threads_(threads), built_(false) {
    }

    ~PrimaryIndex() = default;

    template <typename SegmentContainer>
    void
    build(const SegmentContainer& segments) {
        if (built_) {
            throw std::runtime_error("PrimaryIndex already built");
        }

        segmentids_list_.reserve(segments.size());
        for (const auto& segment : segments) {
            segmentids_list_.push_back(segment.segment_id);
        }

        std::vector<T> all_keys;
        std::vector<uint64_t> key_to_segment_index;

        for (size_t segment_idx = 0; segment_idx < segments.size();
             ++segment_idx) {
            const auto& segment = segments[segment_idx];
            for (const auto& key : segment.keys) {
                all_keys.push_back(key);
                key_to_segment_index.push_back(segment_idx);
            }
        }

        bbhash_ = boomphf::mphf<Hasher_t>(
            all_keys.size(), all_keys, num_threads_, gamma_factor_);

        uint64_t max_segment_index = segments.size() - 1;
        value_array_.init(max_segment_index, all_keys.size());

        hash_array_.resize(all_keys.size());

        for (size_t i = 0; i < all_keys.size(); ++i) {
            auto [idx, hash] = bbhash_.lookup1(all_keys[i]);
            hash_array_[idx] = hash;
            if (idx != std::numeric_limits<uint64_t>::max()) {
                value_array_.set(idx, key_to_segment_index[i]);
            }
        }

        built_ = true;
    }

    int64_t
    lookup(T key) {
        if (!built_) {
            return -1;
        }

        std::pair<uint64_t, uint64_t> result = bbhash_.lookup1(key);
        uint64_t idx = result.first;
        if (idx != std::numeric_limits<uint64_t>::max() &&
            idx < value_array_.size() && hash_array_[idx] == result.second) {
            uint64_t segment_index = value_array_.get(idx);

            if (segment_index < segmentids_list_.size()) {
                return static_cast<int64_t>(segmentids_list_[segment_index]);
            }
        }

        return -1;
    }

    const boomphf::mphf<Hasher_t>*
    get_bbhash() const {
        return &bbhash_;
    }

    void
    reset_segment_id(uint64_t to_segment_id, uint64_t from_segment_id) {
        for (size_t i = 0; i < segmentids_list_.size(); i++) {
            if (segmentids_list_[i] == from_segment_id) {
                segmentids_list_[i] = to_segment_id;
            }
        }
    }

    std::vector<int64_t>
    get_segment_list() const {
        return segmentids_list_;
    }

    double
    calculate_segmentid_invalid_percentage() const {
        if (segmentids_list_.empty()) {
            return 0.0;
        }

        uint64_t invalid_count = 0;
        for (size_t i = 0; i < segmentids_list_.size(); i++) {
            if (segmentids_list_[i] == -1) {
                invalid_count++;
            }
        }
        return (invalid_count * 100.0) / segmentids_list_.size();
    }

    void
    save_to_file(const std::string& filename) const {
        if (!built_) {
            throw std::runtime_error("PrimaryIndex not built");
        }

        std::ofstream out(filename, std::ios::binary);
        if (!out) {
            throw std::runtime_error("Cannot open file for writing: " +
                                     filename);
        }

        uint32_t magic = 0x50494E44;  // "PIND"
        out.write(reinterpret_cast<const char*>(&magic), sizeof(magic));

        out.write(reinterpret_cast<const char*>(&gamma_factor_),
                  sizeof(gamma_factor_));
        out.write(reinterpret_cast<const char*>(&num_threads_),
                  sizeof(num_threads_));

        bbhash_.save(out);

        uint32_t segment_count = segmentids_list_.size();
        out.write(reinterpret_cast<const char*>(&segment_count),
                  sizeof(segment_count));
        out.write(reinterpret_cast<const char*>(segmentids_list_.data()),
                  segment_count * sizeof(uint64_t));

        uint32_t hash_count = hash_array_.size();
        out.write(reinterpret_cast<const char*>(&hash_count),
                  sizeof(hash_count));
        out.write(reinterpret_cast<const char*>(hash_array_.data()),
                  hash_count * sizeof(uint64_t));

        value_array_.serialize(out);
    }

    void
    load_from_file(const std::string& filename) {
        std::ifstream in(filename, std::ios::binary);
        if (!in) {
            throw std::runtime_error("Cannot open file for reading: " +
                                     filename);
        }

        uint32_t magic;
        in.read(reinterpret_cast<char*>(&magic), sizeof(magic));
        if (magic != 0x50494E44) {
            throw std::runtime_error("Invalid file format");
        }

        in.read(reinterpret_cast<char*>(&gamma_factor_), sizeof(gamma_factor_));
        in.read(reinterpret_cast<char*>(&num_threads_), sizeof(num_threads_));

        try {
            bbhash_.load(in);
        } catch (const std::exception& e) {
            throw std::runtime_error("Failed to load BBHash: " +
                                     std::string(e.what()));
        }

        uint32_t segment_count;
        in.read(reinterpret_cast<char*>(&segment_count), sizeof(segment_count));
        segmentids_list_.resize(segment_count);
        in.read(reinterpret_cast<char*>(segmentids_list_.data()),
                segment_count * sizeof(uint64_t));

        uint32_t hash_count;
        in.read(reinterpret_cast<char*>(&hash_count), sizeof(hash_count));
        hash_array_.resize(hash_count);
        in.read(reinterpret_cast<char*>(hash_array_.data()),
                hash_count * sizeof(uint64_t));

        value_array_.deserialize(in);

        built_ = true;
    }

    bool
    load_from_mmap(const std::string& filename) {
        std::ifstream in(filename, std::ios::binary);
        if (!in)
            return false;

        uint32_t magic;
        in.read(reinterpret_cast<char*>(&magic), sizeof(magic));
        if (magic != 0x50494E44)
            return false;

        in.read(reinterpret_cast<char*>(&gamma_factor_), sizeof(gamma_factor_));
        in.read(reinterpret_cast<char*>(&num_threads_), sizeof(num_threads_));

        bbhash_.load(in);

        uint32_t segment_count;
        in.read(reinterpret_cast<char*>(&segment_count), sizeof(segment_count));
        segmentids_list_.resize(segment_count);
        in.read(reinterpret_cast<char*>(segmentids_list_.data()),
                segment_count * sizeof(uint64_t));

        std::streamoff hash_array_offset = in.tellg();
        uint32_t hash_count;
        in.read(reinterpret_cast<char*>(&hash_count), sizeof(hash_count));

        int fd = open(filename.c_str(), O_RDONLY);
        if (fd == -1)
            return false;
        struct stat st;
        if (fstat(fd, &st) == -1) {
            close(fd);
            return false;
        }
        size_t file_size = st.st_size;
        void* mmap_ptr =
            mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (mmap_ptr == MAP_FAILED) {
            close(fd);
            return false;
        }
        const char* data = static_cast<const char*>(mmap_ptr);
        size_t hash_array_data_offset =
            static_cast<size_t>(hash_array_offset) + sizeof(uint32_t);
        hash_array_.resize(hash_count);
        memcpy(hash_array_.data(),
               data + hash_array_data_offset,
               hash_count * sizeof(uint64_t));
        munmap(mmap_ptr, file_size);
        close(fd);

        in.seekg(hash_array_offset + sizeof(uint32_t) +
                 hash_count * sizeof(uint64_t));
        value_array_.deserialize(in);

        built_ = true;
        return true;
    }
};

}  // namespace primaryIndex