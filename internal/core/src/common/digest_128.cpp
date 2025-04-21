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

#pragma once

#include <array>
#include <iostream>
#include <cstdint>
#include <cstddef>
#include <iomanip>

#include "common/digest_128.h"

#define BLOCK_SIZE 16
#define C1_128 0x87c37b91114253d5
#define C2_128 0x4cf5ad432745937f
#define TO_UINT64(c) (static_cast<uint64_t>(static_cast<unsigned char>(c)))

uint64_t
fmix64(uint64_t k) {
    k ^= k >> 33;
	k *= 0xff51afd7ed558ccd;
	k ^= k >> 33;
	k *= 0xc4ceb9fe1a85ec53;
	k ^= k >> 33;
	return k;
}

void
Digest128::bmix(const char *data, const size_t size){
    auto nblock = size/ BLOCK_SIZE;
    for (size_t i = 0; i < nblock; i++){
    	auto t = reinterpret_cast<const uint64_t*>(data + i * BLOCK_SIZE, data+(i+1)*BLOCK_SIZE);
        uint64_t k1 = t[0];
        uint64_t k2 = t[1];
        this->bmix_words(k1, k2);
    }
}

void
Digest128::bmix_words(uint64_t k1, uint64_t k2){
    auto h1 =this->h1,h2 = this->h2;

    k1 *= C1_128;
    k1 = (k1 << 31) | (k1 >> 33);
    k1 *= C2_128;
    h1 ^= k1;

    h1 = (h1 << 27) | (h1 >> 37);
    h1 += h2;
    h1 = h1 * 5 + 0x52dce729;
    
    k2 *= C2_128;
    k2 = (k2 << 33) | (k2 >> 31);
    k2 *= C1_128;
    h2 ^= k2;

    h2 = (h2 << 31) | (h2 >> 33);
    h2 += h1;
    h2 = h2 * 5 + 0x38495ab5;
    this->h1 = h1;
    this->h2 = h2;
}

std::array<uint64_t, 2>
Digest128::sum128(const char* data, const size_t size,const size_t length, const bool pad_tail){
    auto h1 = this->h1, h2 = this->h2;
    uint64_t k1=0, k2=0;

    if (pad_tail){
        switch ((size+1) & 15) {
            case 15:
                k2 ^= (static_cast<uint64_t>(1) << 48);
                break;
            case 14:
                k2 ^= (static_cast<uint64_t>(1) << 40);
                break;
            case 13:
                k2 ^= (static_cast<uint64_t>(1) << 32);
                break;
            case 12:
                k2 ^= (static_cast<uint64_t>(1) << 24);
                break;
            case 11:
                k2 ^= (static_cast<uint64_t>(1) << 16);
                break;
            case 10:
                k2 ^= (static_cast<uint64_t>(1) << 8);
                break;
            case 9:
                k2 ^= (static_cast<uint64_t>(1) << 0);
                k2 *= C2_128;
                k2 = (k2 << 33) | (k2 >> 31);
                k2 *= C1_128;
                h2 ^= k2;
                break;
            case 8:
                k1 ^= (static_cast<uint64_t>(1) << 56);
                break;
            case 7:
                k1 ^= (static_cast<uint64_t>(1) << 48);
                break;
            case 6:
                k1 ^= (static_cast<uint64_t>(1) << 40);
                break;
            case 5:
                k1 ^= (static_cast<uint64_t>(1) << 32);
                break;
            case 4:
                k1 ^= (static_cast<uint64_t>(1) << 24);
                break;
            case 3:
                k1 ^= (static_cast<uint64_t>(1) << 16);
                break;
            case 2:
                k1 ^= (static_cast<uint64_t>(1) << 8);
                break;
            case 1:
                k1 ^= (static_cast<uint64_t>(1) << 0);
                k1 *= C1_128;
                k1 = (k1 << 31) | (k1 >> 33);
                k1 *= C2_128;
                h1 ^= k1;
        }
    }

    switch (size & 15) {
        case 15:
            k2 ^= (TO_UINT64((data[14])) << 48);
            [[fallthrough]];
        case 14:
            k2 ^= (TO_UINT64(data[13]) << 40);
            [[fallthrough]];
        case 13:
            k2 ^= (TO_UINT64(data[12]) << 32);
            [[fallthrough]];
        case 12:
            k2 ^= (TO_UINT64(data[11]) << 24);
            [[fallthrough]];
        case 11:
            k2 ^= (TO_UINT64(data[10]) << 16);
            [[fallthrough]];
        case 10:
            k2 ^= (TO_UINT64(data[9]) << 8);
            [[fallthrough]];
        case 9:
            k2 ^= (TO_UINT64(data[8]) << 0);
            k2 *= C2_128;
            k2 = (k2 << 33) | (k2 >> 31);
            k2 *= C1_128;
            h2 ^= k2;
            [[fallthrough]];
        
        case 8:
            k1 ^= (TO_UINT64(data[7]) << 56);
            [[fallthrough]];
        case 7:
            k1 ^= (TO_UINT64(data[6]) << 48);
            [[fallthrough]];
        case 6:
            k1 ^= (TO_UINT64(data[5]) << 40);
            [[fallthrough]];
        case 5:
            k1 ^= (TO_UINT64(data[4]) << 32);
            [[fallthrough]];
        case 4:
            k1 ^= (TO_UINT64(data[3]) << 24);
            [[fallthrough]];
        case 3:
            k1 ^= (TO_UINT64(data[2]) << 16);
            [[fallthrough]];
        case 2:
            k1 ^= (TO_UINT64(data[1]) << 8);
            [[fallthrough]];
        case 1:
            k1 ^= ((TO_UINT64(data[0])) << 0);
            k1 *= C1_128;
            k1 = (k1 << 31) | (k1 >> 33);
            k1 *= C2_128;
            h1 ^= k1;
    }

    h1 ^= length;
    h2 ^= length;

    h1 += h2;
    h2 += h1;

    h1 = fmix64(h1);
    h2 = fmix64(h2);

    h1+=h2;
    h2+=h1;

    return {h1, h2};
}

std::array<uint64_t, 4>
Digest128::sum256(const char* data, const size_t size){
	this ->h1=0, this -> h2 = 0;
    bmix(data,  size);

    auto tail_length = size % BLOCK_SIZE;
    auto tail = data + (size - tail_length);

    auto hash = sum128(tail, tail_length,size, false);
    
    std::array<uint64_t,2> hash2;
    if (tail_length+1 == BLOCK_SIZE){
        auto word1 = reinterpret_cast<const uint64_t>(tail);
        auto word2 = reinterpret_cast<const uint64_t>(tail + 8);
        word2 = word2 | (TO_UINT64(tail[12]) << 32) | (TO_UINT64(tail[13]) << 40) | (TO_UINT64(tail[14]) << 48);
        word2 = word2 | (TO_UINT64(1) << 56);

        bmix_words(word1, word2);
        tail = data+size;
        hash2 = sum128(tail,  0, size+1, false);
    } else{
        hash2 = sum128(tail, tail_length,size+1 , true);
    }

    return {hash[0], hash[1], hash2[0], hash2[1]};
}