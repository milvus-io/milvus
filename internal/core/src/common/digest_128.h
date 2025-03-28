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
#include <cstdint>
#include <cstddef>

class Digest128{
    public:
        Digest128() : h1(0), h2(0) {};

        void
        bmix(const char *data, const size_t size);

        void
        bmix_words(uint64_t k1, uint64_t k2);

        std::array<uint64_t, 2>
        sum128(const char* data, const size_t size,const size_t length, const bool pad_tail);

        std::array<uint64_t, 4>
        sum256(const char* data, const size_t size); 
    private:
        uint64_t h1;
        uint64_t h2;
    };