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


//
// Created by wzy on 22-8-6.
//

#ifndef MILVUS_WASMFUNCTION_H
#define MILVUS_WASMFUNCTION_H

#include <wasmtime/wasmtime.hh>
#include <unordered_map>
#include <cassert>
#include <boost/variant.hpp>
#include <limits>
#include <string>
#include <vector>

namespace milvus {

enum class WasmFunctionParamType {
    INT32 = 1,
    INT64 = 2,
    FLOAT = 3,
    STRING = 4,
};

struct WasmtimeRunInstance {
    wasmtime::Func func;
    wasmtime::Instance instance;
    WasmtimeRunInstance(const wasmtime::Func &func, const wasmtime::Instance &instance)
            : func(func), instance(instance) {}
};

class WasmFunctionManager {
 private:
    // wasmtime
    wasmtime::Engine *engine;
    wasmtime::Store *store;
    std::unordered_map<std::string, std::string> funcMap;
    std::unordered_map<std::string, WasmtimeRunInstance> modules;

    WasmFunctionManager() {
        engine = new wasmtime::Engine;
        store = new wasmtime::Store(*engine);
    }

    ~WasmFunctionManager() {
        delete (store);
        delete (engine);
    }

    WasmFunctionManager(const WasmFunctionManager&);
    WasmFunctionManager& operator=(const WasmFunctionManager&);

    // base64 tool
    constexpr static const char b64_table[65] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    constexpr static const char reverse_table[128] = {
            64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
            64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
            64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 62, 64, 64, 64, 63,
            52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 64, 64, 64, 64, 64, 64,
            64,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14,
            15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 64, 64, 64, 64, 64,
            64, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
            41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 64, 64, 64, 64, 64
    };

 public:
    static WasmFunctionManager& getInstance() {
        static WasmFunctionManager instance;
        return instance;
    }

    WasmtimeRunInstance createInstanceAndFunction(const std::string &watString,
                                                  const std::string &functionHandler);

    bool RegisterFunction(std::string functionName,
                          std::string functionHandler,
                          const std::string &base64OrOtherString);

    bool runElemFunc(const std::string functionName, std::vector<wasmtime::Val> args);

    bool DeleteFunction(std::string functionName);

    // base64 tool
    static std::string myBase64Encode(const ::std::string &bindata) {
        using ::std::string;
        using ::std::numeric_limits;

        if (bindata.size() > (numeric_limits<string::size_type>::max() / 4u) * 3u) {
            throw ::std::length_error("Converting too large a string to base64.");
        }

        const ::std::size_t binlen = bindata.size();
        // Use = signs so the end is properly padded.
        string retval((((binlen + 2) / 3) * 4), '=');
        ::std::size_t outpos = 0;
        int bits_collected = 0;
        unsigned int accumulator = 0;
        const string::const_iterator binend = bindata.end();

        for (string::const_iterator i = bindata.begin(); i != binend; ++i) {
            accumulator = (accumulator << 8) | (*i & 0xffu);
            bits_collected += 8;
            while (bits_collected >= 6) {
                bits_collected -= 6;
                retval[outpos++] = b64_table[(accumulator >> bits_collected) & 0x3fu];
            }
        }
        if (bits_collected > 0) { // Any trailing bits that are missing.
            assert(bits_collected < 6);
            accumulator <<= 6 - bits_collected;
            retval[outpos++] = b64_table[accumulator & 0x3fu];
        }
        assert(outpos >= (retval.size() - 2));
        assert(outpos <= retval.size());
        return retval;
    }

    static std::string myBase64Decode(const ::std::string &ascdata) {
        using ::std::string;
        string retval;
        const string::const_iterator last = ascdata.end();
        int bits_collected = 0;
        unsigned int accumulator = 0;

        for (string::const_iterator i = ascdata.begin(); i != last; ++i) {
            const int c = *i;
            if (::std::isspace(c) || c == '=') {
                // Skip whitespace and padding. Be liberal in what you accept.
                continue;
            }
            if ((c > 127) || (c < 0) || (reverse_table[c] > 63)) {
                throw ::std::invalid_argument("This contains characters not legal in a base64 encoded string.");
            }
            accumulator = (accumulator << 6) | reverse_table[c];
            bits_collected += 6;
            if (bits_collected >= 8) {
                bits_collected -= 8;
                retval += static_cast<char>((accumulator >> bits_collected) & 0xffu);
            }
        }
        return retval;
    }
};

}  // namespace milvus
#endif //MILVUS_WASMFUNCTION_H

