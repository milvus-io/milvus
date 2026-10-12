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

#include "storage/plugin/PluginInterface.h"

#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>

using milvus::storage::plugin::ICipherPlugin;
using milvus::storage::plugin::IPlugin;

extern "C" IPlugin*
CreatePlugin();

namespace {

std::string
hexEncode(const std::string& value) {
    std::ostringstream encoded;
    encoded << std::hex << std::setfill('0');
    for (unsigned char byte : value) {
        encoded << std::setw(2) << static_cast<unsigned int>(byte);
    }
    return encoded.str();
}

std::unique_ptr<ICipherPlugin>
newPlugin() {
    std::unique_ptr<IPlugin> plugin(CreatePlugin());
    auto* cipher = dynamic_cast<ICipherPlugin*>(plugin.get());
    if (cipher == nullptr) {
        throw std::runtime_error("fixture does not implement ICipherPlugin");
    }
    plugin.release();
    return std::unique_ptr<ICipherPlugin>(cipher);
}

void
require(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

}  // namespace

int
main() {
    try {
        const std::string edek =
            "v1:000102030405060708090a0b0c0d0e0f:"
            "d9cb9e34022d55b0e61b0c37e4561ad255400a285b27f74cbe31c2574dd10e4d";
        const std::string expected_dek =
            "bcdbeac6f09eb42ea81144b43a0ccc75cba5455134dda7ff6dcc5790da012327";

        auto first = newPlugin();
        auto decryptor = first->GetDecryptor(17, 23, edek);
        require(hexEncode(decryptor->GetKey()) == expected_dek,
                "C++ fixture does not match the Go derivation vector");

        bool wrong_context_rejected = false;
        try {
            static_cast<void>(first->GetDecryptor(18, 23, edek));
        } catch (const std::exception&) {
            wrong_context_rejected = true;
        }
        require(wrong_context_rejected, "wrong EZ context was accepted");

        bool wrong_collection_rejected = false;
        try {
            static_cast<void>(first->GetDecryptor(17, 24, edek));
        } catch (const std::exception&) {
            wrong_collection_rejected = true;
        }
        require(wrong_collection_rejected,
                "wrong collection context was accepted");

        bool malformed_edek_rejected = false;
        try {
            static_cast<void>(first->GetDecryptor(17, 23, "v1:00:00"));
        } catch (const std::exception&) {
            malformed_edek_rejected = true;
        }
        require(malformed_edek_rejected, "malformed EDEK was accepted");

        bool wrong_key_rejected = false;
        try {
            first->Update(17, 23, "wrong-key");
        } catch (const std::exception&) {
            wrong_key_rejected = true;
        }
        require(wrong_key_rejected, "wrong imported EZK was accepted");

        const std::string expected_ezk =
            "quQBfSKoD9W+tUibjbO/ei3OZsOejiHyC8d0M6BU04c=";
        first->Update(17, 23, expected_ezk);
        auto second = newPlugin();
        second->Update(17, 23, expected_ezk);
        auto first_edek = first->GetEncryptor(17, 23).second;
        auto second_edek = second->GetEncryptor(17, 23).second;
        require(first_edek != second_edek,
                "independent plugin instances reused an EDEK");
    } catch (const std::exception& error) {
        std::cerr << error.what() << std::endl;
        return 1;
    }
    return 0;
}
