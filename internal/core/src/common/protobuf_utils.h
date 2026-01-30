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

#include <string>
#include <map>
#include <optional>
#include <google/protobuf/text_format.h>
#include <google/protobuf/repeated_field.h>

#include "pb/schema.pb.h"
#include "common/EasyAssert.h"

using std::string;

namespace milvus {

template <typename T>
using ProtoRepeated = google::protobuf::RepeatedPtrField<T>;
using ProtoParams = ProtoRepeated<proto::common::KeyValuePair>;

static std::map<string, string>
RepeatedKeyValToMap(
    const google::protobuf::RepeatedPtrField<proto::common::KeyValuePair>&
        kvs) {
    std::map<string, string> mapping;
    for (auto& kv : kvs) {
        AssertInfo(
            !mapping.count(kv.key()), "repeat key({}) in protobuf", kv.key());
        mapping.emplace(kv.key(), kv.value());
    }
    return mapping;
}

/**
 * @brief Get a boolean value from repeated KeyValuePair by key.
 *
 * @param kvs The repeated KeyValuePair field to search.
 * @param key The key to look for.
 * @return std::pair<bool, bool> where:
 *         - first: whether the key was found.
 *         - second: the parsed boolean value (true if value is "true", case-insensitive).
 */
static std::pair<bool, bool>
GetBoolFromRepeatedKVs(
    const google::protobuf::RepeatedPtrField<proto::common::KeyValuePair>& kvs,
    const std::string& key) {
    for (auto& kv : kvs) {
        if (kv.key() == key) {
            std::string lower;
            std::transform(kv.value().begin(),
                           kv.value().end(),
                           std::back_inserter(lower),
                           ::tolower);
            return {true, lower == "true"};
        }
    }
    return {false, false};
}

/**
 * @brief Get a string value from repeated KeyValuePair by key.
 *
 * @param kvs The repeated KeyValuePair field to search.
 * @param key The key to look for.
 * @return std::optional<std::string> containing the value if found, std::nullopt otherwise.
 */
static std::optional<std::string>
GetStringFromRepeatedKVs(
    const google::protobuf::RepeatedPtrField<proto::common::KeyValuePair>& kvs,
    const std::string& key) {
    for (const auto& kv : kvs) {
        if (kv.key() == key) {
            return kv.value();
        }
    }
    return std::nullopt;
}

class ProtoLayout;
using ProtoLayoutPtr = std::unique_ptr<ProtoLayout>;

// ProtoLayout is a c++ type for esaier resource management at C-side.
// It's always keep same memory layout with ProtoLayout at C side for cgo call.
class ProtoLayout {
 public:
    ProtoLayout();

    ProtoLayout(const ProtoLayout&) = delete;

    ProtoLayout(ProtoLayout&&) = delete;

    ProtoLayout&
    operator=(const ProtoLayout&) = delete;

    ProtoLayout&
    operator=(ProtoLayout&&) = delete;

    ~ProtoLayout();

    // Serialize the proto into bytes and hold it in the layout.
    // Return false if failure.
    template <typename T>
    bool
    SerializeAndHoldProto(T& proto) {
        if (blob_ != nullptr || size_ != 0) {
            throw std::runtime_error(
                "ProtoLayout should always be empty "
                "before calling SerializeAndHoldProto");
        }
        size_ = proto.ByteSizeLong();
        blob_ = new uint8_t[size_];
        return proto.SerializeToArray(blob_, size_);
    }

 private:
    void* blob_;
    size_t size_;
};

}  //namespace milvus
