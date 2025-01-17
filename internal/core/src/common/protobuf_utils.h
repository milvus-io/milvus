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
#include <google/protobuf/text_format.h>

#include "pb/schema.pb.h"
#include "common/EasyAssert.h"

using std::string;

namespace milvus {
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
