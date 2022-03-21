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

#include "pb/index_cgo_msg.pb.h"
#include "exceptions/EasyAssert.h"
#include <google/protobuf/text_format.h>
#include <string>
#include <map>

namespace milvus::indexbuilder {

using MapParams = std::map<std::string, std::string>;

struct Helper {
    static void
    ParseFromString(google::protobuf::Message& params, const std::string& str) {
        auto ok = google::protobuf::TextFormat::ParseFromString(str, &params);
        AssertInfo(ok, "failed to parse params from string");
    }

    static void
    ParseParams(google::protobuf::Message& params, const void* data, const size_t size) {
        auto ok = params.ParseFromArray(data, size);
        AssertInfo(ok, "failed to parse params from array");
    }
};

}  // namespace milvus::indexbuilder
