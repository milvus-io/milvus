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

#include "grep/IndexCondition.h"

#include <google/protobuf/io/coded_stream.h>
#include <limits>

#include "pb/cgo_msg.pb.h"

namespace milvus::grep {
namespace {

RegexLiteralCondition
DecodeNode(const proto::cgo::LiteralCondition& node) {
    using Wire = proto::cgo::LiteralCondition;
    using Op = RegexLiteralCondition::Op;
    switch (node.kind_case()) {
        case Wire::kAlwaysTrue:
            return {};
        case Wire::kLiteral: {
            const auto& text = node.literal();
            if (text.empty()) {
                ThrowInfo(DataFormatBroken,
                          "empty literal condition tree literal");
            }
            // Rust's encoder guarantees UTF-8; preserve its bytes unchanged.
            return {Op::Literal, text, {}};
        }
        case Wire::kAll:
        case Wire::kAny: {
            const bool all = node.kind_case() == Wire::kAll;
            const auto& list = all ? node.all() : node.any();
            if (list.children_size() < 2) {
                ThrowInfo(
                    DataFormatBroken,
                    "literal condition tree operator requires two children");
            }
            RegexLiteralCondition result{all ? Op::And : Op::Or, {}, {}};
            result.children.reserve(list.children_size());
            for (const auto& child : list.children()) {
                result.children.push_back(DecodeNode(child));
            }
            return result;
        }
        case Wire::KIND_NOT_SET:
            ThrowInfo(DataFormatBroken,
                      "literal condition tree node has no kind");
    }
    ThrowInfo(DataFormatBroken, "unknown literal condition tree node kind");
}

}  // namespace

RegexLiteralCondition
DecodeLiteralConditionTree(std::string_view payload) {
    google::protobuf::io::CodedInputStream input(
        reinterpret_cast<const uint8_t*>(payload.data()),
        static_cast<int>(payload.size()));
    // Rust bounds payload size and depth. Protobuf's default recursion limit
    // is lower than valid Rust output, so do not impose it again here.
    input.SetRecursionLimit(std::numeric_limits<int>::max());
    proto::cgo::LiteralConditionTree message;
    if (!message.ParseFromCodedStream(&input) ||
        !input.ConsumedEntireMessage()) {
        ThrowInfo(DataFormatBroken,
                  "malformed literal condition tree protobuf");
    }
    if (!message.has_root()) {
        ThrowInfo(DataFormatBroken,
                  "literal condition tree protobuf has no root");
    }
    // Validate the whole decoded tree, including branches beyond any planner
    // budget, before the planner is allowed to weaken the condition.
    return DecodeNode(message.root());
}

}  // namespace milvus::grep
