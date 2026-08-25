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

namespace milvus::index {
template <typename T>
struct IndexStructure {
    IndexStructure() : a_(0), idx_(0) {
    }
    explicit IndexStructure(const T a) : a_(a), idx_(0) {
    }
    IndexStructure(const T a, const size_t idx) : a_(a), idx_(idx) {
    }
    bool
    operator<(const IndexStructure& b) const {
        return a_ < b.a_;
    }
    bool
    operator<=(const IndexStructure& b) const {
        return a_ <= b.a_;
    }
    bool
    operator>(const IndexStructure& b) const {
        return a_ > b.a_;
    }
    bool
    operator>=(const IndexStructure& b) const {
        return a_ >= b.a_;
    }
    bool
    operator==(const IndexStructure& b) const {
        return a_ == b.a_;
    }
    T a_;
    int32_t idx_;
};
}  // namespace milvus::index
