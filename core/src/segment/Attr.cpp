// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <memory>
#include <string>
#include <vector>
#include <cstring>

#include "Attr.h"

namespace milvus {
namespace segment {

Attr::Attr(void* data, size_t nbytes, std::vector<int64_t> uids, const std::string& name)
    : data_(data), nbytes_(nbytes), uids_(uids), name_(name) {

}

void
Attr::AddAttr(const void* data, size_t nbytes) {
    memcpy((void *)(static_cast<char *>(data_) + nbytes_), data, nbytes);
    nbytes_ += nbytes;
}

void
Attr::AddUids(const std::vector<int64_t>& uids) {
    uids_.reserve(uids_.size() + uids.size());
    uids_.insert(uids_.end(), std::make_move_iterator(uids.begin()), std::make_move_iterator(uids.end()));
}

void
Attr::SetName(const std::string& name) {
    name_ = name;
}

const void*
Attr::GetData() const {
    return data_;
}

const std::string&
Attr::GetName() const {
    return name_;
}

const std::string&
Attr::GetCollectionId() {
    return collection_id_;
}

const
size_t& Attr::GetNbytes() const {
    return nbytes_;
}

const std::vector<int64_t>&
Attr::GetUids() const {
    return uids_;
}

size_t
Attr::GetCount() const {
    return uids_.size();
}

size_t
Attr::GetCodeLength() const {
    return uids_.empty() ? 0 : nbytes_ / uids_.size();
}

}
}