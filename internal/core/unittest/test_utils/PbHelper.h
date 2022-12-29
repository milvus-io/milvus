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

#include <vector>
#include <stdint.h>

namespace {
template <typename Message>
std::vector<uint8_t>
serialize(const Message* msg) {
    auto l = msg->ByteSizeLong();
    std::vector<uint8_t> ret(l);
    auto ok = msg->SerializeToArray(ret.data(), l);
    assert(ok);
    return ret;
}

template <class Msg>
std::unique_ptr<Msg>
clone_msg(const Msg* msg) {
    std::unique_ptr<Msg> p(msg->New());
    p->CopyFrom(*msg);
    return p;
}
}  // namespace