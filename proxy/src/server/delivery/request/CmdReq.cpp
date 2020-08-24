// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "server/delivery/request/CmdReq.h"
#include "config/ConfigMgr.h"
// #include "metrics/SystemInfo.h"
// #include "scheduler/SchedInst.h"
#include "src/version.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <algorithm>
#include <cctype>
#include <memory>
#include <vector>

namespace milvus {
namespace server {

CmdReq::CmdReq(const ContextPtr& context, const std::string& cmd, std::string& result)
    : BaseReq(context, ReqType::kCmd), origin_cmd_(cmd), cmd_(tolower(cmd)), result_(result) {
}

BaseReqPtr
CmdReq::Create(const ContextPtr& context, const std::string& cmd, std::string& result) {
    return std::shared_ptr<BaseReq>(new CmdReq(context, cmd, result));
}

Status
CmdReq::OnExecute() {
    std::string hdr = "CmdReq(cmd=" + cmd_ + ")";
    TimeRecorderAuto rc(hdr);
    Status stat = Status::OK();

    return stat;
}

std::vector<std::string>
CmdReq::split(const std::string& src, char delimiter) {
    std::stringstream ss(src);
    std::vector<std::string> words;
    std::string word;
    while (std::getline(ss, word, delimiter)) {
        words.push_back(word);
    }
    return words;
}

std::string
CmdReq::tolower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });
    return s;
}

}  // namespace server
}  // namespace milvus
