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
#include "metrics/SystemInfo.h"
#include "scheduler/SchedInst.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <memory>
#include <vector>

namespace milvus {
namespace server {

CmdReq::CmdReq(const std::shared_ptr<milvus::server::Context>& context, const std::string& cmd, std::string& result)
    : BaseReq(context, BaseReq::kCmd), cmd_(cmd), result_(result) {
}

BaseReqPtr
CmdReq::Create(const std::shared_ptr<milvus::server::Context>& context, const std::string& cmd, std::string& result) {
    return std::shared_ptr<BaseReq>(new CmdReq(context, cmd, result));
}

Status
CmdReq::OnExecute() {
    std::string hdr = "CmdReq(cmd=" + cmd_ + ")";
    TimeRecorderAuto rc(hdr);
    Status stat = Status::OK();

    if (cmd_ == "version") {
        result_ = MILVUS_VERSION;
    } else if (cmd_ == "status") {
        result_ = "OK";
    } else if (cmd_ == "tasktable") {
        result_ = scheduler::ResMgrInst::GetInstance()->DumpTaskTables();
    } else if (cmd_ == "mode") {
#ifdef MILVUS_GPU_VERSION
        result_ = "GPU";
#else
        result_ = "CPU";
#endif
    } else if (cmd_ == "get_system_info") {
        server::SystemInfo& sys_info_inst = server::SystemInfo::GetInstance();
        sys_info_inst.GetSysInfoJsonStr(result_);
    } else if (cmd_ == "build_commit_id") {
        result_ = LAST_COMMIT_ID;
    } else if (cmd_.substr(0, 3) == "GET") {
        try {
            std::stringstream ss(cmd_);
            std::vector<std::string> words;
            std::string word;
            while (std::getline(ss, word, ' ')) {
                words.push_back(word);
            }
            if (words.size() == 2) {
                result_ = ConfigMgr::GetInstance().Get(words[1]);
            }
        } catch (ConfigStatus& cs) {
            stat = Status(SERVER_UNEXPECTED_ERROR, cs.message);
        } catch (...) {
            stat = Status(SERVER_UNEXPECTED_ERROR, "Unknown exception happened on GET command.");
        }
    } else if (cmd_.substr(0, 3) == "SET") {
        try {
            std::stringstream ss(cmd_);
            std::vector<std::string> words;
            std::string word;
            while (std::getline(ss, word, ' ')) {
                words.push_back(word);
            }
            if (words.size() == 3) {
                ConfigMgr::GetInstance().Set(words[1], words[2]);
            }
        } catch (ConfigStatus& cs) {
            stat = Status(SERVER_UNEXPECTED_ERROR, cs.message);
        } catch (...) {
            stat = Status(SERVER_UNEXPECTED_ERROR, "Unknown exception happened on SET command.");
        }
    } else {
        result_ = "Unknown command";
    }

    return stat;
}

}  // namespace server
}  // namespace milvus
