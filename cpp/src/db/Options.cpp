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

#include "db/Options.h"
#include "utils/Exception.h"
#include "utils/Log.h"

#include <stdlib.h>
#include <assert.h>
#include <boost/algorithm/string.hpp>

namespace zilliz {
namespace milvus {
namespace engine {

ArchiveConf::ArchiveConf(const std::string &type, const std::string &criterias) {
    ParseType(type);
    ParseCritirias(criterias);
}

void
ArchiveConf::SetCriterias(const ArchiveConf::CriteriaT &criterial) {
    for (auto &pair : criterial) {
        criterias_[pair.first] = pair.second;
    }
}

void
ArchiveConf::ParseCritirias(const std::string &criterias) {
    std::stringstream ss(criterias);
    std::vector<std::string> tokens;

    boost::algorithm::split(tokens, criterias, boost::is_any_of(";"));

    if (tokens.size() == 0) {
        return;
    }

    for (auto &token : tokens) {
        if (token.empty()) {
            continue;
        }

        std::vector<std::string> kv;
        boost::algorithm::split(kv, token, boost::is_any_of(":"));
        if (kv.size() != 2) {
            ENGINE_LOG_WARNING << "Invalid ArchiveConf Criterias: " << token << " Ignore!";
            continue;
        }
        if (kv[0] != "disk" && kv[0] != "days") {
            ENGINE_LOG_WARNING << "Invalid ArchiveConf Criterias: " << token << " Ignore!";
            continue;
        }
        try {
            auto value = std::stoi(kv[1]);
            criterias_[kv[0]] = value;
        }
        catch (std::out_of_range &) {
            std::string msg = "Out of range: '" + kv[1] + "'";
            ENGINE_LOG_ERROR << msg;
            throw InvalidArgumentException(msg);
        }
        catch (...) {
            std::string msg = "Invalid argument: '" + kv[1] + "'";
            ENGINE_LOG_ERROR << msg;
            throw InvalidArgumentException(msg);
        }
    }
}

void
ArchiveConf::ParseType(const std::string &type) {
    if (type != "delete" && type != "swap") {
        std::string msg = "Invalid argument: type='" + type + "'";
        throw InvalidArgumentException(msg);
    }
    type_ = type;
}

} // namespace engine
} // namespace milvus
} // namespace zilliz
