/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include <stdlib.h>
#include <assert.h>
#include <easylogging++.h>
#include <boost/algorithm/string.hpp>

#include "Options.h"
#include "DBMetaImpl.h"
#include "Exception.h"

namespace zilliz {
namespace milvus {
namespace engine {

Options::Options() {
}

ArchiveConf::ArchiveConf(const std::string& type, const std::string& criterias) {
    ParseType(type);
    ParseCritirias(criterias);
}

void ArchiveConf::SetCriterias(const ArchiveConf::CriteriaT& criterial) {
    for(auto& pair : criterial) {
        criterias_[pair.first] = pair.second;
    }
}

void ArchiveConf::ParseCritirias(const std::string& criterias) {
    std::stringstream ss(criterias);
    std::vector<std::string> tokens;

    boost::algorithm::split(tokens, criterias, boost::is_any_of(";"));

    if (tokens.size() == 0) {
        return;
    }

    for (auto& token : tokens) {
        if(token.empty()) {
            continue;
        }

        std::vector<std::string> kv;
        boost::algorithm::split(kv, token, boost::is_any_of(":"));
        if (kv.size() != 2) {
            LOG(WARNING) << "Invalid ArchiveConf Criterias: " << token << " Ignore!";
            continue;
        }
        if (kv[0] != "disk" && kv[0] != "days") {
            LOG(WARNING) << "Invalid ArchiveConf Criterias: " << token << " Ignore!";
            continue;
        }
        try {
            auto value = std::stoi(kv[1]);
            criterias_[kv[0]] = value;
        }
        catch (std::out_of_range&){
            LOG(ERROR) << "Out of range: '" << kv[1] << "'";
            throw OutOfRangeException();
        }
        catch (...){
            LOG(ERROR) << "Invalid argument: '" << kv[1] << "'";
            throw InvalidArgumentException();
        }
    }
}

void ArchiveConf::ParseType(const std::string& type) {
    if (type != "delete" && type != "swap") {
        LOG(ERROR) << "Invalid argument: type='" << type << "'";
        throw InvalidArgumentException();
    }
    type_ = type;
}

} // namespace engine
} // namespace milvus
} // namespace zilliz
