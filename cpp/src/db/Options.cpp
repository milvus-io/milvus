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
#include "Env.h"
#include "DBMetaImpl.h"

namespace zilliz {
namespace vecwise {
namespace engine {

Options::Options()
    : env(Env::Default()) {
}

ArchiveConf::ArchiveConf(const std::string& type, const std::string& criterias) {
    ParseType(type);
    ParseCritirias(criterias);
}

void ArchiveConf::ParseCritirias(const std::string& criterias) {
    std::stringstream ss(criterias);
    std::vector<std::string> tokens;

    boost::algorithm::split(tokens, criterias, boost::is_any_of(";"));

    if (tokens.size() == 0) {
        return;
    }

    for (auto& token : tokens) {
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
        auto value = std::stoi(kv[1]);
        criterias_[kv[0]] = value;
    }
}

void ArchiveConf::ParseType(const std::string& type) {
    if (type != "delete" && type != "swap") {
        LOG(ERROR) << "Invalid Archive";
        assert(false);
    }
    type_ = type;
}

/* DBMetaOptions::DBMetaOptions(const std::string& dbpath, */
/*         const std::string& uri) */
/*     : path(dbpath), backend_uri(uri) { */
/* } */

} // namespace engine
} // namespace vecwise
} // namespace zilliz
