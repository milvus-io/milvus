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

#include "db/meta/MetaFactory.h"
#include "MySQLMetaImpl.h"
#include "SqliteMetaImpl.h"
#include "db/Utils.h"
#include "utils/Exception.h"
#include "utils/Log.h"

#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <cstdlib>
#include <memory>
#include <sstream>
#include <string>

namespace milvus {
namespace engine {

DBMetaOptions
MetaFactory::BuildOption(const std::string& path) {
    auto p = path;
    if (p == "") {
        srand(time(nullptr));
        std::stringstream ss;
        uint32_t seed = 1;
        ss << "/tmp/" << rand_r(&seed);
        p = ss.str();
    }

    DBMetaOptions meta;
    meta.path_ = p;
    return meta;
}

meta::MetaPtr
MetaFactory::Build(const DBMetaOptions& metaOptions, const int& mode) {
    std::string uri = metaOptions.backend_uri_;

    utils::MetaUriInfo uri_info;
    auto status = utils::ParseMetaUri(uri, uri_info);
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << "Wrong URI format: URI = " << uri;
        throw InvalidArgumentException("Wrong URI format ");
    }

    if (strcasecmp(uri_info.dialect_.c_str(), "mysql") == 0) {
        LOG_ENGINE_INFO_ << "Using MySQL";
        return std::make_shared<meta::MySQLMetaImpl>(metaOptions, mode);
    } else if (strcasecmp(uri_info.dialect_.c_str(), "sqlite") == 0) {
        LOG_ENGINE_INFO_ << "Using SQLite";
        return std::make_shared<meta::SqliteMetaImpl>(metaOptions);
    } else {
        LOG_ENGINE_ERROR_ << "Invalid dialect in URI: dialect = " << uri_info.dialect_;
        throw InvalidArgumentException("URI dialect is not mysql / sqlite");
    }
}

}  // namespace engine
}  // namespace milvus
