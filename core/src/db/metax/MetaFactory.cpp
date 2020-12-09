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

#include "db/metax/MetaFactory.h"

#include <memory>
#include <sstream>
#include <string>

#include "db/Utils.h"
#include "db/metax/MetaDef.h"
#include "db/metax/MetaProxy.h"
#include "utils/Exception.h"

namespace milvus::engine::metax {

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

MetaAdapterPtr
MetaFactory::Build(const DBMetaOptions& meta_options) {
    std::string uri = meta_options.backend_uri_;

    utils::MetaUriInfo uri_info;
    auto status = utils::ParseMetaUri(uri, uri_info);
    if (!status.ok()) {
        throw InvalidArgumentException("Wrong URI format ");
    }

    if (strcasecmp(uri_info.dialect_.c_str(), "mysql") == 0) {
        /* options.backend_uri_ = "mysql://root:12345678@127.0.0.1:3307/milvus"; */
        auto proxy = std::make_shared<MetaProxy>(EngineType::mysql);
        return std::make_shared<metax::MetaAdapter>(proxy);
    } else if (strcasecmp(uri_info.dialect_.c_str(), "mock") == 0) {
        auto proxy = std::make_shared<MetaProxy>(EngineType::mock);
        return std::make_shared<metax::MetaAdapter>(proxy);
    } else if (strcasecmp(uri_info.dialect_.c_str(), "sqlite") == 0) {
        auto proxy = std::make_shared<metax::MetaProxy>(EngineType::sqlite);
        return std::make_shared<metax::MetaAdapter>(proxy);
    } else {
        throw InvalidArgumentException("URI dialect is not mysql / sqlite / mock");
    }
}

}  // namespace milvus::engine::metax
