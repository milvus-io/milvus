////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include "Factories.h"
#include "DBImpl.h"
#include "FaissExecutionEngine.h"
#include "Traits.h"

#include <stdlib.h>
#include <time.h>
#include <sstream>
#include <iostream>
#include <vector>
#include <assert.h>
#include <easylogging++.h>

namespace zilliz {
namespace vecwise {
namespace engine {

DBMetaOptions DBMetaOptionsFactory::Build(const std::string& path) {
    auto p = path;
    if(p == "") {
        srand(time(nullptr));
        std::stringstream ss;
        ss << "/tmp/" << rand();
        p = ss.str();
    }
    DBMetaOptions meta;
    meta.path = p;
    return meta;
}

Options OptionsFactory::Build() {
    auto meta = DBMetaOptionsFactory::Build();
    Options options;
    options.meta = meta;
    return options;
}

std::shared_ptr<meta::DBMetaImpl> DBMetaImplFactory::Build() {
    DBMetaOptions options = DBMetaOptionsFactory::Build();
    return std::shared_ptr<meta::DBMetaImpl>(new meta::DBMetaImpl(options));
}

std::shared_ptr<DB> DBFactory::Build(const std::string& db_type) {
    auto options = OptionsFactory::Build();
    auto db = DBFactory::Build(options, db_type);
    return std::shared_ptr<DB>(db);
}

DB* DBFactory::Build(const Options& options, const std::string& db_type) {
    std::stringstream ss(db_type);
    std::string token;
    std::vector<std::string> tokens;
    while (std::getline(ss, token, ',')) {
        tokens.push_back(token);
    }

    assert(tokens.size()==2);
    assert(tokens[0]=="Faiss");
    if (tokens[1] == "IVF") {
        return new DBImpl<FaissExecutionEngine<IVFIndexTrait>>(options);
    } else if (tokens[1] == "IDMap") {
        return new DBImpl<FaissExecutionEngine<IDMapIndexTrait>>(options);
    }
    return nullptr;
}

} // namespace engine
} // namespace vecwise
} // namespace zilliz
