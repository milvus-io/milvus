////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include "Factories.h"
#include "DBImpl.h"
#include "src/db/insert/MemManagerImpl.h"
#include "Exception.h"

#include <stdlib.h>
#include <time.h>
#include <sstream>
#include <iostream>
#include <vector>
#include <assert.h>
#include <easylogging++.h>
#include <regex>
#include <cstdlib>
#include <string>
#include <algorithm>

namespace zilliz {
namespace milvus {
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

std::shared_ptr<meta::SqliteMetaImpl> DBMetaImplFactory::Build() {
    DBMetaOptions options = DBMetaOptionsFactory::Build();
    return std::shared_ptr<meta::SqliteMetaImpl>(new meta::SqliteMetaImpl(options));
}

std::shared_ptr<meta::Meta> DBMetaImplFactory::Build(const DBMetaOptions& metaOptions,
                                                     const int& mode) {

    std::string uri = metaOptions.backend_uri;

    std::string dialectRegex = "(.*)";
    std::string usernameRegex = "(.*)";
    std::string passwordRegex = "(.*)";
    std::string hostRegex = "(.*)";
    std::string portRegex = "(.*)";
    std::string dbNameRegex = "(.*)";
    std::string uriRegexStr = dialectRegex + "\\:\\/\\/" +
                              usernameRegex + "\\:" +
                              passwordRegex + "\\@" +
                              hostRegex + "\\:" +
                              portRegex + "\\/" +
                              dbNameRegex;
    std::regex uriRegex(uriRegexStr);
    std::smatch pieces_match;

    if (std::regex_match(uri, pieces_match, uriRegex)) {
        std::string dialect = pieces_match[1].str();
        std::transform(dialect.begin(), dialect.end(), dialect.begin(), ::tolower);
        if (dialect.find("mysql") != std::string::npos) {
            ENGINE_LOG_INFO << "Using MySQL";
            return std::make_shared<meta::MySQLMetaImpl>(metaOptions, mode);
        } else if (dialect.find("sqlite") != std::string::npos) {
            ENGINE_LOG_INFO << "Using SQLite";
            return std::make_shared<meta::SqliteMetaImpl>(metaOptions);
        } else {
            ENGINE_LOG_ERROR << "Invalid dialect in URI: dialect = " << dialect;
            throw InvalidArgumentException("URI dialect is not mysql / sqlite");
        }
    } else {
        ENGINE_LOG_ERROR << "Wrong URI format: URI = " << uri;
        throw InvalidArgumentException("Wrong URI format ");
    }
}

std::shared_ptr<DB> DBFactory::Build() {
    auto options = OptionsFactory::Build();
    auto db = DBFactory::Build(options);
    return std::shared_ptr<DB>(db);
}

DB* DBFactory::Build(const Options& options) {
    return new DBImpl(options);
}

MemManagerAbstractPtr MemManagerFactory::Build(const std::shared_ptr<meta::Meta>& meta,
                                               const Options& options) {
    return std::make_shared<MemManagerImpl>(meta, options);
}

} // namespace engine
} // namespace milvus
} // namespace zilliz
