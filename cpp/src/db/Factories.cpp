////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <stdlib.h>
#include "Factories.h"
#include "DBImpl.h"

#include <time.h>
#include <sstream>
#include <iostream>
#include <vector>
#include <assert.h>
#include <easylogging++.h>
#include <regex>
#include "Exception.h"

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

//    std::string uri;
//    const char* uri_p = getenv("MILVUS_DB_META_URI");
//    if (uri_p) {
//        uri = uri_p;
//    }

    DBMetaOptions meta;
    meta.path = p;
//    meta.backend_uri = uri;
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

std::shared_ptr<meta::Meta> DBMetaImplFactory::Build(const DBMetaOptions& metaOptions) {

    std::string uri = metaOptions.backend_uri;
//    if (uri.empty()) {
//        //Default to sqlite if uri is empty
////        return std::make_shared<meta::DBMetaImpl>(new meta::DBMetaImpl(metaOptions));
//        return std::shared_ptr<meta::DBMetaImpl>(new meta::DBMetaImpl(metaOptions));
//    }

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
            return std::make_shared<meta::MySQLMetaImpl>(meta::MySQLMetaImpl(metaOptions));
        }
        else if (dialect.find("sqlite") != std::string::npos) {
            return std::make_shared<meta::DBMetaImpl>(meta::DBMetaImpl(metaOptions));
        }
        else {
            LOG(ERROR) << "Invalid dialect in URI: dialect = " << dialect;
            throw InvalidArgumentException("URI dialect is not mysql / sqlite");
        }
    }
    else {
        LOG(ERROR) << "Wrong URI format: URI = " << uri;
        throw InvalidArgumentException("Wrong URI format");
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

} // namespace engine
} // namespace milvus
} // namespace zilliz
