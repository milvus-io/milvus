////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include "MetaFactory.h"
#include "SqliteMetaImpl.h"
#include "MySQLMetaImpl.h"
#include "db/Log.h"
#include "db/Exception.h"

#include <stdlib.h>
#include <time.h>
#include <sstream>
#include <cstdlib>
#include <string>
#include <regex>

namespace zilliz {
namespace milvus {
namespace engine {

    DBMetaOptions MetaFactory::BuildOption(const std::string &path) {
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

    meta::MetaPtr MetaFactory::Build(const DBMetaOptions &metaOptions, const int &mode) {
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

} // namespace engine
} // namespace milvus
} // namespace zilliz
