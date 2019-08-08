////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#pragma once

#include "DB.h"
#include "src/db/meta/SqliteMetaImpl.h"
#include "meta/MySQLMetaImpl.h"
#include "Options.h"
#include "src/db/insert/MemManager.h"

#include <string>
#include <memory>


namespace zilliz {
namespace milvus {
namespace engine {

struct DBMetaOptionsFactory {
    static DBMetaOptions Build(const std::string &path = "");
};

struct OptionsFactory {
    static Options Build();
};

struct DBMetaImplFactory {
    static std::shared_ptr<meta::SqliteMetaImpl> Build();
    static std::shared_ptr<meta::Meta> Build(const DBMetaOptions &metaOptions, const int &mode);
};

struct DBFactory {
    static std::shared_ptr<DB> Build();
    static DB *Build(const Options &);
};

struct MemManagerFactory {
    static MemManagerAbstractPtr Build(const std::shared_ptr<meta::Meta> &meta, const Options &options);
};

} // namespace engine
} // namespace milvus
} // namespace zilliz
