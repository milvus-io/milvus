////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#pragma once

#include "DB.h"
#include "DBMetaImpl.h"
#include "MySQLMetaImpl.h"
#include "Options.h"
#include "ExecutionEngine.h"

#include <string>
#include <memory>

namespace zilliz {
namespace milvus {
namespace engine {

struct DBMetaOptionsFactory {
    static DBMetaOptions Build(const std::string& path = "");
};

struct OptionsFactory {
    static Options Build();
};

struct DBMetaImplFactory {
    static std::shared_ptr<meta::DBMetaImpl> Build();
    static std::shared_ptr<meta::Meta> Build(const DBMetaOptions& metaOptions, const std::string& mode);
};

struct DBFactory {
    static std::shared_ptr<DB> Build();
    static DB* Build(const Options&);
};

} // namespace engine
} // namespace milvus
} // namespace zilliz
