////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include "DBFactory.h"
#include "DBImpl.h"
#include "utils/Exception.h"
#include "meta/MetaFactory.h"
#include "meta/SqliteMetaImpl.h"
#include "meta/MySQLMetaImpl.h"

#include <stdlib.h>
#include <time.h>
#include <sstream>
#include <cstdlib>
#include <string>

namespace zilliz {
namespace milvus {
namespace engine {

Options DBFactory::BuildOption() {
    auto meta = MetaFactory::BuildOption();
    Options options;
    options.meta = meta;
    return options;
}

DBPtr DBFactory::Build(const Options& options) {
    return std::make_shared<DBImpl>(options);
}

} // namespace engine
} // namespace milvus
} // namespace zilliz
