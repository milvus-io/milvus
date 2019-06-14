/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "DBImpl.h"
#include "DBMetaImpl.h"
#include "Env.h"
#include "Factories.h"

namespace zilliz {
namespace milvus {
namespace engine {

DB::~DB() {}

void DB::Open(const Options& options, DB** dbptr) {
    *dbptr = DBFactory::Build(options);
    return;
}

} // namespace engine
} // namespace milvus
} // namespace zilliz
