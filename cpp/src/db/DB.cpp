/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "DBImpl.h"
#include "DBMetaImpl.h"
#include "Env.h"
/* #include "FaissExecutionEngine.h" */
/* #include "Traits.h" */
#include "Factories.h"

namespace zilliz {
namespace vecwise {
namespace engine {

DB::~DB() {}

void DB::Open(const Options& options, DB** dbptr) {
    *dbptr = nullptr;
    *dbptr = DBFactory::Build(options);
    return;
}

} // namespace engine
} // namespace vecwise
} // namespace zilliz
