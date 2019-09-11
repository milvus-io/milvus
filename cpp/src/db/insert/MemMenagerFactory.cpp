////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include "MemMenagerFactory.h"
#include "MemManagerImpl.h"
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

MemManagerPtr MemManagerFactory::Build(const std::shared_ptr<meta::Meta>& meta,
                                        const Options& options) {
    return std::make_shared<MemManagerImpl>(meta, options);
}

} // namespace engine
} // namespace milvus
} // namespace zilliz
