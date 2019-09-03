/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "resource/Resource.h"
#include "ResourceMgr.h"

#include <vector>
#include <string>

namespace zilliz {
namespace milvus {
namespace engine {

uint64_t
ShortestPath(const ResourcePtr &src,
             const ResourcePtr &dest,
             const ResourceMgrPtr &res_mgr,
             std::vector<std::string>& path);

}
}
}