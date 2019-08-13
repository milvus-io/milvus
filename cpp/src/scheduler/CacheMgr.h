/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <memory>


namespace zilliz {
namespace milvus {
namespace engine {

// dummy cache_mgr
class CacheMgr {

};

using CacheMgrPtr = std::shared_ptr<CacheMgr>;

}
}
}
