/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "IConfigMgr.h"
#include "YamlConfigMgr.h"

namespace zilliz {
namespace vecwise {
namespace server {

IConfigMgr * IConfigMgr::GetInstance() {
    static YamlConfigMgr mgr;
    return &mgr;
}

}
}
}
