////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include "LogUtil.h"

#include <easylogging++.h>

namespace zilliz {
namespace vecwise {
namespace server {

int32_t InitLog() {
    el::Configurations conf("../../conf/vecwise_engine_log.conf");
    el::Loggers::reconfigureAllLoggers(conf);

    return 0;
}


}   // server
}   // vecwise
}   // zilliz
