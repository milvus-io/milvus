/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "DBWrapper.h"
#include "ServerConfig.h"
#include "utils/CommonUtil.h"
#include "utils/Log.h"

namespace zilliz {
namespace milvus {
namespace server {

DBWrapper::DBWrapper() {
    zilliz::milvus::engine::Options opt;
    ConfigNode& config = ServerConfig::GetInstance().GetConfig(CONFIG_DB);
    opt.meta.backend_uri = config.GetValue(CONFIG_DB_URL);
    std::string db_path = config.GetValue(CONFIG_DB_PATH);
    opt.meta.path = db_path + "/db";
    int64_t index_size = config.GetInt64Value(CONFIG_DB_INDEX_TRIGGER_SIZE);
    if(index_size > 0) {//ensure larger than zero, unit is MB
        opt.index_trigger_size = (size_t)index_size * engine::ONE_MB;
    }

    CommonUtil::CreateDirectory(opt.meta.path);

    zilliz::milvus::engine::DB::Open(opt, &db_);
    if(db_ == nullptr) {
        SERVER_LOG_ERROR << "Failed to open db. Provided database uri = " << opt.meta.backend_uri;
        throw ServerException(SERVER_NULL_POINTER, "Failed to open db");
    }
}

DBWrapper::~DBWrapper() {
    delete db_;
}

}
}
}