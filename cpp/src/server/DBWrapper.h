/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "utils/Error.h"
#include "db/DB.h"

#include <memory>

namespace zilliz {
namespace milvus {
namespace server {

class DBWrapper {
private:
    DBWrapper();
    ~DBWrapper() = default;

public:
    static DBWrapper& GetInstance() {
        static DBWrapper wrapper;
        return wrapper;
    }

    static std::shared_ptr<engine::DB> DB() {
        return GetInstance().EngineDB();
    }

    ErrorCode StartService();
    ErrorCode StopService();

    std::shared_ptr<engine::DB> EngineDB() {
        return db_;
    }

private:
    std::shared_ptr<engine::DB> db_;
};

}
}
}
