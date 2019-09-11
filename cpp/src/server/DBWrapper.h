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

    static engine::DBPtr DB() {
        return GetInstance().EngineDB();
    }

    ErrorCode StartService();
    ErrorCode StopService();

    engine::DBPtr EngineDB() {
        return db_;
    }

private:
    engine::DBPtr db_;
};

}
}
}
