/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "db/DB.h"

namespace zilliz {
namespace milvus {
namespace server {

class DBWrapper {
private:
    DBWrapper();
    ~DBWrapper();

public:
    static zilliz::milvus::engine::DB* DB() {
        static DBWrapper db_wrapper;
        return db_wrapper.db();
    }

    zilliz::milvus::engine::DB* db() { return db_; }

private:
    zilliz::milvus::engine::DB* db_ = nullptr;
};

}
}
}
