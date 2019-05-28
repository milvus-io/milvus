/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "utils/Error.h"
#include "VecIdMapper.h"

#include "rocksdb/db.h"

#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>

namespace zilliz {
namespace vecwise {
namespace server {

class RocksIdMapper : public IVecIdMapper{
public:
    RocksIdMapper();
    ~RocksIdMapper();

    ServerError AddGroup(const std::string& group) override;
    bool IsGroupExist(const std::string& group) const override;
    ServerError AllGroups(std::vector<std::string>& groups) const override;

    ServerError Put(const std::string& nid, const std::string& sid, const std::string& group = "") override;
    ServerError Put(const std::vector<std::string>& nid, const std::vector<std::string>& sid, const std::string& group = "") override;

    ServerError Get(const std::string& nid, std::string& sid, const std::string& group = "") const override;
    ServerError Get(const std::vector<std::string>& nid, std::vector<std::string>& sid, const std::string& group = "") const override;

    ServerError Delete(const std::string& nid, const std::string& group = "") override;
    ServerError DeleteGroup(const std::string& group) override;

private:
    void OpenDb();
    void CloseDb();

    ServerError AddGroupInternal(const std::string& group);

    bool IsGroupExistInternal(const std::string& group) const;

    ServerError PutInternal(const std::string& nid, const std::string& sid, const std::string& group);

    ServerError GetInternal(const std::string& nid, std::string& sid, const std::string& group) const;

    ServerError DeleteInternal(const std::string& nid, const std::string& group);

    ServerError DeleteGroupInternal(const std::string& group);

private:
    rocksdb::DB* db_;
    mutable std::unordered_map<std::string, rocksdb::ColumnFamilyHandle*> column_handles_;
    mutable std::mutex db_mutex_;
};

}
}
}
