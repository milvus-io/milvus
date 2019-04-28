/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "VecIdMapper.h"
#include "ServerConfig.h"
#include "utils/Log.h"
#include "utils/CommonUtil.h"

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

namespace zilliz {
namespace vecwise {
namespace server {

IVecIdMapper* IVecIdMapper::GetInstance() {
#if 0
    static SimpleIdMapper s_mapper;
    return &s_mapper;
#else
    static RocksIdMapper s_mapper;
    return &s_mapper;
#endif
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SimpleIdMapper::SimpleIdMapper() {

}

SimpleIdMapper::~SimpleIdMapper() {

}

ServerError SimpleIdMapper::Put(const std::string& nid, const std::string& sid) {
    ids_[nid] = sid;
    return SERVER_SUCCESS;
}

ServerError SimpleIdMapper::Put(const std::vector<std::string>& nid, const std::vector<std::string>& sid) {
    if(nid.size() != sid.size()) {
        return SERVER_INVALID_ARGUMENT;
    }

    for(size_t i = 0; i < nid.size(); i++) {
        ids_[nid[i]] = sid[i];
    }

    return SERVER_SUCCESS;
}

ServerError SimpleIdMapper::Get(const std::string& nid, std::string& sid) const {
    auto iter = ids_.find(nid);
    if(iter == ids_.end()) {
        return SERVER_INVALID_ARGUMENT;
    }

    sid = iter->second;

    return SERVER_SUCCESS;
}

ServerError SimpleIdMapper::Get(const std::vector<std::string>& nid, std::vector<std::string>& sid) const {
    sid.clear();

    ServerError err = SERVER_SUCCESS;
    for(size_t i = 0; i < nid.size(); i++) {
        auto iter = ids_.find(nid[i]);
        if(iter == ids_.end()) {
            sid.push_back("");
            SERVER_LOG_ERROR << "ID mapper failed to find id: " << nid[i];
            err = SERVER_INVALID_ARGUMENT;
            continue;
        }

        sid.push_back(iter->second);
    }

    return err;
}

ServerError SimpleIdMapper::Delete(const std::string& nid) {
    ids_.erase(nid);
    return SERVER_SUCCESS;
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
RocksIdMapper::RocksIdMapper() {
    ConfigNode& config = ServerConfig::GetInstance().GetConfig(CONFIG_DB);
    std::string db_path = config.GetValue(CONFIG_DB_PATH);
    db_path += "/id_mapping";
    CommonUtil::CreateDirectory(db_path);

    rocksdb::Options options;
    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    // create the DB if it's not already present
    options.create_if_missing = true;
    options.max_open_files = config.GetInt32Value(CONFIG_DB_IDMAPPER_MAX_FILE, 128);

    // open DB
    rocksdb::Status s = rocksdb::DB::Open(options, db_path, &db_);
    if(!s.ok()) {
        SERVER_LOG_ERROR << "ID mapper failed to initialize:" << s.ToString();
        db_ = nullptr;
    }
}
RocksIdMapper::~RocksIdMapper() {
    if(db_) {
        db_->Close();
        delete db_;
    }
}

ServerError RocksIdMapper::Put(const std::string& nid, const std::string& sid) {
    if(db_ == nullptr) {
        return SERVER_NULL_POINTER;
    }

    rocksdb::Slice key(nid);
    rocksdb::Slice value(sid);
    rocksdb::Status s = db_->Put(rocksdb::WriteOptions(), key, value);
    if(!s.ok()) {
        SERVER_LOG_ERROR << "ID mapper failed to put:" << s.ToString();
        return SERVER_UNEXPECTED_ERROR;
    }

    return SERVER_SUCCESS;
}

ServerError RocksIdMapper::Put(const std::vector<std::string>& nid, const std::vector<std::string>& sid) {
    if(nid.size() != sid.size()) {
        return SERVER_INVALID_ARGUMENT;
    }

    ServerError err = SERVER_SUCCESS;
    for(size_t i = 0; i < nid.size(); i++) {
        err = Put(nid[i], sid[i]);
        if(err != SERVER_SUCCESS) {
            return err;
        }
    }

    return err;
}

ServerError RocksIdMapper::Get(const std::string& nid, std::string& sid) const {
    if(db_ == nullptr) {
        return SERVER_NULL_POINTER;
    }

    rocksdb::Slice key(nid);
    rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), key, &sid);
    if(!s.ok()) {
        SERVER_LOG_ERROR << "ID mapper failed to get:" << s.ToString();
        return SERVER_UNEXPECTED_ERROR;
    }

    return SERVER_SUCCESS;
}

ServerError RocksIdMapper::Get(const std::vector<std::string>& nid, std::vector<std::string>& sid) const {
    sid.clear();

    ServerError err = SERVER_SUCCESS;
    for(size_t i = 0; i < nid.size(); i++) {
        std::string str_id;
        ServerError temp_err = Get(nid[i], str_id);
        if(temp_err != SERVER_SUCCESS) {
            sid.push_back("");
            SERVER_LOG_ERROR << "ID mapper failed to get id: " << nid[i];
            err = temp_err;
            continue;
        }

        sid.push_back(str_id);
    }

    return err;
}

ServerError RocksIdMapper::Delete(const std::string& nid) {
    if(db_ == nullptr) {
        return SERVER_NULL_POINTER;
    }

    rocksdb::Slice key(nid);
    rocksdb::Status s = db_->Delete(rocksdb::WriteOptions(), key);
    if(!s.ok()) {
        SERVER_LOG_ERROR << "ID mapper failed to delete:" << s.ToString();
        return SERVER_UNEXPECTED_ERROR;
    }

    return SERVER_SUCCESS;
}

}
}
}