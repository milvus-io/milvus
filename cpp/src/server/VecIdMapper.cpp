/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "VecIdMapper.h"
#include "RocksIdMapper.h"
#include "ServerConfig.h"
#include "utils/Log.h"
#include "utils/CommonUtil.h"

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

#include <exception>
#include <unordered_map>

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
class SimpleIdMapper : public IVecIdMapper{
public:
    SimpleIdMapper();
    ~SimpleIdMapper();

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
    using ID_MAPPING = std::unordered_map<std::string, std::string>;
    mutable std::unordered_map<std::string, ID_MAPPING> id_groups_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SimpleIdMapper::SimpleIdMapper() {

}

SimpleIdMapper::~SimpleIdMapper() {

}

ServerError
SimpleIdMapper::AddGroup(const std::string& group) {
    if(id_groups_.count(group) == 0) {
        id_groups_.insert(std::make_pair(group, ID_MAPPING()));
    }
}

//not thread-safe
bool
SimpleIdMapper::IsGroupExist(const std::string& group) const {
    return id_groups_.count(group) > 0;
}

ServerError SimpleIdMapper::AllGroups(std::vector<std::string>& groups) const {
    groups.clear();

    for(auto& pair : id_groups_) {
        groups.push_back(pair.first);
    }

    return SERVER_SUCCESS;
}

//not thread-safe
ServerError SimpleIdMapper::Put(const std::string& nid, const std::string& sid, const std::string& group) {
    ID_MAPPING& mapping = id_groups_[group];
    mapping[nid] = sid;
    return SERVER_SUCCESS;
}

//not thread-safe
ServerError SimpleIdMapper::Put(const std::vector<std::string>& nid, const std::vector<std::string>& sid, const std::string& group) {
    if(nid.size() != sid.size()) {
        return SERVER_INVALID_ARGUMENT;
    }

    ID_MAPPING& mapping = id_groups_[group];
    for(size_t i = 0; i < nid.size(); i++) {
        mapping[nid[i]] = sid[i];
    }

    return SERVER_SUCCESS;
}

//not thread-safe
ServerError SimpleIdMapper::Get(const std::string& nid, std::string& sid, const std::string& group) const {
    ID_MAPPING& mapping = id_groups_[group];

    auto iter = mapping.find(nid);
    if(iter == mapping.end()) {
        return SERVER_INVALID_ARGUMENT;
    }

    sid = iter->second;

    return SERVER_SUCCESS;
}

//not thread-safe
ServerError SimpleIdMapper::Get(const std::vector<std::string>& nid, std::vector<std::string>& sid, const std::string& group) const {
    sid.clear();

    ID_MAPPING& mapping = id_groups_[group];

    ServerError err = SERVER_SUCCESS;
    for(size_t i = 0; i < nid.size(); i++) {
        auto iter = mapping.find(nid[i]);
        if(iter == mapping.end()) {
            sid.push_back("");
            SERVER_LOG_ERROR << "ID mapper failed to find id: " << nid[i];
            err = SERVER_INVALID_ARGUMENT;
            continue;
        }

        sid.push_back(iter->second);
    }

    return err;
}

//not thread-safe
ServerError SimpleIdMapper::Delete(const std::string& nid, const std::string& group) {
    ID_MAPPING& mapping = id_groups_[group];
    mapping.erase(nid);
    return SERVER_SUCCESS;
}

//not thread-safe
ServerError SimpleIdMapper::DeleteGroup(const std::string& group) {
    id_groups_.erase(group);
    return SERVER_SUCCESS;
}

}
}
}