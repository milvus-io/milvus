/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "VecIdMapper.h"

#include "utils/Log.h"

namespace zilliz {
namespace vecwise {
namespace server {

IVecIdMapper* IVecIdMapper::GetInstance() {
    static SimpleIdMapper s_mapper;
    return &s_mapper;
}

SimpleIdMapper::SimpleIdMapper() {

}

SimpleIdMapper::~SimpleIdMapper() {

}

ServerError SimpleIdMapper::Put(INTEGER_ID nid, const std::string& sid) {
    ids_[nid] = sid;
    return SERVER_SUCCESS;
}

ServerError SimpleIdMapper::Put(const std::vector<INTEGER_ID>& nid, const std::vector<std::string>& sid) {
    return Put(nid.data(), nid.size(), sid);
}

ServerError SimpleIdMapper::Put(const INTEGER_ID *nid, uint64_t count, const std::vector<std::string>& sid) {
    if(count != sid.size()) {
        return SERVER_INVALID_ARGUMENT;
    }

    for(int64_t i = 0; i < count; i++) {
        ids_[nid[i]] = sid[i];
    }

    return SERVER_SUCCESS;
}

ServerError SimpleIdMapper::Get(INTEGER_ID nid, std::string& sid) const {
    auto iter = ids_.find(nid);
    if(iter == ids_.end()) {
        return SERVER_INVALID_ARGUMENT;
    }

    sid = iter->second;

    return SERVER_SUCCESS;
}

ServerError SimpleIdMapper::Get(const std::vector<INTEGER_ID>& nid, std::vector<std::string>& sid) const {
    return Get(nid.data(), nid.size(), sid);
}

ServerError SimpleIdMapper::Get(const INTEGER_ID *nid, uint64_t count, std::vector<std::string>& sid) const {
    sid.clear();

    ServerError err = SERVER_SUCCESS;
    for(uint64_t i = 0; i < count; i++) {
        auto iter = ids_.find(nid[i]);
        if(iter == ids_.end()) {
            sid.push_back("");
            SERVER_LOG_ERROR << "Failed to find id: " << nid[i];
            err = SERVER_INVALID_ARGUMENT;
            continue;
        }

        sid.push_back(iter->second);
    }

    return err;
}

ServerError SimpleIdMapper::Delete(INTEGER_ID nid) {
    ids_.erase(nid);
    return SERVER_SUCCESS;
}

}
}
}