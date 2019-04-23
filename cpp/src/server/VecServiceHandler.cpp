/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "VecServiceHandler.h"
#include "ServerConfig.h"
#include "VecIdMapper.h"
#include "utils/Log.h"
#include "utils/CommonUtil.h"

#include "db/DB.h"
#include "db/Env.h"

namespace zilliz {
namespace vecwise {
namespace server {

VecServiceHandler::VecServiceHandler() {
    zilliz::vecwise::engine::Options opt;
    ConfigNode& config = ServerConfig::GetInstance().GetConfig(CONFIG_SERVER);
    opt.meta.backend_uri = config.GetValue(CONFIG_SERVER_DB_URL);
    std::string db_path = config.GetValue(CONFIG_SERVER_DB_PATH);
    opt.meta.path = db_path + "/db";

    CommonUtil::CreateDirectory(opt.meta.path);

    zilliz::vecwise::engine::DB::Open(opt, &db_);
}

void
VecServiceHandler::add_group(const VecGroup &group) {
    SERVER_LOG_INFO << "add_group() called";
    SERVER_LOG_TRACE << "group.id = " << group.id << ", group.dimension = " << group.dimension
                        << ", group.index_type = " << group.index_type;

    try {
        engine::meta::GroupSchema group_info;
        group_info.dimension = (size_t)group.dimension;
        group_info.group_id = group.id;
        engine::Status stat = db_->add_group(group_info);
        if(!stat.ok()) {
            SERVER_LOG_ERROR << "Engine failed: " << stat.ToString();
        }

        SERVER_LOG_INFO << "add_group() finished";
    } catch (std::exception& ex) {
        SERVER_LOG_ERROR << ex.what();
    }
}

void
VecServiceHandler::get_group(VecGroup &_return, const std::string &group_id) {
    SERVER_LOG_INFO << "get_group() called";
    SERVER_LOG_TRACE << "group_id = " << group_id;

    try {
        engine::meta::GroupSchema group_info;
        group_info.group_id = group_id;
        engine::Status stat = db_->get_group(group_info);
        if(!stat.ok()) {
            SERVER_LOG_ERROR << "Engine failed: " << stat.ToString();
        } else {
            _return.id = group_info.group_id;
            _return.dimension = (int32_t)group_info.dimension;
        }

        SERVER_LOG_INFO << "get_group() finished";
    } catch (std::exception& ex) {
        SERVER_LOG_ERROR << ex.what();
    }
}

void
VecServiceHandler::del_group(const std::string &group_id) {
    SERVER_LOG_INFO << "del_group() called";
    SERVER_LOG_TRACE << "group_id = " << group_id;

    try {

        SERVER_LOG_INFO << "del_group() not implemented";
    } catch (std::exception& ex) {
        SERVER_LOG_ERROR << ex.what();
    }
}


void
VecServiceHandler::add_vector(const std::string &group_id, const VecTensor &tensor) {
    SERVER_LOG_INFO << "add_vector() called";
    SERVER_LOG_TRACE << "group_id = " << group_id << ", vector size = " << tensor.tensor.size();

    try {
        engine::IDNumbers vector_ids;
        std::vector<float> vec_f(tensor.tensor.begin(), tensor.tensor.end());
        engine::Status stat = db_->add_vectors(group_id, 1, vec_f.data(), vector_ids);
        if(!stat.ok()) {
            SERVER_LOG_ERROR << "Engine failed: " << stat.ToString();
        } else {
            if(vector_ids.size() != 1) {
                SERVER_LOG_ERROR << "Vector ID not returned";
            } else {
                IVecIdMapper::GetInstance()->Put(vector_ids[0], tensor.uid);
            }
        }

        SERVER_LOG_INFO << "add_vector() finished";
    } catch (std::exception& ex) {
        SERVER_LOG_ERROR << ex.what();
    }
}

void
VecServiceHandler::add_vector_batch(const std::string &group_id,
                                    const VecTensorList &tensor_list) {
    SERVER_LOG_INFO << "add_vector_batch() called";
    SERVER_LOG_TRACE << "group_id = " << group_id << ", vector list size = "
                     << tensor_list.tensor_list.size();

    try {
        std::vector<float> vec_f;
        for(const VecTensor& tensor : tensor_list.tensor_list) {
            vec_f.insert(vec_f.begin(), tensor.tensor.begin(), tensor.tensor.end());
        }

        engine::IDNumbers vector_ids;
        engine::Status stat = db_->add_vectors(group_id, tensor_list.tensor_list.size(), vec_f.data(), vector_ids);
        if(!stat.ok()) {
            SERVER_LOG_ERROR << "Engine failed: " << stat.ToString();
        } else {
            if(vector_ids.size() != tensor_list.tensor_list.size()) {
                SERVER_LOG_ERROR << "Vector ID not returned";
            } else {
                for(size_t i = 0; i < vector_ids.size(); i++) {
                    IVecIdMapper::GetInstance()->Put(vector_ids[i], tensor_list.tensor_list[i].uid);
                }
            }
        }

        SERVER_LOG_INFO << "add_vector_batch() finished";
    } catch (std::exception& ex) {
        SERVER_LOG_ERROR << ex.what();
    }
}


void
VecServiceHandler::search_vector(VecSearchResult &_return,
                                 const std::string &group_id,
                                 const int64_t top_k,
                                 const VecTensor &tensor,
                                 const VecTimeRangeList &time_range_list) {
    SERVER_LOG_INFO << "search_vector() called";
    SERVER_LOG_TRACE << "group_id = " << group_id << ", top_k = " << top_k
                        << ", vector size = " << tensor.tensor.size()
                        << ", time range list size = " << time_range_list.range_list.size();

    try {
        engine::QueryResults results;
        std::vector<float> vec_f(tensor.tensor.begin(), tensor.tensor.end());
        engine::Status stat = db_->search(group_id, (size_t)top_k, 1, vec_f.data(), results);
        if(!stat.ok()) {
            SERVER_LOG_ERROR << "Engine failed: " << stat.ToString();
        } else {
            if(!results.empty()) {
                for(auto id : results[0]) {
                    std::string sid;
                    IVecIdMapper::GetInstance()->Get(id, sid);
                    _return.id_list.push_back(sid);
                }
            }
        }

        SERVER_LOG_INFO << "search_vector() finished";
    } catch (std::exception& ex) {
        SERVER_LOG_ERROR << ex.what();
    }
}

void
VecServiceHandler::search_vector_batch(VecSearchResultList &_return,
                                       const std::string &group_id,
                                       const int64_t top_k,
                                       const VecTensorList &tensor_list,
                                       const VecTimeRangeList &time_range_list) {
    SERVER_LOG_INFO << "search_vector_batch() called";
    SERVER_LOG_TRACE << "group_id = " << group_id << ", top_k = " << top_k
                     << ", vector list size = " << tensor_list.tensor_list.size()
                     << ", time range list size = " << time_range_list.range_list.size();

    try {
        std::vector<float> vec_f;
        for(const VecTensor& tensor : tensor_list.tensor_list) {
            vec_f.insert(vec_f.begin(), tensor.tensor.begin(), tensor.tensor.end());
        }

        engine::QueryResults results;
        engine::Status stat = db_->search(group_id, (size_t)top_k, tensor_list.tensor_list.size(), vec_f.data(), results);
        if(!stat.ok()) {
            SERVER_LOG_ERROR << "Engine failed: " << stat.ToString();
        } else {
            for(engine::QueryResult& res : results){
                for(auto nid : results) {
                    VecSearchResult v_res;
                    IVecIdMapper::GetInstance()->Get(nid, v_res.id_list);
                    _return.result_list.push_back(v_res);
                }
            }
        }

        SERVER_LOG_INFO << "search_vector_batch() finished";
    } catch (std::exception& ex) {
        SERVER_LOG_ERROR << ex.what();
    }
}

VecServiceHandler::~VecServiceHandler() {
    if (db_ != nullptr) {
        delete db_;
        db_ = nullptr;
    }
}

}
}
}
