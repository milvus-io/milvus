/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "VecServiceHandler.h"
#include "VecServiceTask.h"
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

    BaseTaskPtr task_ptr = AddGroupTask::Create(group.dimension, group.id);
    VecServiceScheduler& scheduler = VecServiceScheduler::GetInstance();
    scheduler.ExecuteTask(task_ptr);

    SERVER_LOG_INFO << "add_group() finished";
}

void
VecServiceHandler::get_group(VecGroup &_return, const std::string &group_id) {
    SERVER_LOG_INFO << "get_group() called";
    SERVER_LOG_TRACE << "group_id = " << group_id;

    _return.id = group_id;
    BaseTaskPtr task_ptr = GetGroupTask::Create(group_id, _return.dimension);
    VecServiceScheduler& scheduler = VecServiceScheduler::GetInstance();
    scheduler.ExecuteTask(task_ptr);

    SERVER_LOG_INFO << "get_group() finished";
}

void
VecServiceHandler::del_group(const std::string &group_id) {
    SERVER_LOG_INFO << "del_group() called";
    SERVER_LOG_TRACE << "group_id = " << group_id;

    BaseTaskPtr task_ptr = DeleteGroupTask::Create(group_id);
    VecServiceScheduler& scheduler = VecServiceScheduler::GetInstance();
    scheduler.ExecuteTask(task_ptr);

    SERVER_LOG_INFO << "del_group() not implemented";
}


void
VecServiceHandler::add_vector(const std::string &group_id, const VecTensor &tensor) {
    SERVER_LOG_INFO << "add_vector() called";
    SERVER_LOG_TRACE << "group_id = " << group_id << ", vector size = " << tensor.tensor.size();

    VecTensorList tensor_list;
    tensor_list.tensor_list.push_back(tensor);
    BaseTaskPtr task_ptr = AddVectorTask::Create(group_id, tensor_list);
    VecServiceScheduler& scheduler = VecServiceScheduler::GetInstance();
    scheduler.ExecuteTask(task_ptr);

    SERVER_LOG_INFO << "add_vector() finished";
}

void
VecServiceHandler::add_vector_batch(const std::string &group_id,
                                    const VecTensorList &tensor_list) {
    SERVER_LOG_INFO << "add_vector_batch() called";
    SERVER_LOG_TRACE << "group_id = " << group_id << ", vector list size = "
                     << tensor_list.tensor_list.size();

    BaseTaskPtr task_ptr = AddVectorTask::Create(group_id, tensor_list);
    VecServiceScheduler& scheduler = VecServiceScheduler::GetInstance();
    scheduler.ExecuteTask(task_ptr);

    SERVER_LOG_INFO << "add_vector_batch() finished";
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
                std::string nid_prefix = group_id + "_";
                for(auto id : results[0]) {
                    std::string sid;
                    std::string nid = nid_prefix + std::to_string(id);
                    IVecIdMapper::GetInstance()->Get(nid, sid);
                    _return.id_list.push_back(sid);
                    _return.distance_list.push_back(0.0);//TODO: return distance
                }
            }
        }


    } catch (std::exception& ex) {
        SERVER_LOG_ERROR << ex.what();
    }

    SERVER_LOG_INFO << "search_vector() finished";
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
                VecSearchResult v_res;
                std::string nid_prefix = group_id + "_";
                for(auto id : results[0]) {
                    std::string sid;
                    std::string nid = nid_prefix + std::to_string(id);
                    IVecIdMapper::GetInstance()->Get(nid, sid);
                    v_res.id_list.push_back(sid);
                    v_res.distance_list.push_back(0.0);//TODO: return distance
                }

                _return.result_list.push_back(v_res);
            }
        }


    } catch (std::exception& ex) {
        SERVER_LOG_ERROR << ex.what();
    }

    SERVER_LOG_INFO << "search_vector_batch() finished";
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
