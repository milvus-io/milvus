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
#include "utils/TimeRecorder.h"

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

    BaseTaskPtr task_ptr = AddSingleVectorTask::Create(group_id, tensor);
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
    TimeRecorder rc("Add VECTOR BATCH");
    BaseTaskPtr task_ptr = AddBatchVectorTask::Create(group_id, tensor_list);
    VecServiceScheduler& scheduler = VecServiceScheduler::GetInstance();
    scheduler.ExecuteTask(task_ptr);
    rc.Elapse("DONE!");

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

    VecTensorList tensor_list;
    tensor_list.tensor_list.push_back(tensor);
    VecSearchResultList result;
    BaseTaskPtr task_ptr = SearchVectorTask::Create(group_id, top_k, tensor_list, time_range_list, result);
    VecServiceScheduler& scheduler = VecServiceScheduler::GetInstance();
    scheduler.ExecuteTask(task_ptr);

    if(!result.result_list.empty()) {
        _return = result.result_list[0];
    } else {
        SERVER_LOG_ERROR << "No search result returned";
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

    BaseTaskPtr task_ptr = SearchVectorTask::Create(group_id, top_k, tensor_list, time_range_list, _return);
    VecServiceScheduler& scheduler = VecServiceScheduler::GetInstance();
    scheduler.ExecuteTask(task_ptr);

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
