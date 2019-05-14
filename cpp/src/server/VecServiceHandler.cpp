/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "VecServiceHandler.h"
#include "VecServiceTask.h"
#include "ServerConfig.h"
#include "utils/Log.h"
#include "utils/CommonUtil.h"
#include "utils/TimeRecorder.h"

#include "db/DB.h"
#include "db/Env.h"

namespace zilliz {
namespace vecwise {
namespace server {

using namespace megasearch;

namespace {
    class TimeRecordWrapper {
    public:
        TimeRecordWrapper(const std::string& func_name)
        : recorder_(func_name), func_name_(func_name) {
            //SERVER_LOG_TRACE << func_name << " called";
        }

        ~TimeRecordWrapper() {
            recorder_.Elapse("cost");
            //SERVER_LOG_TRACE << func_name_ << " finished";
        }

    private:
        TimeRecorder recorder_;
        std::string func_name_;
    };
    void TimeRecord(const std::string& func_name) {

    }

    const std::map<ServerError, VecErrCode::type>& ErrorMap() {
        static const std::map<ServerError, VecErrCode::type> code_map = {
            {SERVER_UNEXPECTED_ERROR, VecErrCode::ILLEGAL_ARGUMENT},
            {SERVER_NULL_POINTER, VecErrCode::ILLEGAL_ARGUMENT},
            {SERVER_INVALID_ARGUMENT, VecErrCode::ILLEGAL_ARGUMENT},
            {SERVER_FILE_NOT_FOUND, VecErrCode::ILLEGAL_ARGUMENT},
            {SERVER_NOT_IMPLEMENT, VecErrCode::ILLEGAL_ARGUMENT},
            {SERVER_BLOCKING_QUEUE_EMPTY, VecErrCode::ILLEGAL_ARGUMENT},
            {SERVER_GROUP_NOT_EXIST, VecErrCode::GROUP_NOT_EXISTS},
            {SERVER_INVALID_TIME_RANGE, VecErrCode::ILLEGAL_TIME_RANGE},
            {SERVER_INVALID_VECTOR_DIMENSION, VecErrCode::ILLEGAL_VECTOR_DIMENSION},
        };

        return code_map;
    }

    const std::map<ServerError, std::string>& ErrorMessage() {
        static const std::map<ServerError, std::string> msg_map = {
            {SERVER_UNEXPECTED_ERROR, "unexpected error occurs"},
            {SERVER_NULL_POINTER, "null pointer error"},
            {SERVER_INVALID_ARGUMENT, "invalid argument"},
            {SERVER_FILE_NOT_FOUND, "file not found"},
            {SERVER_NOT_IMPLEMENT, "not implemented"},
            {SERVER_BLOCKING_QUEUE_EMPTY, "queue empty"},
            {SERVER_GROUP_NOT_EXIST, "group not exist"},
            {SERVER_INVALID_TIME_RANGE, "invalid time range"},
            {SERVER_INVALID_VECTOR_DIMENSION, "invalid vector dimension"},
        };

        return msg_map;
    }

    void ExecTask(BaseTaskPtr& task_ptr) {
        if(task_ptr == nullptr) {
            return;
        }

        VecServiceScheduler& scheduler = VecServiceScheduler::GetInstance();
        scheduler.ExecuteTask(task_ptr);

        if(!task_ptr->IsAsync()) {
            task_ptr->WaitToFinish();
            ServerError err = task_ptr->ErrorCode();
            if (err != SERVER_SUCCESS) {
                VecException ex;
                ex.__set_code(ErrorMap().at(err));
                std::string msg = task_ptr->ErrorMsg();
                if(msg.empty()){
                    msg = ErrorMessage().at(err);
                }
                ex.__set_reason(msg);
                throw ex;
            }
        }
    }
}

void
VecServiceHandler::add_group(const VecGroup &group) {
    std::string info = "add_group() " + group.id + " dimension = " + std::to_string(group.dimension)
            + " index_type = " + std::to_string(group.index_type);
    TimeRecordWrapper rc(info);

    BaseTaskPtr task_ptr = AddGroupTask::Create(group.dimension, group.id);
    ExecTask(task_ptr);
}

void
VecServiceHandler::get_group(VecGroup &_return, const std::string &group_id) {
    TimeRecordWrapper rc("get_group() " + group_id);

    _return.id = group_id;
    BaseTaskPtr task_ptr = GetGroupTask::Create(group_id, _return.dimension);
    ExecTask(task_ptr);
}

void
VecServiceHandler::del_group(const std::string &group_id) {
    TimeRecordWrapper rc("del_group() " + group_id);

    BaseTaskPtr task_ptr = DeleteGroupTask::Create(group_id);
    ExecTask(task_ptr);
}


void
VecServiceHandler::add_vector(std::string& _return, const std::string &group_id, const VecTensor &tensor) {
    TimeRecordWrapper rc("add_vector() to " + group_id);

    BaseTaskPtr task_ptr = AddVectorTask::Create(group_id, &tensor, _return);
    ExecTask(task_ptr);
}

void
VecServiceHandler::add_vector_batch(std::vector<std::string> & _return,
                                    const std::string &group_id,
                                    const VecTensorList &tensor_list) {
    TimeRecordWrapper rc("add_vector_batch() to " + group_id);

    BaseTaskPtr task_ptr = AddBatchVectorTask::Create(group_id, &tensor_list, _return);
    ExecTask(task_ptr);
}

void
VecServiceHandler::add_binary_vector(std::string& _return,
                                     const std::string& group_id,
                                     const VecBinaryTensor& tensor) {
    TimeRecordWrapper rc("add_binary_vector() to " + group_id);

    BaseTaskPtr task_ptr = AddVectorTask::Create(group_id, &tensor, _return);
    ExecTask(task_ptr);
}

void
VecServiceHandler::add_binary_vector_batch(std::vector<std::string> & _return,
                                           const std::string& group_id,
                                           const VecBinaryTensorList& tensor_list) {
    TimeRecordWrapper rc("add_binary_vector_batch() to " + group_id);

    BaseTaskPtr task_ptr = AddBatchVectorTask::Create(group_id, &tensor_list, _return);
    ExecTask(task_ptr);
}

void
VecServiceHandler::search_vector(VecSearchResult &_return,
                                 const std::string &group_id,
                                 const int64_t top_k,
                                 const VecTensor &tensor,
                                 const VecSearchFilter& filter) {
    TimeRecordWrapper rc("search_vector() in " + group_id);

    VecTensorList tensor_list;
    tensor_list.tensor_list.push_back(tensor);
    VecSearchResultList result;
    BaseTaskPtr task_ptr = SearchVectorTask::Create(group_id, top_k, &tensor_list, filter, result);
    ExecTask(task_ptr);

    if(!result.result_list.empty()) {
        _return = result.result_list[0];
    } else {
        SERVER_LOG_ERROR << "No search result returned";
    }
}

void
VecServiceHandler::search_vector_batch(VecSearchResultList &_return,
                                       const std::string &group_id,
                                       const int64_t top_k,
                                       const VecTensorList &tensor_list,
                                       const VecSearchFilter& filter) {
    TimeRecordWrapper rc("search_vector_batch() in " + group_id);

    BaseTaskPtr task_ptr = SearchVectorTask::Create(group_id, top_k, &tensor_list, filter, _return);
    ExecTask(task_ptr);
}

void
VecServiceHandler::search_binary_vector(VecSearchResult& _return,
                                        const std::string& group_id,
                                        const int64_t top_k,
                                        const VecBinaryTensor& tensor,
                                        const VecSearchFilter& filter) {
    TimeRecordWrapper rc("search_binary_vector() in " + group_id);

    VecBinaryTensorList tensor_list;
    tensor_list.tensor_list.push_back(tensor);
    VecSearchResultList result;
    BaseTaskPtr task_ptr = SearchVectorTask::Create(group_id, top_k, &tensor_list, filter, result);
    ExecTask(task_ptr);

    if(!result.result_list.empty()) {
        _return = result.result_list[0];
    } else {
        SERVER_LOG_ERROR << "No search result returned";
    }
}

void
VecServiceHandler::search_binary_vector_batch(VecSearchResultList& _return,
                                              const std::string& group_id,
                                              const int64_t top_k,
                                              const VecBinaryTensorList& tensor_list,
                                              const VecSearchFilter& filter) {
    TimeRecordWrapper rc("search_binary_vector_batch() in " + group_id);

    BaseTaskPtr task_ptr = SearchVectorTask::Create(group_id, top_k, &tensor_list, filter, _return);
    ExecTask(task_ptr);
}


}
}
}
