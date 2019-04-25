/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "VecServiceTask.h"
#include "ServerConfig.h"
#include "VecIdMapper.h"
#include "utils/CommonUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "db/DB.h"
#include "db/Env.h"

namespace zilliz {
namespace vecwise {
namespace server {

static const std::string NORMAL_TASK_GROUP = "normal";
static const std::string SEARCH_TASK_GROUP = "search";

namespace {
    class DBWrapper {
    public:
        DBWrapper() {
            zilliz::vecwise::engine::Options opt;
            ConfigNode& config = ServerConfig::GetInstance().GetConfig(CONFIG_SERVER);
            opt.meta.backend_uri = config.GetValue(CONFIG_SERVER_DB_URL);
            std::string db_path = config.GetValue(CONFIG_SERVER_DB_PATH);
            opt.meta.path = db_path + "/db";

            CommonUtil::CreateDirectory(opt.meta.path);

            zilliz::vecwise::engine::DB::Open(opt, &db_);
            if(db_ == nullptr) {
                SERVER_LOG_ERROR << "Failed to open db";
                throw ServerException(SERVER_NULL_POINTER, "Failed to open db");
            }
        }

        zilliz::vecwise::engine::DB* DB() { return db_; }

    private:
        zilliz::vecwise::engine::DB* db_ = nullptr;
    };

    zilliz::vecwise::engine::DB* DB() {
        static DBWrapper db_wrapper;
        return db_wrapper.DB();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
AddGroupTask::AddGroupTask(int32_t dimension,
                           const std::string& group_id)
: BaseTask(NORMAL_TASK_GROUP),
  dimension_(dimension),
  group_id_(group_id) {

}

BaseTaskPtr AddGroupTask::Create(int32_t dimension,
                                 const std::string& group_id) {
    return std::shared_ptr<BaseTask>(new AddGroupTask(dimension,group_id));
}

ServerError AddGroupTask::OnExecute() {
    try {
        engine::meta::GroupSchema group_info;
        group_info.dimension = (size_t)dimension_;
        group_info.group_id = group_id_;
        engine::Status stat = DB()->add_group(group_info);
        if(!stat.ok()) {
            SERVER_LOG_ERROR << "Engine failed: " << stat.ToString();
        }

    } catch (std::exception& ex) {
        SERVER_LOG_ERROR << ex.what();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
GetGroupTask::GetGroupTask(const std::string& group_id, int32_t&  dimension)
    : BaseTask(NORMAL_TASK_GROUP),
      group_id_(group_id),
      dimension_(dimension) {

}

BaseTaskPtr GetGroupTask::Create(const std::string& group_id, int32_t&  dimension) {
    return std::shared_ptr<BaseTask>(new GetGroupTask(group_id, dimension));
}

ServerError GetGroupTask::OnExecute() {
    try {
        dimension_ = 0;

        engine::meta::GroupSchema group_info;
        group_info.group_id = group_id_;
        engine::Status stat = DB()->get_group(group_info);
        if(!stat.ok()) {
            SERVER_LOG_ERROR << "Engine failed: " << stat.ToString();
        } else {
            dimension_ = (int32_t)group_info.dimension;
        }

    } catch (std::exception& ex) {
        SERVER_LOG_ERROR << ex.what();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
DeleteGroupTask::DeleteGroupTask(const std::string& group_id)
    : BaseTask(NORMAL_TASK_GROUP),
      group_id_(group_id) {

}

BaseTaskPtr DeleteGroupTask::Create(const std::string& group_id) {
    return std::shared_ptr<BaseTask>(new DeleteGroupTask(group_id));
}

ServerError DeleteGroupTask::OnExecute() {
    try {


    } catch (std::exception& ex) {
        SERVER_LOG_ERROR << ex.what();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
AddSingleVectorTask::AddSingleVectorTask(const std::string& group_id,
                                         const VecTensor &tensor)
    : BaseTask(NORMAL_TASK_GROUP),
      group_id_(group_id),
      tensor_(tensor) {

}

BaseTaskPtr AddSingleVectorTask::Create(const std::string& group_id,
                                        const VecTensor &tensor) {
    return std::shared_ptr<BaseTask>(new AddSingleVectorTask(group_id, tensor));
}

ServerError AddSingleVectorTask::OnExecute() {
    try {
        engine::IDNumbers vector_ids;
        std::vector<float> vec_f(tensor_.tensor.begin(), tensor_.tensor.end());
        engine::Status stat = DB()->add_vectors(group_id_, 1, vec_f.data(), vector_ids);
        if(!stat.ok()) {
            SERVER_LOG_ERROR << "Engine failed: " << stat.ToString();
        } else {
            if(vector_ids.empty()) {
                SERVER_LOG_ERROR << "Vector ID not returned";
            } else {
                std::string nid = group_id_ + "_" + std::to_string(vector_ids[0]);
                IVecIdMapper::GetInstance()->Put(nid, tensor_.uid);
                SERVER_LOG_TRACE << "nid = " << vector_ids[0] << ", sid = " << tensor_.uid;
            }
        }

    } catch (std::exception& ex) {
        SERVER_LOG_ERROR << ex.what();
    }
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
AddBatchVectorTask::AddBatchVectorTask(const std::string& group_id,
                             const VecTensorList &tensor_list)
    : BaseTask(NORMAL_TASK_GROUP),
      group_id_(group_id),
      tensor_list_(tensor_list) {

}

BaseTaskPtr AddBatchVectorTask::Create(const std::string& group_id,
                                  const VecTensorList &tensor_list) {
    return std::shared_ptr<BaseTask>(new AddBatchVectorTask(group_id, tensor_list));
}

ServerError AddBatchVectorTask::OnExecute() {
    try {
        TimeRecorder rc("Add vector batch");

        engine::meta::GroupSchema group_info;
        group_info.group_id = group_id_;
        engine::Status stat = DB()->get_group(group_info);
        if(!stat.ok()) {
            SERVER_LOG_ERROR << "Engine failed: " << stat.ToString();
            return SERVER_UNEXPECTED_ERROR;
        }

        std::vector<float> vec_f;
        vec_f.reserve(tensor_list_.tensor_list.size()*group_info.dimension*4);
        for(const VecTensor& tensor : tensor_list_.tensor_list) {
            if(tensor.tensor.size() != group_info.dimension) {
                SERVER_LOG_ERROR << "Invalid vector data size: " << tensor.tensor.size()
                << " vs. group dimension:" << group_info.dimension;
                return SERVER_UNEXPECTED_ERROR;
            }
            vec_f.insert(vec_f.begin(), tensor.tensor.begin(), tensor.tensor.end());
        }
        rc.Record("prepare vectors data");

        engine::IDNumbers vector_ids;
        stat = DB()->add_vectors(group_id_, tensor_list_.tensor_list.size(), vec_f.data(), vector_ids);
        rc.Record("add vectors to engine");
        if(!stat.ok()) {
            SERVER_LOG_ERROR << "Engine failed: " << stat.ToString();
        } else {
            if(vector_ids.size() < tensor_list_.tensor_list.size()) {
                SERVER_LOG_ERROR << "Vector ID not returned";
            } else {
                std::string nid_prefix = group_id_ + "_";
                for(size_t i = 0; i < tensor_list_.tensor_list.size(); i++) {
                    std::string nid = nid_prefix + std::to_string(vector_ids[i]);
                    IVecIdMapper::GetInstance()->Put(nid, tensor_list_.tensor_list[i].uid);
                }
                rc.Record("build id mapping");
            }
        }

    } catch (std::exception& ex) {
        SERVER_LOG_ERROR << ex.what();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SearchVectorTask::SearchVectorTask(const std::string& group_id,
                                   const int64_t top_k,
                                   const VecTensorList& tensor_list,
                                   const VecTimeRangeList& time_range_list,
                                   VecSearchResultList& result)
    : BaseTask(SEARCH_TASK_GROUP),
      group_id_(group_id),
      top_k_(top_k),
      tensor_list_(tensor_list),
      time_range_list_(time_range_list),
      result_(result) {

}

BaseTaskPtr SearchVectorTask::Create(const std::string& group_id,
                                     const int64_t top_k,
                                     const VecTensorList& tensor_list,
                                     const VecTimeRangeList& time_range_list,
                                     VecSearchResultList& result) {
    return std::shared_ptr<BaseTask>(new SearchVectorTask(group_id, top_k, tensor_list, time_range_list, result));
}

ServerError SearchVectorTask::OnExecute() {
    try {
        std::vector<float> vec_f;
        for(const VecTensor& tensor : tensor_list_.tensor_list) {
            vec_f.insert(vec_f.begin(), tensor.tensor.begin(), tensor.tensor.end());
        }

        engine::QueryResults results;
        engine::Status stat = DB()->search(group_id_, (size_t)top_k_, tensor_list_.tensor_list.size(), vec_f.data(), results);
        if(!stat.ok()) {
            SERVER_LOG_ERROR << "Engine failed: " << stat.ToString();
        } else {
            for(engine::QueryResult& res : results){
                VecSearchResult v_res;
                std::string nid_prefix = group_id_ + "_";
                for(auto id : results[0]) {
                    std::string sid;
                    std::string nid = nid_prefix + std::to_string(id);
                    IVecIdMapper::GetInstance()->Get(nid, sid);
                    v_res.id_list.push_back(sid);
                    v_res.distance_list.push_back(0.0);//TODO: return distance

                    SERVER_LOG_TRACE << "nid = " << nid << ", string id = " << sid;

                }

                result_.result_list.push_back(v_res);
            }
        }

    } catch (std::exception& ex) {
        SERVER_LOG_ERROR << ex.what();
    }
}

}
}
}
