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

static const std::string DQL_TASK_GROUP = "dql";
static const std::string DDL_DML_TASK_GROUP = "ddl_dml";

static const std::string VECTOR_UID = "uid";

namespace {
    class DBWrapper {
    public:
        DBWrapper() {
            zilliz::vecwise::engine::Options opt;
            ConfigNode& config = ServerConfig::GetInstance().GetConfig(CONFIG_DB);
            opt.meta.backend_uri = config.GetValue(CONFIG_DB_URL);
            std::string db_path = config.GetValue(CONFIG_DB_PATH);
            opt.memory_sync_interval = (uint16_t)config.GetInt32Value(CONFIG_DB_FLUSH_INTERVAL, 10);
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
: BaseTask(DDL_DML_TASK_GROUP),
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
            return SERVER_UNEXPECTED_ERROR;
        }

    } catch (std::exception& ex) {
        SERVER_LOG_ERROR << ex.what();
        return SERVER_UNEXPECTED_ERROR;
    }

    return SERVER_SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
GetGroupTask::GetGroupTask(const std::string& group_id, int32_t&  dimension)
    : BaseTask(DDL_DML_TASK_GROUP),
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
            return SERVER_UNEXPECTED_ERROR;
        } else {
            dimension_ = (int32_t)group_info.dimension;
        }

    } catch (std::exception& ex) {
        SERVER_LOG_ERROR << ex.what();
        return SERVER_UNEXPECTED_ERROR;
    }

    return SERVER_SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
DeleteGroupTask::DeleteGroupTask(const std::string& group_id)
    : BaseTask(DDL_DML_TASK_GROUP),
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

    return SERVER_SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
AddVectorTask::AddVectorTask(const std::string& group_id,
                             const VecTensor* tensor)
    : BaseTask(DDL_DML_TASK_GROUP),
      group_id_(group_id),
      tensor_(tensor),
      bin_tensor_(nullptr){

}

BaseTaskPtr AddVectorTask::Create(const std::string& group_id,
                                  const VecTensor* tensor) {
    return std::shared_ptr<BaseTask>(new AddVectorTask(group_id, tensor));
}

AddVectorTask::AddVectorTask(const std::string& group_id,
                             const VecBinaryTensor* tensor)
        : BaseTask(DDL_DML_TASK_GROUP),
          group_id_(group_id),
          tensor_(nullptr),
          bin_tensor_(tensor) {

}

BaseTaskPtr AddVectorTask::Create(const std::string& group_id,
                                  const VecBinaryTensor* tensor) {
    return std::shared_ptr<BaseTask>(new AddVectorTask(group_id, tensor));
}


uint64_t AddVectorTask::GetVecDimension() const {
    if(tensor_) {
        return (uint64_t) tensor_->tensor.size();
    } else if(bin_tensor_) {
        return (uint64_t) bin_tensor_->tensor.size()/8;
    } else {
        return 0;
    }
}

const double* AddVectorTask::GetVecData() const {
    if(tensor_) {
        return (const double*)(tensor_->tensor.data());
    } else if(bin_tensor_) {
        return (const double*)(bin_tensor_->tensor.data());
    } else {
        return nullptr;
    }

}

std::string AddVectorTask::GetVecID() const {
    if(tensor_) {
        return tensor_->uid;
    } else if(bin_tensor_) {
        return bin_tensor_->uid;
    } else {
        return "";
    }
}

const AttribMap& AddVectorTask::GetVecAttrib() const {
    if(tensor_) {
        return tensor_->attrib;
    } else {
        return bin_tensor_->attrib;
    }
}

ServerError AddVectorTask::OnExecute() {
    try {
        engine::meta::GroupSchema group_info;
        group_info.group_id = group_id_;
        engine::Status stat = DB()->get_group(group_info);
        if(!stat.ok()) {
            SERVER_LOG_ERROR << "Engine failed: " << stat.ToString();
            return SERVER_INVALID_ARGUMENT;
        }

        uint64_t group_dim = group_info.dimension;
        uint64_t vec_dim = GetVecDimension();
        if(group_dim != vec_dim) {
            SERVER_LOG_ERROR << "Invalid vector dimension: " << vec_dim
                             << " vs. group dimension:" << group_dim;
            return SERVER_INVALID_ARGUMENT;
        }

        std::vector<float> vec_f;
        vec_f.resize(vec_dim);
        const double* d_p = GetVecData();
        for(uint64_t d = 0; d < vec_dim; d++) {
            vec_f[d] = (float)(d_p[d]);
        }

        engine::IDNumbers vector_ids;
        stat = DB()->add_vectors(group_id_, 1, vec_f.data(), vector_ids);
        if(!stat.ok()) {
            SERVER_LOG_ERROR << "Engine failed: " << stat.ToString();
            return SERVER_UNEXPECTED_ERROR;
        } else {
            if(vector_ids.empty()) {
                SERVER_LOG_ERROR << "Vector ID not returned";
                return SERVER_UNEXPECTED_ERROR;
            } else {
                std::string uid = GetVecID();
                std::string nid = group_id_ + "_" + std::to_string(vector_ids[0]);
                AttribMap attrib = GetVecAttrib();
                attrib[VECTOR_UID] = uid;
                std::string attrib_str;
                AttributeSerializer::Encode(attrib, attrib_str);
                IVecIdMapper::GetInstance()->Put(nid, attrib_str);
                SERVER_LOG_TRACE << "nid = " << vector_ids[0] << ", uid = " << uid;
            }
        }

    } catch (std::exception& ex) {
        SERVER_LOG_ERROR << ex.what();
        return SERVER_UNEXPECTED_ERROR;
    }

    return SERVER_SUCCESS;
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
AddBatchVectorTask::AddBatchVectorTask(const std::string& group_id,
                                       const VecTensorList* tensor_list)
    : BaseTask(DDL_DML_TASK_GROUP),
      group_id_(group_id),
      tensor_list_(tensor_list),
      bin_tensor_list_(nullptr) {

}

BaseTaskPtr AddBatchVectorTask::Create(const std::string& group_id,
                                       const VecTensorList* tensor_list) {
    return std::shared_ptr<BaseTask>(new AddBatchVectorTask(group_id, tensor_list));
}

AddBatchVectorTask::AddBatchVectorTask(const std::string& group_id,
                                       const VecBinaryTensorList* tensor_list)
        : BaseTask(DDL_DML_TASK_GROUP),
          group_id_(group_id),
          tensor_list_(nullptr),
          bin_tensor_list_(tensor_list) {

}

BaseTaskPtr AddBatchVectorTask::Create(const std::string& group_id,
                                       const VecBinaryTensorList* tensor_list) {
    return std::shared_ptr<BaseTask>(new AddBatchVectorTask(group_id, tensor_list));
}

uint64_t AddBatchVectorTask::GetVecListCount() const {
    if(tensor_list_) {
        return (uint64_t) tensor_list_->tensor_list.size();
    } else if(bin_tensor_list_) {
        return (uint64_t) bin_tensor_list_->tensor_list.size();
    } else {
        return 0;
    }
}

uint64_t AddBatchVectorTask::GetVecDimension(uint64_t index) const {
    if(tensor_list_) {
        if(index >= tensor_list_->tensor_list.size()){
            return 0;
        }
        return (uint64_t) tensor_list_->tensor_list[index].tensor.size();
    } else if(bin_tensor_list_) {
        if(index >= bin_tensor_list_->tensor_list.size()){
            return 0;
        }
        return (uint64_t) bin_tensor_list_->tensor_list[index].tensor.size()/8;
    } else {
        return 0;
    }
}

const double* AddBatchVectorTask::GetVecData(uint64_t index) const {
    if(tensor_list_) {
        if(index >= tensor_list_->tensor_list.size()){
            return nullptr;
        }
        return tensor_list_->tensor_list[index].tensor.data();
    } else if(bin_tensor_list_) {
        if(index >= bin_tensor_list_->tensor_list.size()){
            return nullptr;
        }
        return (const double*)bin_tensor_list_->tensor_list[index].tensor.data();
    } else {
        return nullptr;
    }
}

std::string AddBatchVectorTask::GetVecID(uint64_t index) const {
    if(tensor_list_) {
        if(index >= tensor_list_->tensor_list.size()){
            return 0;
        }
        return tensor_list_->tensor_list[index].uid;
    } else if(bin_tensor_list_) {
        if(index >= bin_tensor_list_->tensor_list.size()){
            return 0;
        }
        return bin_tensor_list_->tensor_list[index].uid;
    } else {
        return "";
    }
}

const AttribMap& AddBatchVectorTask::GetVecAttrib(uint64_t index) const {
    if(tensor_list_) {
        return tensor_list_->tensor_list[index].attrib;
    } else {
        return bin_tensor_list_->tensor_list[index].attrib;
    }
}

ServerError AddBatchVectorTask::OnExecute() {
    try {
        TimeRecorder rc("AddBatchVectorTask");

        engine::meta::GroupSchema group_info;
        group_info.group_id = group_id_;
        engine::Status stat = DB()->get_group(group_info);
        if(!stat.ok()) {
            SERVER_LOG_ERROR << "Engine failed: " << stat.ToString();
            return SERVER_UNEXPECTED_ERROR;
        }

        rc.Record("check group dimension");

        uint64_t group_dim = group_info.dimension;
        uint64_t vec_count = GetVecListCount();
        std::vector<float> vec_f;
        vec_f.resize(vec_count*group_dim);//allocate enough memory
        for(uint64_t i = 0; i < vec_count; i ++) {
            uint64_t vec_dim = GetVecDimension(i);
            if(vec_dim != group_dim) {
                SERVER_LOG_ERROR << "Invalid vector dimension: " << vec_dim
                                 << " vs. group dimension:" << group_dim;
                return SERVER_INVALID_ARGUMENT;
            }

            const double* d_p = GetVecData(i);
            for(uint64_t d = 0; d < vec_dim; d++) {
                vec_f[i*vec_dim + d] = (float)(d_p[d]);
            }
        }

        rc.Record("prepare vectors data");

        engine::IDNumbers vector_ids;
        stat = DB()->add_vectors(group_id_, vec_count, vec_f.data(), vector_ids);
        rc.Record("add vectors to engine");
        if(!stat.ok()) {
            SERVER_LOG_ERROR << "Engine failed: " << stat.ToString();
        } else {
            if(vector_ids.size() < vec_count) {
                SERVER_LOG_ERROR << "Vector ID not returned";
                return SERVER_UNEXPECTED_ERROR;
            } else {
                std::string nid_prefix = group_id_ + "_";
                for(size_t i = 0; i < vec_count; i++) {
                    std::string uid = GetVecID(i);
                    std::string nid = nid_prefix + std::to_string(vector_ids[i]);
                    AttribMap attrib = GetVecAttrib(i);
                    attrib[VECTOR_UID] = uid;
                    std::string attrib_str;
                    AttributeSerializer::Encode(attrib, attrib_str);
                    IVecIdMapper::GetInstance()->Put(nid, attrib_str);
                }
                rc.Record("build id mapping");
            }
        }

    } catch (std::exception& ex) {
        SERVER_LOG_ERROR << ex.what();
        return SERVER_UNEXPECTED_ERROR;
    }

    return SERVER_SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SearchVectorTask::SearchVectorTask(const std::string& group_id,
                                   const int64_t top_k,
                                   const VecTensorList* tensor_list,
                                   const VecSearchFilter& filter,
                                   VecSearchResultList& result)
    : BaseTask(DQL_TASK_GROUP),
      group_id_(group_id),
      top_k_(top_k),
      tensor_list_(tensor_list),
      bin_tensor_list_(nullptr),
      filter_(filter),
      result_(result) {

}

SearchVectorTask::SearchVectorTask(const std::string& group_id,
                                   const int64_t top_k,
                                   const VecBinaryTensorList* bin_tensor_list,
                                   const VecSearchFilter& filter,
                                   VecSearchResultList& result)
    : BaseTask(DQL_TASK_GROUP),
      group_id_(group_id),
      top_k_(top_k),
      tensor_list_(nullptr),
      bin_tensor_list_(bin_tensor_list),
      filter_(filter),
      result_(result) {

}

BaseTaskPtr SearchVectorTask::Create(const std::string& group_id,
                                     const int64_t top_k,
                                     const VecTensorList* tensor_list,
                                     const VecSearchFilter& filter,
                                     VecSearchResultList& result) {
    return std::shared_ptr<BaseTask>(new SearchVectorTask(group_id, top_k, tensor_list, filter, result));
}

BaseTaskPtr SearchVectorTask::Create(const std::string& group_id,
                                     const int64_t top_k,
                                     const VecBinaryTensorList* bin_tensor_list,
                                     const VecSearchFilter& filter,
                                     VecSearchResultList& result) {
    return std::shared_ptr<BaseTask>(new SearchVectorTask(group_id, top_k, bin_tensor_list, filter, result));
}


ServerError SearchVectorTask::GetTargetData(std::vector<float>& data) const {
    if(tensor_list_ && !tensor_list_->tensor_list.empty()) {
        uint64_t count = tensor_list_->tensor_list.size();
        uint64_t dim = tensor_list_->tensor_list[0].tensor.size();
        data.resize(count*dim);
        for(size_t i = 0; i < count; i++) {
            if(tensor_list_->tensor_list[i].tensor.size() != dim) {
                SERVER_LOG_ERROR << "Invalid vector dimension: " << tensor_list_->tensor_list[i].tensor.size();
                return SERVER_INVALID_ARGUMENT;
            }
            const double* d_p = tensor_list_->tensor_list[i].tensor.data();
            for(int64_t k = 0; k < dim; k++) {
                data[i*dim + k] = (float)(d_p[k]);
            }
        }
    } else if(bin_tensor_list_ && !bin_tensor_list_->tensor_list.empty()) {
        uint64_t count = bin_tensor_list_->tensor_list.size();
        uint64_t dim = bin_tensor_list_->tensor_list[0].tensor.size()/8;
        data.resize(count*dim);
        for(size_t i = 0; i < count; i++) {
            if(bin_tensor_list_->tensor_list[i].tensor.size()/8 != dim) {
                SERVER_LOG_ERROR << "Invalid vector dimension: " << bin_tensor_list_->tensor_list[i].tensor.size()/8;
                return SERVER_INVALID_ARGUMENT;
            }
            const double* d_p = (const double*)(bin_tensor_list_->tensor_list[i].tensor.data());
            for(int64_t k = 0; k < dim; k++) {
                data[i*dim + k] = (float)(d_p[k]);
            }
        }
    }

    return SERVER_SUCCESS;
}

uint64_t SearchVectorTask::GetTargetDimension() const {
    if(tensor_list_ && !tensor_list_->tensor_list.empty()) {
        return tensor_list_->tensor_list[0].tensor.size();
    } else if(bin_tensor_list_ && !bin_tensor_list_->tensor_list.empty()) {
        return bin_tensor_list_->tensor_list[0].tensor.size()/8;
    }

    return 0;
}

uint64_t SearchVectorTask::GetTargetCount() const {
    if(tensor_list_) {
        return tensor_list_->tensor_list.size();
    } else if(bin_tensor_list_) {
        return bin_tensor_list_->tensor_list.size();
    }
}

ServerError SearchVectorTask::OnExecute() {
    try {
        TimeRecorder rc("SearchVectorTask");

        engine::meta::GroupSchema group_info;
        group_info.group_id = group_id_;
        engine::Status stat = DB()->get_group(group_info);
        if(!stat.ok()) {
            SERVER_LOG_ERROR << "Engine failed: " << stat.ToString();
            return SERVER_UNEXPECTED_ERROR;
        }

        uint64_t vec_dim = GetTargetDimension();
        if(vec_dim != group_info.dimension) {
            SERVER_LOG_ERROR << "Invalid vector dimension: " << vec_dim
                             << " vs. group dimension:" << group_info.dimension;
            return SERVER_INVALID_ARGUMENT;
        }

        rc.Record("check group dimension");

        std::vector<float> vec_f;
        ServerError err = GetTargetData(vec_f);
        if(err != SERVER_SUCCESS) {
            return err;
        }

        uint64_t vec_count = GetTargetCount();

        rc.Record("prepare input data");

        engine::QueryResults results;
        stat = DB()->search(group_id_, (size_t)top_k_, vec_count, vec_f.data(), results);
        if(!stat.ok()) {
            SERVER_LOG_ERROR << "Engine failed: " << stat.ToString();
            return SERVER_UNEXPECTED_ERROR;
        } else {
            rc.Record("do search");
            for(engine::QueryResult& res : results){
                VecSearchResult v_res;
                std::string nid_prefix = group_id_ + "_";
                for(auto id : res) {
                    std::string attrib_str;
                    std::string nid = nid_prefix + std::to_string(id);
                    IVecIdMapper::GetInstance()->Get(nid, attrib_str);

                    AttribMap attrib_map;
                    AttributeSerializer::Decode(attrib_str, attrib_map);

                    VecSearchResultItem item;
                    item.__set_attrib(attrib_map);
                    item.uid = item.attrib[VECTOR_UID];
                    item.distance = 0.0;////TODO: return distance
                    v_res.result_list.emplace_back(item);

                    SERVER_LOG_TRACE << "nid = " << nid << ", uid = " << item.uid;
                }

                result_.result_list.push_back(v_res);
            }
            rc.Record("construct result");
        }

    } catch (std::exception& ex) {
        SERVER_LOG_ERROR << ex.what();
        return SERVER_UNEXPECTED_ERROR;
    }

    return SERVER_SUCCESS;
}

}
}
}
