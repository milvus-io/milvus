/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "VecServiceScheduler.h"
#include "utils/Error.h"
#include "db/Types.h"

#include "thrift/gen-cpp/VectorService_types.h"

#include <condition_variable>
#include <memory>

namespace zilliz {
namespace vecwise {
namespace server {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class AddGroupTask : public BaseTask {
public:
    static BaseTaskPtr Create(int32_t dimension,
                              const std::string& group_id);

protected:
    AddGroupTask(int32_t dimension,
                 const std::string& group_id);

    ServerError OnExecute() override;

private:
    int32_t  dimension_;
    std::string group_id_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class GetGroupTask : public BaseTask {
public:
    static BaseTaskPtr Create(const std::string& group_id, int32_t&  dimension);

protected:
    GetGroupTask(const std::string& group_id, int32_t&  dimension);

    ServerError OnExecute() override;


private:
    std::string group_id_;
    int32_t&  dimension_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class DeleteGroupTask : public BaseTask {
public:
    static BaseTaskPtr Create(const std::string& group_id);

protected:
    DeleteGroupTask(const std::string& group_id);

    ServerError OnExecute() override;


private:
    std::string group_id_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class AddVectorTask : public BaseTask {
public:
    static BaseTaskPtr Create(const std::string& group_id,
                              const VecTensor* tensor);

    static BaseTaskPtr Create(const std::string& group_id,
                              const VecBinaryTensor* tensor);

protected:
    AddVectorTask(const std::string& group_id,
                  const VecTensor* tensor);

    AddVectorTask(const std::string& group_id,
                  const VecBinaryTensor* tensor);

    uint64_t GetVecDimension() const;
    const double* GetVecData() const;
    std::string GetVecID() const;

    ServerError OnExecute() override;

private:
    std::string group_id_;
    const VecTensor* tensor_;
    const VecBinaryTensor* bin_tensor_;
};


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class AddBatchVectorTask : public BaseTask {
public:
    static BaseTaskPtr Create(const std::string& group_id,
                              const VecTensorList* tensor_list);

    static BaseTaskPtr Create(const std::string& group_id,
                              const VecBinaryTensorList* tensor_list);

protected:
    AddBatchVectorTask(const std::string& group_id,
                  const VecTensorList* tensor_list);

    AddBatchVectorTask(const std::string& group_id,
                       const VecBinaryTensorList* tensor_list);

    uint64_t GetVecListCount() const;
    uint64_t GetVecDimension(uint64_t index) const;
    const double* GetVecData(uint64_t index) const;
    std::string GetVecID(uint64_t index) const;

    ServerError OnExecute() override;

private:
    std::string group_id_;
    const VecTensorList* tensor_list_;
    const VecBinaryTensorList* bin_tensor_list_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class SearchVectorTask : public BaseTask {
public:
    static BaseTaskPtr Create(const std::string& group_id,
                              const int64_t top_k,
                              const VecTensorList* tensor_list,
                              const VecTimeRangeList& time_range_list,
                              VecSearchResultList& result);

    static BaseTaskPtr Create(const std::string& group_id,
                              const int64_t top_k,
                              const VecBinaryTensorList* bin_tensor_list,
                              const VecTimeRangeList& time_range_list,
                              VecSearchResultList& result);

protected:
    SearchVectorTask(const std::string& group_id,
                     const int64_t top_k,
                     const VecTensorList* tensor_list,
                     const VecTimeRangeList& time_range_list,
                     VecSearchResultList& result);

    SearchVectorTask(const std::string& group_id,
                     const int64_t top_k,
                     const VecBinaryTensorList* bin_tensor_list,
                     const VecTimeRangeList& time_range_list,
                     VecSearchResultList& result);

    ServerError GetTargetData(std::vector<float>& data) const;
    uint64_t GetTargetDimension() const;
    uint64_t GetTargetCount() const;

    ServerError OnExecute() override;

private:
    std::string group_id_;
    int64_t top_k_;
    const VecTensorList* tensor_list_;
    const VecBinaryTensorList* bin_tensor_list_;
    const VecTimeRangeList& time_range_list_;
    VecSearchResultList& result_;
};

}
}
}