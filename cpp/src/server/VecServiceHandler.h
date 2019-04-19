/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once
/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "utils/Error.h"
#include "thrift/gen-cpp/VecService.h"

#include <cstdint>
#include <string>

namespace zilliz {
namespace vecwise {
namespace engine {
    class DB;
}
}
}

namespace zilliz {
namespace vecwise {
namespace server {

class VecServiceHandler : virtual public VecServiceIf {
public:
    VecServiceHandler();

    /**
     * group interfaces
     *
     * @param group
     */
    void add_group(const VecGroup& group);

    void get_group(VecGroup& _return, const std::string& group_id);

    void del_group(const std::string& group_id);

    /**
     * vector interfaces
     *
     *
     * @param group_id
     * @param tensor
     */
    void add_vector(VecTensorIdList& _return, const std::string& group_id, const VecTensor& tensor);

    void add_vector_batch(VecTensorIdList& _return, const std::string& group_id, const VecTensorList& tensor_list);

    /**
     * search interfaces
     * if time_range_list is empty, engine will search without time limit
     *
     * @param group_id
     * @param top_k
     * @param tensor
     * @param time_range_list
     */
    void search_vector(VecSearchResult& _return,
                       const std::string& group_id,
                       const int64_t top_k,
                       const VecTensor& tensor,
                       const VecTimeRangeList& time_range_list);

    void search_vector_batch(VecSearchResultList& _return,
                             const std::string& group_id,
                             const int64_t top_k,
                             const VecTensorList& tensor_list,
                             const VecTimeRangeList& time_range_list);

    ~VecServiceHandler();

private:
    zilliz::vecwise::engine::DB* db_;

};


}
}
}
