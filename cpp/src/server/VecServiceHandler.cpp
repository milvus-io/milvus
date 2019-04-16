//
// Created by yhmo on 19-4-16.
//

#include "VecServiceHandler.h"
#include "utils/Log.h"

namespace zilliz {
namespace vecwise {
namespace server {

VecServiceHandler::VecServiceHandler() {
}

void
VecServiceHandler::add_group(const VecGroup &group) {
    SERVER_LOG_INFO << "add_group() called";
    SERVER_LOG_TRACE << "group.id = " << group.id << ", group.dimension = " << group.dimension
                        << ", group.index_type = " << group.index_type;

    try {

    } catch (std::exception& ex) {
        SERVER_LOG_ERROR << ex.what();
    }
}

void
VecServiceHandler::get_group(VecGroup &_return, const std::string &group_id) {
    SERVER_LOG_INFO << "get_group() called";
    SERVER_LOG_TRACE << "group_id = " << group_id;

    try {

    } catch (std::exception& ex) {
        SERVER_LOG_ERROR << ex.what();
    }
}

void
VecServiceHandler::del_group(const std::string &group_id) {
    SERVER_LOG_INFO << "del_group() called";
    SERVER_LOG_TRACE << "group_id = " << group_id;

    try {

    } catch (std::exception& ex) {
        SERVER_LOG_ERROR << ex.what();
    }
}


int64_t
VecServiceHandler::add_vector(const std::string &group_id, const VecTensor &tensor) {
    SERVER_LOG_INFO << "add_vector() called";
    SERVER_LOG_TRACE << "group_id = " << group_id << ", vector size = " << tensor.tensor.size();

    try {

    } catch (std::exception& ex) {
        SERVER_LOG_ERROR << ex.what();
    }
}

void
VecServiceHandler::add_vector_batch(VecTensorIdList &_return,
                                    const std::string &group_id,
                                    const VecTensorList &tensor_list) {
    SERVER_LOG_INFO << "add_vector_batch() called";
    SERVER_LOG_TRACE << "group_id = " << group_id << ", vector list size = "
                     << tensor_list.tensor_list.size();

    try {

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

    } catch (std::exception& ex) {
        SERVER_LOG_ERROR << ex.what();
    }
}

}
}
}