////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include "ClientTest.h"

#include <gtest/gtest.h>
#include "utils/TimeRecorder.h"
#include "utils/AttributeSerializer.h"
#include "ClientSession.h"
#include "server/ServerConfig.h"
#include "Log.h"

#include <time.h>

using namespace megasearch;
using namespace zilliz;
using namespace zilliz::vecwise;
using namespace zilliz::vecwise::client;

namespace {
    static const int32_t VEC_DIMENSION = 256;
    static const int64_t BATCH_COUNT = 10000;
    static const int64_t REPEAT_COUNT = 1;
    static const int64_t TOP_K = 10;

    static const std::string TEST_ATTRIB_NUM = "number";
    static const std::string TEST_ATTRIB_COMMENT = "comment";

    std::string CurrentTime() {
        time_t tt;
        time( &tt );
        tt = tt + 8*3600;
        tm* t= gmtime( &tt );

        std::string str = std::to_string(t->tm_year + 1900) + "_" + std::to_string(t->tm_mon + 1)
                          + "_" + std::to_string(t->tm_mday) + "_" + std::to_string(t->tm_hour)
                          + "_" + std::to_string(t->tm_min) + "_" + std::to_string(t->tm_sec);

        return str;
    }

    void GetDate(int& year, int& month, int& day) {
        time_t tt;
        time( &tt );
        tm* t= gmtime( &tt );
        year = t->tm_year;
        month = t->tm_mon;
        day = t->tm_mday;
    }

    void GetServerAddress(std::string& address, int32_t& port, std::string& protocol) {
        server::ServerConfig& config = server::ServerConfig::GetInstance();
        server::ConfigNode server_config = config.GetConfig(server::CONFIG_SERVER);
        address = server_config.GetValue(server::CONFIG_SERVER_ADDRESS, "127.0.0.1");
        port = server_config.GetInt32Value(server::CONFIG_SERVER_PORT, 33001);
        protocol = server_config.GetValue(server::CONFIG_SERVER_PROTOCOL, "binary");
        //std::string mode = server_config.GetValue(server::CONFIG_SERVER_MODE, "thread_pool");
    }

    int32_t GetFlushInterval() {
        server::ServerConfig& config = server::ServerConfig::GetInstance();
        server::ConfigNode db_config = config.GetConfig(server::CONFIG_DB);
        return db_config.GetInt32Value(server::CONFIG_DB_FLUSH_INTERVAL);
    }

    std::string GetGroupID() {
        static std::string s_id(CurrentTime());
        return s_id;
    }

    void BuildVectors(int64_t from, int64_t to,
                      VecTensorList* tensor_list,
                      VecBinaryTensorList* bin_tensor_list) {
        if(to <= from) {
            return;
        }

        static int64_t total_build = 0;
        int64_t count = to - from;
        server::TimeRecorder rc(std::to_string(count) + " vectors built");
        for (int64_t k = from; k < to; k++) {
            VecTensor tensor;
            tensor.tensor.reserve(VEC_DIMENSION);
            VecBinaryTensor bin_tensor;
            bin_tensor.tensor.resize(VEC_DIMENSION * sizeof(double));
            double *d_p = (double *) (const_cast<char *>(bin_tensor.tensor.data()));
            for (int32_t i = 0; i < VEC_DIMENSION; i++) {
                double val = (double) (i + k);
                tensor.tensor.push_back(val);
                d_p[i] = val;
            }

            server::AttribMap attrib_map;
            attrib_map[TEST_ATTRIB_NUM] = "No." + std::to_string(k);

            if(tensor_list) {
                tensor.uid = "normal_vec_" + std::to_string(k);
                attrib_map[TEST_ATTRIB_COMMENT] = "this is vector " + tensor.uid;
                tensor.__set_attrib(attrib_map);
                tensor_list->tensor_list.emplace_back(tensor);
            }

            if(bin_tensor_list) {
                bin_tensor.uid = "binary_vec_" + std::to_string(k);
                attrib_map[TEST_ATTRIB_COMMENT] = "this is binary vector " + bin_tensor.uid;
                bin_tensor.__set_attrib(attrib_map);
                bin_tensor_list->tensor_list.emplace_back(bin_tensor);
            }

            total_build++;
            if (total_build % 10000 == 0) {
                CLIENT_LOG_INFO << total_build << " vectors built";
            }
        }

        rc.Elapse("done");
    }
}

TEST(AddVector, CLIENT_TEST) {
    try {
        std::string address, protocol;
        int32_t port = 0;
        GetServerAddress(address, port, protocol);
        client::ClientSession session(address, port, protocol);

        //verify get invalid group
        try {
            std::string id;
            VecTensor tensor;
            for(int32_t i = 0; i < VEC_DIMENSION; i++) {
                tensor.tensor.push_back(0.5);
            }
            session.interface()->add_vector(id, GetGroupID(), tensor);
        } catch (VecException& ex) {
            CLIENT_LOG_ERROR << "request encounter exception: " << ex.what();
            ASSERT_EQ(ex.code, VecErrCode::ILLEGAL_ARGUMENT);
        }

        try {
            VecGroup temp_group;
            session.interface()->get_group(temp_group, GetGroupID());
            //ASSERT_TRUE(temp_group.id.empty());
        } catch (VecException& ex) {
            CLIENT_LOG_ERROR << "request encounter exception: " << ex.what();
            ASSERT_EQ(ex.code, VecErrCode::GROUP_NOT_EXISTS);
        }

        //add group
        VecGroup group;
        group.id = GetGroupID();
        group.dimension = VEC_DIMENSION;
        group.index_type = 0;
        session.interface()->add_group(group);

        for(int64_t r = 0; r < REPEAT_COUNT; r++) {
            //prepare data
            CLIENT_LOG_INFO << "Preparing vectors...";
            const int64_t count = BATCH_COUNT;
            int64_t offset = r*count*2;
            VecTensorList tensor_list_1, tensor_list_2;
            VecBinaryTensorList bin_tensor_list_1, bin_tensor_list_2;
            BuildVectors(0 + offset, count + offset, &tensor_list_1, &bin_tensor_list_1);
            BuildVectors(count + offset, count * 2 + offset, &tensor_list_2, &bin_tensor_list_2);

#if 0
            //add vectors one by one
            {
                server::TimeRecorder rc("Add " + std::to_string(count) + " vectors one by one");
                for (int64_t k = 0; k < count; k++) {
                    std::string id;
                    tensor_list_1.tensor_list[k].uid = "";
                    session.interface()->add_vector(id, group.id, tensor_list_1.tensor_list[k]);
                    if (k % 1000 == 0) {
                        CLIENT_LOG_INFO << "add normal vector no." << k;
                    }
                    ASSERT_TRUE(!id.empty());
                }
                rc.Elapse("done!");
            }

            //add vectors in one batch
            {
                server::TimeRecorder rc("Add " + std::to_string(count) + " vectors in one batch");
                std::vector<std::string> ids;
                session.interface()->add_vector_batch(ids, group.id, tensor_list_2);
                rc.Elapse("done!");
            }

#else
            //add binary vectors one by one
            {
                server::TimeRecorder rc("Add " + std::to_string(count) + " binary vectors one by one");
                for (int64_t k = 0; k < count; k++) {
                    std::string id;
                    bin_tensor_list_1.tensor_list[k].uid = "";
                    session.interface()->add_binary_vector(id, group.id, bin_tensor_list_1.tensor_list[k]);
                    if (k % 1000 == 0) {
                        CLIENT_LOG_INFO << "add binary vector no." << k;
                    }
                    ASSERT_TRUE(!id.empty());
                }
                rc.Elapse("done!");
            }

            //add binary vectors in one batch
            {
                server::TimeRecorder rc("Add " + std::to_string(count) + " binary vectors in one batch");
                std::vector<std::string> ids;
                session.interface()->add_binary_vector_batch(ids, group.id, bin_tensor_list_2);
                ASSERT_EQ(ids.size(), bin_tensor_list_2.tensor_list.size());
                for(size_t i = 0; i < ids.size(); i++) {
                    ASSERT_TRUE(!ids[i].empty());
                }
                rc.Elapse("done!");
            }
#endif
        }

    } catch (std::exception &ex) {
        CLIENT_LOG_ERROR << "request encounter exception: " << ex.what();
        ASSERT_TRUE(false);
    }
}

TEST(SearchVector, CLIENT_TEST) {
    uint32_t sleep_seconds = GetFlushInterval();
    std::cout << "Sleep " << sleep_seconds << " seconds..." << std::endl;
    sleep(sleep_seconds);

    try {
        std::string address, protocol;
        int32_t port = 0;
        GetServerAddress(address, port, protocol);
        client::ClientSession session(address, port, protocol);

        //search vector
        {
            const int32_t anchor_index = 100;
            VecTensor tensor;
            for (int32_t i = 0; i < VEC_DIMENSION; i++) {
                tensor.tensor.push_back((double) (i + anchor_index));
            }

            //build time range
            VecSearchResult res;
            VecSearchFilter filter;
            VecTimeRange range;
            VecDateTime date;
            GetDate(date.year, date.month, date.day);
            range.time_begin = date;
            range.time_end = date;
            std::vector<VecTimeRange>  time_ranges;
            time_ranges.emplace_back(range);
            filter.__set_time_ranges(time_ranges);

            //normal search
            {
                server::TimeRecorder rc("Search top_k");
                session.interface()->search_vector(res, GetGroupID(), TOP_K, tensor, filter);
                rc.Elapse("done!");

                //build result
                std::cout << "Search result: " << std::endl;
                for (VecSearchResultItem &item : res.result_list) {
                    std::cout << "\t" << item.uid << std::endl;

                    ASSERT_TRUE(item.attrib.count(TEST_ATTRIB_NUM) != 0);
                    ASSERT_TRUE(item.attrib.count(TEST_ATTRIB_COMMENT) != 0);
                    ASSERT_TRUE(!item.attrib[TEST_ATTRIB_COMMENT].empty());
                }

                ASSERT_EQ(res.result_list.size(), (uint64_t) TOP_K);
                if (!res.result_list.empty()) {
                    ASSERT_TRUE(!res.result_list[0].uid.empty());
                }
            }

            //filter attribute search
            {
                std::vector<std::string> require_attributes = {TEST_ATTRIB_COMMENT};
                filter.__set_return_attribs(require_attributes);
                server::TimeRecorder rc("Search top_k with attribute filter");
                session.interface()->search_vector(res, GetGroupID(), TOP_K, tensor, filter);
                rc.Elapse("done!");

                //build result
                std::cout << "Search result attributes: " << std::endl;
                for (VecSearchResultItem &item : res.result_list) {
                    ASSERT_EQ(item.attrib.size(), 1UL);
                    ASSERT_TRUE(item.attrib.count(TEST_ATTRIB_COMMENT) != 0);
                    ASSERT_TRUE(!item.attrib[TEST_ATTRIB_COMMENT].empty());
                    std::cout << "\t" << item.uid << ":" << item.attrib[TEST_ATTRIB_COMMENT] << std::endl;
                }

                ASSERT_EQ(res.result_list.size(), (uint64_t) TOP_K);
            }

            //empty search
            {
                date.day > 0 ? date.day -= 1 : date.day += 1;
                range.time_begin = date;
                range.time_end = date;
                time_ranges.clear();
                time_ranges.emplace_back(range);
                filter.__set_time_ranges(time_ranges);
                session.interface()->search_vector(res, GetGroupID(), TOP_K, tensor, filter);

                ASSERT_EQ(res.result_list.size(), 0);
            }
        }

        //search binary vector
        {
            const int32_t anchor_index = BATCH_COUNT + 200;
            const int32_t search_count = 10;
            server::TimeRecorder rc("Search binary batch top_k");
            VecBinaryTensorList tensor_list;
            for(int32_t k = anchor_index; k < anchor_index + search_count; k++) {
                VecBinaryTensor bin_tensor;
                bin_tensor.tensor.resize(VEC_DIMENSION * sizeof(double));
                double* d_p = new double[VEC_DIMENSION];
                for (int32_t i = 0; i < VEC_DIMENSION; i++) {
                    d_p[i] = (double)(i + k);
                }
                memcpy(const_cast<char*>(bin_tensor.tensor.data()), d_p, VEC_DIMENSION * sizeof(double));
                tensor_list.tensor_list.emplace_back(bin_tensor);
            }

            VecSearchResultList res;
            VecSearchFilter filter;
            session.interface()->search_binary_vector_batch(res, GetGroupID(), TOP_K, tensor_list, filter);

            std::cout << "Search binary batch result: " << std::endl;
            for(size_t i = 0 ; i < res.result_list.size(); i++) {
                std::cout << "No " << i << ":" << std::endl;
                for(VecSearchResultItem& item : res.result_list[i].result_list) {
                    std::cout << "\t" << item.uid << std::endl;
                    ASSERT_TRUE(item.attrib.count(TEST_ATTRIB_NUM) != 0);
                    ASSERT_TRUE(item.attrib.count(TEST_ATTRIB_COMMENT) != 0);
                    ASSERT_TRUE(!item.attrib[TEST_ATTRIB_COMMENT].empty());
                }
            }

            rc.Elapse("done!");

            ASSERT_EQ(res.result_list.size(), search_count);
            for(size_t i = 0 ; i < res.result_list.size(); i++) {
                ASSERT_EQ(res.result_list[i].result_list.size(), (uint64_t) TOP_K);
                ASSERT_TRUE(!res.result_list[i].result_list.empty());
            }
        }

    } catch (std::exception& ex) {
        CLIENT_LOG_ERROR << "request encounter exception: " << ex.what();
        ASSERT_TRUE(false);
    }
}
