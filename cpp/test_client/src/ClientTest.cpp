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

using namespace zilliz;
using namespace zilliz::vecwise;
using namespace zilliz::vecwise::client;

namespace {
    static const int32_t VEC_DIMENSION = 256;

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
                      VecTensorList& tensor_list,
                      VecBinaryTensorList& bin_tensor_list) {
        if(to <= from) {
            return;
        }

        int64_t count = to - from;
        server::TimeRecorder rc(std::to_string(count) + " vectors built");
        for (int64_t k = from; k < count; k++) {
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

            tensor.uid = "normal_vec_" + std::to_string(k);
            attrib_map[TEST_ATTRIB_COMMENT] = tensor.uid;
            tensor.__set_attrib(attrib_map);
            tensor_list.tensor_list.emplace_back(tensor);

            bin_tensor.uid = "binary_vec_" + std::to_string(k);
            attrib_map[TEST_ATTRIB_COMMENT] = bin_tensor.uid;
            bin_tensor.__set_attrib(attrib_map);
            bin_tensor_list.tensor_list.emplace_back(bin_tensor);

            if ((k + 1) % 10000 == 0) {
                CLIENT_LOG_INFO << k + 1 << " vectors built";
            }
        }

        rc.Elapse("done");
    }
}

void ClientTest::LoopTest() {
    server::TimeRecorder rc("LoopTest");

    std::string address, protocol;
    int32_t port = 0;
    GetServerAddress(address, port, protocol);
    client::ClientSession session(address, port, protocol);

    rc.Record("connection");

    //add group
    VecGroup group;
    group.id = "loop_group";
    group.dimension = VEC_DIMENSION;
    group.index_type = 0;
    session.interface()->add_group(group);
    rc.Record("add group");

    const int64_t batch = 10000;
    for(int64_t i = 0; i < 1000; i++) {
        VecTensorList tensor_list;
        VecBinaryTensorList bin_tensor_list;
        BuildVectors(i*batch, (i+1)*batch, tensor_list, bin_tensor_list);
        rc.Record("build batch no." + std::to_string(i));

        session.interface()->add_binary_vector_batch(group.id, bin_tensor_list);
        rc.Record("add batch no." + std::to_string(i));

        sleep(1);
        rc.Record("sleep 1 second");

        VecTensor tensor;
        for (int32_t k = 0; k < VEC_DIMENSION; k++) {
            tensor.tensor.push_back((double) (k + i*666));
        }

        //do search
        VecSearchResult res;
        VecSearchFilter filter;
        session.interface()->search_vector(res, group.id, 10, tensor, filter);
        rc.Record("search finish");

        std::cout << "Search result: " << std::endl;
        for(VecSearchResultItem& item : res.result_list) {
            std::cout << "\t" << item.uid << std::endl;
        }
    }
}

TEST(AddVector, CLIENT_TEST) {
    try {
        std::string address, protocol;
        int32_t port = 0;
        GetServerAddress(address, port, protocol);
        client::ClientSession session(address, port, protocol);

        //add group
        VecGroup group;
        group.id = GetGroupID();
        group.dimension = VEC_DIMENSION;
        group.index_type = 0;
        session.interface()->add_group(group);

        //prepare data
        CLIENT_LOG_INFO << "Preparing vectors...";
        const int64_t count = 100000;
        VecTensorList tensor_list;
        VecBinaryTensorList bin_tensor_list;
        BuildVectors(0, count, tensor_list, bin_tensor_list);

//        //add vectors one by one
//        {
//            server::TimeRecorder rc("Add " + std::to_string(count) + " vectors one by one");
//            for (int64_t k = 0; k < count; k++) {
//                session.interface()->add_vector(group.id, tensor_list.tensor_list[k]);
//                if (k % 1000 == 0) {
//                    CLIENT_LOG_INFO << "add normal vector no." << k;
//                }
//            }
//            rc.Elapse("done!");
//        }
//
//        //add vectors in one batch
//        {
//            server::TimeRecorder rc("Add " + std::to_string(count) + " vectors in one batch");
//            session.interface()->add_vector_batch(group.id, tensor_list);
//            rc.Elapse("done!");
//        }

#if 0
        //add binary vectors one by one
        {
            server::TimeRecorder rc("Add " + std::to_string(count) + " binary vectors one by one");
            for (int64_t k = 0; k < count; k++) {
                session.interface()->add_binary_vector(group.id, bin_tensor_list.tensor_list[k]);
                if (k % 1000 == 0) {
                    CLIENT_LOG_INFO << "add binary vector no." << k;
                }
            }
            rc.Elapse("done!");
        }
#else
        //add binary vectors in one batch
        {
            server::TimeRecorder rc("Add " + std::to_string(count) + " binary vectors in one batch");
            session.interface()->add_binary_vector_batch(group.id, bin_tensor_list);
            rc.Elapse("done!");
        }
#endif
    } catch (std::exception &ex) {
        CLIENT_LOG_ERROR << "request encounter exception: " << ex.what();
        ASSERT_TRUE(false);
    }
}

TEST(SearchVector, CLIENT_TEST) {
    std::cout << "Sleep " << GetFlushInterval() << " seconds..." << std::endl;
    sleep(GetFlushInterval());

    try {
        std::string address, protocol;
        int32_t port = 0;
        GetServerAddress(address, port, protocol);
        client::ClientSession session(address, port, protocol);

        //search vector
        {
            const int32_t anchor_index = 100;
            const int64_t top_k = 10;
            server::TimeRecorder rc("Search top_k");
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

            //do search
            session.interface()->search_vector(res, GetGroupID(), top_k, tensor, filter);

            //build result
            std::cout << "Search result: " << std::endl;
            for(VecSearchResultItem& item : res.result_list) {
                std::cout << "\t" << item.uid << std::endl;

                ASSERT_TRUE(item.attrib.count(TEST_ATTRIB_NUM) != 0);
                ASSERT_TRUE(item.attrib.count(TEST_ATTRIB_COMMENT) != 0);
                ASSERT_TRUE(item.attrib[TEST_ATTRIB_COMMENT].find(item.uid) != std::string::npos);
            }
            rc.Elapse("done!");

            ASSERT_EQ(res.result_list.size(), (uint64_t)top_k);
            if(!res.result_list.empty()) {
                ASSERT_TRUE(res.result_list[0].uid.find(std::to_string(anchor_index)) != std::string::npos);
            }

            //empty search
            date.day > 0 ? date.day -= 1 : date.day += 1;
            range.time_begin = date;
            range.time_end = date;
            time_ranges.clear();
            time_ranges.emplace_back(range);
            filter.__set_time_ranges(time_ranges);
            session.interface()->search_vector(res, GetGroupID(), top_k, tensor, filter);

            ASSERT_EQ(res.result_list.size(), 0);
        }

        //search binary vector
        {
            const int32_t anchor_index = 300;
            const int32_t search_count = 10;
            const int64_t top_k = 5;
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
            session.interface()->search_binary_vector_batch(res, GetGroupID(), top_k, tensor_list, filter);

            std::cout << "Search binary batch result: " << std::endl;
            for(size_t i = 0 ; i < res.result_list.size(); i++) {
                std::cout << "No " << i << ":" << std::endl;
                for(VecSearchResultItem& item : res.result_list[i].result_list) {
                    std::cout << "\t" << item.uid << std::endl;
                    ASSERT_TRUE(item.attrib.count(TEST_ATTRIB_NUM) != 0);
                    ASSERT_TRUE(item.attrib.count(TEST_ATTRIB_COMMENT) != 0);
                    ASSERT_TRUE(item.attrib[TEST_ATTRIB_COMMENT].find(item.uid) != std::string::npos);
                }
            }

            rc.Elapse("done!");

            ASSERT_EQ(res.result_list.size(), search_count);
            for(size_t i = 0 ; i < res.result_list.size(); i++) {
                ASSERT_EQ(res.result_list[i].result_list.size(), (uint64_t) top_k);
                if (!res.result_list[i].result_list.empty()) {
                    ASSERT_TRUE(res.result_list[i].result_list[0].uid.find(std::to_string(anchor_index + i)) != std::string::npos);
                }
            }
        }

    } catch (std::exception& ex) {
        CLIENT_LOG_ERROR << "request encounter exception: " << ex.what();
        ASSERT_TRUE(false);
    }
}
