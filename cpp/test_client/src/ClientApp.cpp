/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include <utils/TimeRecorder.h>
#include "ClientApp.h"
#include "ClientSession.h"
#include "server/ServerConfig.h"
#include "Log.h"

#include <time.h>

namespace zilliz {
namespace vecwise {
namespace client {

namespace {
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
}

void ClientApp::Run(const std::string &config_file) {
    server::ServerConfig& config = server::ServerConfig::GetInstance();
    config.LoadConfigFile(config_file);

    CLIENT_LOG_INFO << "Load config file:" << config_file;

    server::ConfigNode server_config = config.GetConfig(server::CONFIG_SERVER);
    std::string address = server_config.GetValue(server::CONFIG_SERVER_ADDRESS, "127.0.0.1");
    int32_t port = server_config.GetInt32Value(server::CONFIG_SERVER_PORT, 33001);
    std::string protocol = server_config.GetValue(server::CONFIG_SERVER_PROTOCOL, "binary");
    //std::string mode = server_config.GetValue(server::CONFIG_SERVER_MODE, "thread_pool");
    int32_t flush_interval = server_config.GetInt32Value(server::CONFIG_SERVER_DB_FLUSH_INTERVAL);

    CLIENT_LOG_INFO << "Connect to server: " << address << ":" << std::to_string(port);

    try {
        ClientSession session(address, port, protocol);

        //add group
        const int32_t dim = 256;
        VecGroup group;
        group.id = CurrentTime();
        group.dimension = dim;
        group.index_type = 0;
        session.interface()->add_group(group);

        //prepare data
        const int64_t count = 100000;
        VecTensorList tensor_list;
        VecBinaryTensorList bin_tensor_list;
        for (int64_t k = 0; k < count; k++) {
            VecTensor tensor;
            tensor.tensor.reserve(dim);
            VecBinaryTensor bin_tensor;
            bin_tensor.tensor.resize(dim*sizeof(double));
            double* d_p = (double*)(const_cast<char*>(bin_tensor.tensor.data()));
            for (int32_t i = 0; i < dim; i++) {
                double val = (double)(i + k);
                tensor.tensor.push_back(val);
                d_p[i] = val;
            }

            tensor.uid = "normal_vec_" + std::to_string(k);
            tensor_list.tensor_list.emplace_back(tensor);

            bin_tensor.uid = "binary_vec_" + std::to_string(k);
            bin_tensor_list.tensor_list.emplace_back(bin_tensor);
        }

//        //add vectors one by one
//        {
//            server::TimeRecorder rc("Add " + std::to_string(count) + " vectors one by one");
//            for (int64_t k = 0; k < count; k++) {
//                session.interface()->add_vector(group.id, tensor_list.tensor_list[k]);
//                if(k%1000 == 0) {
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
//
//        //add binary vectors one by one
//        {
//            server::TimeRecorder rc("Add " + std::to_string(count) + " binary vectors one by one");
//            for (int64_t k = 0; k < count; k++) {
//                session.interface()->add_binary_vector(group.id, bin_tensor_list.tensor_list[k]);
//                if(k%1000 == 0) {
//                    CLIENT_LOG_INFO << "add binary vector no." << k;
//                }
//            }
//            rc.Elapse("done!");
//        }

        //add binary vectors in one batch
        {
            server::TimeRecorder rc("Add " + std::to_string(count) + " binary vectors in one batch");
            session.interface()->add_binary_vector_batch(group.id, bin_tensor_list);
            rc.Elapse("done!");
        }

        std::cout << "Sleep " << flush_interval << " seconds..." << std::endl;
        sleep(flush_interval);
        
        //search vector
        {
            server::TimeRecorder rc("Search top_k");
            VecTensor tensor;
            for (int32_t i = 0; i < dim; i++) {
                tensor.tensor.push_back((double) (i + 100));
            }

            VecSearchResult res;
            VecTimeRangeList range;
            session.interface()->search_vector(res, group.id, 10, tensor, range);

            std::cout << "Search result: " << std::endl;
            for(auto id : res.id_list) {
                std::cout << id << std::endl;
            }
            rc.Elapse("done!");
        }

    } catch (std::exception& ex) {
        CLIENT_LOG_ERROR << "request encounter exception: " << ex.what();
    }

    CLIENT_LOG_INFO << "Test finished";
}

}
}
}

