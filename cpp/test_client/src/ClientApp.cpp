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

namespace zilliz {
namespace vecwise {
namespace client {

void ClientApp::Run(const std::string &config_file) {
    server::ServerConfig& config = server::ServerConfig::GetInstance();
    config.LoadConfigFile(config_file);

    CLIENT_LOG_INFO << "Load config file:" << config_file;

    server::ConfigNode server_config = config.GetConfig(server::CONFIG_SERVER);
    std::string address = server_config.GetValue(server::CONFIG_SERVER_ADDRESS, "127.0.0.1");
    int32_t port = server_config.GetInt32Value(server::CONFIG_SERVER_PORT, 33001);
    std::string protocol = server_config.GetValue(server::CONFIG_SERVER_PROTOCOL, "binary");
    //std::string mode = server_config.GetValue(server::CONFIG_SERVER_MODE, "thread_pool");

    CLIENT_LOG_INFO << "Connect to server: " << address << ":" << std::to_string(port);

    try {
        ClientSession session(address, port, protocol);

        //add group
        const int32_t dim = 256;
        VecGroup group;
        group.id = "test_group";
        group.dimension = dim;
        group.index_type = 0;
        session.interface()->add_group(group);

        const int64_t count = 500;
        //add vectors one by one
        {

            server::TimeRecorder rc("Add " + std::to_string(count) + " vectors one by one");
            for (int64_t k = 0; k < count; k++) {
                VecTensor tensor;
                for (int32_t i = 0; i < dim; i++) {
                    tensor.tensor.push_back((double) (i + k));
                }
                tensor.uid = "vec_" + std::to_string(k);

                session.interface()->add_vector(group.id, tensor);

                CLIENT_LOG_INFO << "add vector no." << k;
            }
            rc.Elapse("done!");
        }

        //add vectors in one batch
        {
            server::TimeRecorder rc("Add " + std::to_string(count) + " vectors in one batch");
            VecTensorList vec_list;
            for (int64_t k = 0; k < count; k++) {
                VecTensor tensor;
                for (int32_t i = 0; i < dim; i++) {
                    tensor.tensor.push_back((double) (i + k));
                }
                tensor.uid = "vec_" + std::to_string(k);
                vec_list.tensor_list.push_back(tensor);
            }
            session.interface()->add_vector_batch(group.id, vec_list);
            rc.Elapse("done!");
        }

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

