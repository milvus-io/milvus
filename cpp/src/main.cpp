// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <getopt.h>
#include <libgen.h>
#include <signal.h>
#include <unistd.h>
#include <cstring>
#include <string>

#include "../version.h"
#include "metrics/Metrics.h"
#include "server/Server.h"
#include "utils/CommonUtil.h"
#include "utils/SignalUtil.h"
#include "utils/easylogging++.h"

INITIALIZE_EASYLOGGINGPP

void
print_help(const std::string& app_name);

int
main(int argc, char* argv[]) {
    std::cout << std::endl << "Welcome to use Milvus by Zilliz!" << std::endl;
    std::cout << "Milvus " << BUILD_TYPE << " version: v" << MILVUS_VERSION << " built at " << BUILD_TIME << std::endl;

    static struct option long_options[] = {{"conf_file", required_argument, 0, 'c'},
                                           {"log_conf_file", required_argument, 0, 'l'},
                                           {"help", no_argument, 0, 'h'},
                                           {"daemon", no_argument, 0, 'd'},
                                           {"pid_file", required_argument, 0, 'p'},
                                           {NULL, 0, 0, 0}};

    int option_index = 0;
    int64_t start_daemonized = 0;

    std::string config_filename, log_config_file;
    std::string pid_filename;
    std::string app_name = argv[0];

    if (argc < 2) {
        print_help(app_name);
        std::cout << "Milvus server exit..." << std::endl;
        return EXIT_FAILURE;
    }

    int value;
    while ((value = getopt_long(argc, argv, "c:l:p:dh", long_options, &option_index)) != -1) {
        switch (value) {
            case 'c': {
                char* config_filename_ptr = strdup(optarg);
                config_filename = config_filename_ptr;
                free(config_filename_ptr);
                std::cout << "Loading configuration from: " << config_filename << std::endl;
                break;
            }
            case 'l': {
                char* log_filename_ptr = strdup(optarg);
                log_config_file = log_filename_ptr;
                free(log_filename_ptr);
                std::cout << "Initial log config from: " << log_config_file << std::endl;
                break;
            }
            case 'p': {
                char* pid_filename_ptr = strdup(optarg);
                pid_filename = pid_filename_ptr;
                free(pid_filename_ptr);
                std::cout << pid_filename << std::endl;
                break;
            }
            case 'd':
                start_daemonized = 1;
                break;
            case 'h':
                print_help(app_name);
                return EXIT_SUCCESS;
            case '?':
                print_help(app_name);
                return EXIT_FAILURE;
            default:
                print_help(app_name);
                break;
        }
    }

    /* Handle Signal */
    signal(SIGHUP, zilliz::milvus::server::SignalUtil::HandleSignal);
    signal(SIGINT, zilliz::milvus::server::SignalUtil::HandleSignal);
    signal(SIGUSR1, zilliz::milvus::server::SignalUtil::HandleSignal);
    signal(SIGSEGV, zilliz::milvus::server::SignalUtil::HandleSignal);
    signal(SIGUSR2, zilliz::milvus::server::SignalUtil::HandleSignal);
    signal(SIGTERM, zilliz::milvus::server::SignalUtil::HandleSignal);

    zilliz::milvus::server::Server& server = zilliz::milvus::server::Server::GetInstance();
    server.Init(start_daemonized, pid_filename, config_filename, log_config_file);
    server.Start();

    /* wait signal */
    pause();

    return 0;
}

void
print_help(const std::string& app_name) {
    std::cout << std::endl << "Usage: " << app_name << " [OPTIONS]" << std::endl << std::endl;
    std::cout << "  Options:" << std::endl;
    std::cout << "   -h --help                 Print this help" << std::endl;
    std::cout << "   -c --conf_file filename   Read configuration from the file" << std::endl;
    std::cout << "   -d --daemon               Daemonize this application" << std::endl;
    std::cout << "   -p --pid_file  filename   PID file used by daemonized app" << std::endl;
    std::cout << std::endl;
}
