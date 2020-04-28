// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include <getopt.h>
#include <unistd.h>
#include <csignal>
#include <cstring>
#include <string>

#include "easyloggingpp/easylogging++.h"
#include "server/Server.h"
#include "src/version.h"
#include "utils/SignalUtil.h"
#include "utils/Status.h"

INITIALIZE_EASYLOGGINGPP

void
print_help(const std::string& app_name) {
    std::cout << std::endl << "Usage: " << app_name << " [OPTIONS]" << std::endl << std::endl;
    std::cout << "  Options:" << std::endl;
    std::cout << "   -h --help                 Print this help." << std::endl;
    std::cout << "   -c --conf_file filename   Read configuration from the file." << std::endl;
    std::cout << "   -d --daemon               Daemonize this application." << std::endl;
    std::cout << "   -p --pid_file  filename   PID file used by daemonized app." << std::endl;
    std::cout << std::endl;
}

void
print_banner() {
    std::cout << std::endl;
    std::cout << "    __  _________ _   ____  ______    " << std::endl;
    std::cout << "   /  |/  /  _/ /| | / / / / / __/    " << std::endl;
    std::cout << "  / /|_/ // // /_| |/ / /_/ /\\ \\    " << std::endl;
    std::cout << " /_/  /_/___/____/___/\\____/___/     " << std::endl;
    std::cout << std::endl;
    std::cout << "Welcome to use Milvus!" << std::endl;
    std::cout << "Milvus " << BUILD_TYPE << " version: v" << MILVUS_VERSION << ", built at " << BUILD_TIME << ", with "
#ifdef WITH_MKL
              << "MKL"
#else
              << "OpenBLAS"
#endif
              << " library." << std::endl;
#ifdef MILVUS_GPU_VERSION
    std::cout << "You are using Milvus GPU edition" << std::endl;
#else
    std::cout << "You are using Milvus CPU edition" << std::endl;
#endif
    std::cout << "Last commit id: " << LAST_COMMIT_ID << std::endl;
    std::cout << std::endl;
}

int
main(int argc, char* argv[]) {
    print_banner();

    static struct option long_options[] = {{"conf_file", required_argument, nullptr, 'c'},
                                           {"log_conf_file", required_argument, nullptr, 'l'},
                                           {"help", no_argument, nullptr, 'h'},
                                           {"daemon", no_argument, nullptr, 'd'},
                                           {"pid_file", required_argument, nullptr, 'p'},
                                           {nullptr, 0, nullptr, 0}};

    int option_index = 0;
    int64_t start_daemonized = 0;

    std::string config_filename, log_config_file;
    std::string pid_filename;
    std::string app_name = argv[0];
    milvus::Status s;

    milvus::server::Server& server = milvus::server::Server::GetInstance();

    if (argc < 2) {
        print_help(app_name);
        goto FAIL;
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
                std::cout << "Initializing log config from: " << log_config_file << std::endl;
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
    signal(SIGHUP, milvus::server::SignalUtil::HandleSignal);
    signal(SIGINT, milvus::server::SignalUtil::HandleSignal);
    signal(SIGUSR1, milvus::server::SignalUtil::HandleSignal);
    signal(SIGSEGV, milvus::server::SignalUtil::HandleSignal);
    signal(SIGUSR2, milvus::server::SignalUtil::HandleSignal);
    signal(SIGTERM, milvus::server::SignalUtil::HandleSignal);

    server.Init(start_daemonized, pid_filename, config_filename, log_config_file);

    s = server.Start();
    if (s.ok()) {
        std::cout << "Milvus server started successfully!" << std::endl;
    } else {
        std::cout << s.message() << std::endl;
        goto FAIL;
    }

    /* wait signal */
    pause();

    return EXIT_SUCCESS;

FAIL:
    std::cout << "Milvus server exit..." << std::endl;
    return EXIT_FAILURE;
}
