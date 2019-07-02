////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include "server/Server.h"
#include "version.h"

#include <getopt.h>
#include <libgen.h>
#include <cstring>
#include <string>
#include <signal.h>
#include <easylogging++.h>
#include "metrics/Metrics.h"

#include "utils/SignalUtil.h"
#include "utils/CommonUtil.h"
#include "utils/LogUtil.h"

INITIALIZE_EASYLOGGINGPP

void print_help(const std::string &app_name);

using namespace zilliz::milvus;

int
main(int argc, char *argv[]) {
    std::cout << std::endl << "Welcome to use Milvus by Zilliz!" << std::endl;
    std::cout << "Milvus " << BUILD_TYPE << " version: v" << MILVUS_VERSION << " built at " << BUILD_TIME << std::endl;

    signal(SIGINT, server::SignalUtil::HandleSignal);
    signal(SIGSEGV, server::SignalUtil::HandleSignal);
    signal(SIGUSR1, server::SignalUtil::HandleSignal);
    signal(SIGUSR2, server::SignalUtil::HandleSignal);

    std::string app_name = basename(argv[0]);
    static struct option long_options[] = {{"conf_file", required_argument, 0, 'c'},
                                           {"log_conf_file", required_argument, 0, 'l'},
                                           {"help", no_argument, 0, 'h'},
                                           {"daemon", no_argument, 0, 'd'},
                                           {"pid_file", required_argument, 0, 'p'},
                                           {NULL, 0, 0, 0}};

    int option_index = 0;
    int64_t start_daemonized = 0;
//    int pid_fd;

    std::string config_filename, log_config_file;
    std::string pid_filename;

    app_name = argv[0];

    if(argc < 2) {
        print_help(app_name);
        std::cout << "Milvus server exit..." << std::endl;
        return EXIT_FAILURE;
    }

    int value;
    while ((value = getopt_long(argc, argv, "c:l:p:dh", long_options, &option_index)) != -1) {
        switch (value) {
            case 'c': {
                char *config_filename_ptr = strdup(optarg);
                config_filename = config_filename_ptr;
                free(config_filename_ptr);
                std::cout << "Loading configuration from: " << config_filename << std::endl;
                break;
            }
            case 'l': {
                char *log_filename_ptr = strdup(optarg);
                log_config_file = log_filename_ptr;
                free(log_filename_ptr);
                std::cout << "Initial log config from: " << log_config_file << std::endl;
                break;
            }

            case 'p': {
                char *pid_filename_ptr = strdup(optarg);
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

    zilliz::milvus::server::InitLog(log_config_file);

    server::Server* server_ptr = server::Server::Instance();
    server_ptr->Init(start_daemonized, pid_filename, config_filename);
    return server_ptr->Start();
}

void
print_help(const std::string &app_name) {
    std::cout << std::endl<< "Usage: " << app_name << " [OPTIONS]" << std::endl << std::endl;
    std::cout << "  Options:" << std::endl;
    std::cout << "   -h --help                 Print this help" << std::endl;
    std::cout << "   -c --conf_file filename   Read configuration from the file" << std::endl;
    std::cout << "   -d --daemon               Daemonize this application" << std::endl;
    std::cout << "   -p --pid_file  filename   PID file used by daemonized app" << std::endl;
    std::cout << std::endl;
}
