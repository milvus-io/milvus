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

using namespace zilliz::vecwise;

int
main(int argc, char *argv[]) {
    printf("Megasearch %s version: v%s built at %s\n", BUILD_TYPE, MEGASEARCH_VERSION, BUILD_TIME);
    printf("Megasearch server start...\n");

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
        printf("Vecwise engine server exit...\n");
        return EXIT_FAILURE;
    }

    int value;
    while ((value = getopt_long(argc, argv, "c:l:p:dh", long_options, &option_index)) != -1) {
        switch (value) {
            case 'c': {
                char *config_filename_ptr = strdup(optarg);
                config_filename = config_filename_ptr;
                free(config_filename_ptr);
                printf("Loading configuration from: %s\n", config_filename.c_str());
                break;
            }
            case 'l': {
                char *log_filename_ptr = strdup(optarg);
                log_config_file = log_filename_ptr;
                free(log_filename_ptr);
                printf("Initial log config from: %s\n", log_config_file.c_str());
                break;
            }

            case 'p': {
                char *pid_filename_ptr = strdup(optarg);
                pid_filename = pid_filename_ptr;
                free(pid_filename_ptr);
                printf("%s\n", pid_filename.c_str());
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

    zilliz::vecwise::server::InitLog(log_config_file);

    server::Server* server_ptr = server::Server::Instance();
    server_ptr->Init(start_daemonized, pid_filename, config_filename);
    return server_ptr->Start();
}

void
print_help(const std::string &app_name) {
    printf("\n Usage: %s [OPTIONS]\n\n", app_name.c_str());
    printf("  Options:\n");
    printf("   -h --help                 Print this help\n");
    printf("   -c --conf_file filename   Read configuration from the file\n");
    printf("   -d --daemon               Daemonize this application\n");
    printf("   -p --pid_file  filename   PID file used by daemonized app\n");
    printf("\n");
}
