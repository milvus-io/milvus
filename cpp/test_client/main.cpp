////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include <getopt.h>
#include <libgen.h>
#include <cstring>
#include <string>
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <easylogging++.h>

#include "src/FaissTest.h"
#include "src/Log.h"
#include "src/ClientTest.h"
#include "server/ServerConfig.h"

INITIALIZE_EASYLOGGINGPP

void print_help(const std::string &app_name);


int
main(int argc, char *argv[]) {
    printf("Client start...\n");

//    FaissTest::test();
//    return 0;

    std::string app_name = basename(argv[0]);
    static struct option long_options[] = {{"conf_file", optional_argument, 0, 'c'},
                                           {"help", no_argument, 0, 'h'},
                                           {NULL, 0, 0, 0}};

    int option_index = 0;
    std::string config_filename = "../../conf/server_config.yaml";
    app_name = argv[0];

    int value;
    while ((value = getopt_long(argc, argv, "c:p:dh", long_options, &option_index)) != -1) {
        switch (value) {
            case 'c': {
                char *config_filename_ptr = strdup(optarg);
                config_filename = config_filename_ptr;
                free(config_filename_ptr);
                break;
            }
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

    zilliz::vecwise::server::ServerConfig& config = zilliz::vecwise::server::ServerConfig::GetInstance();
    config.LoadConfigFile(config_filename);

    CLIENT_LOG_INFO << "Load config file:" << config_filename;

#if 1
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
#else
    zilliz::vecwise::client::ClientTest::LoopTest();
    return 0;
#endif
}

void
print_help(const std::string &app_name) {
    printf("\n Usage: %s [OPTIONS]\n\n", app_name.c_str());
    printf("  Options:\n");
    printf("   -h --help                 Print this help\n");
    printf("   -c --conf_file filename   Read configuration from the file\n");
    printf("\n");
}