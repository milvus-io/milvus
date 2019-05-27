////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include <getopt.h>
#include <libgen.h>
#include <cstring>
#include <string>

#include "src/ClientTest.h"

void print_help(const std::string &app_name);


int
main(int argc, char *argv[]) {
    printf("Client start...\n");

    std::string app_name = basename(argv[0]);
    static struct option long_options[] = {{"conf_file", optional_argument, 0, 'c'},
                                           {"help", no_argument, 0, 'h'},
                                           {NULL, 0, 0, 0}};

    int option_index = 0;
    std::string address = "127.0.0.1", port = "33001";
    app_name = argv[0];

    int value;
    while ((value = getopt_long(argc, argv, "c:p:dh", long_options, &option_index)) != -1) {
        switch (value) {
            case 'h': {
                char *address_ptr = strdup(optarg);
                address = address_ptr;
                free(address_ptr);
                break;
            }
            case 'p': {
                char *port_ptr = strdup(optarg);
                address = port_ptr;
                free(port_ptr);
                break;
            }
            default:
                break;
        }
    }

    ClientTest test;
    test.Test(address, port);

    printf("Client stop...\n");
    return 0;
}

void
print_help(const std::string &app_name) {
    printf("\n Usage: %s [OPTIONS]\n\n", app_name.c_str());
    printf("  Options:\n");
    printf("   -h    Megasearch server address\n");
    printf("   -p    Megasearch server port\n");
    printf("\n");
}