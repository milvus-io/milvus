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
#include <libgen.h>
#include <cstring>
#include <string>

#include "src/ClientTest.h"

void
print_help(const std::string& app_name);

int
main(int argc, char* argv[]) {
    printf("Client start...\n");

    std::string app_name = basename(argv[0]);
    static struct option long_options[] = {{"server", optional_argument, nullptr, 's'},
                                           {"port", optional_argument, nullptr, 'p'},
                                           {"help", no_argument, nullptr, 'h'},
                                           {"collection_name", no_argument, nullptr, 't'},
                                           {"index", optional_argument, nullptr, 'i'},
                                           {"index_file_size", optional_argument, nullptr, 'f'},
                                           {"nlist", optional_argument, nullptr, 'l'},
                                           {"metric", optional_argument, nullptr, 'm'},
                                           {"dimension", optional_argument, nullptr, 'd'},
                                           {"rowcount", optional_argument, nullptr, 'r'},
                                           {"concurrency", optional_argument, nullptr, 'c'},
                                           {"query_count", optional_argument, nullptr, 'q'},
                                           {"nq", optional_argument, nullptr, 'n'},
                                           {"topk", optional_argument, nullptr, 'k'},
                                           {"nprobe", optional_argument, nullptr, 'b'},
                                           {"print", optional_argument, nullptr, 'v'},
                                           {nullptr, 0, nullptr, 0}};

    int option_index = 0;
    std::string address = "127.0.0.1", port = "19530";
    app_name = argv[0];

    TestParameters parameters;
    int value;
    while ((value = getopt_long(argc, argv, "s:p:t:i:f:l:m:d:r:c:q:n:k:b:vh", long_options, &option_index)) != -1) {
        switch (value) {
            case 's': {
                char* address_ptr = strdup(optarg);
                address = address_ptr;
                free(address_ptr);
                break;
            }
            case 'p': {
                char* port_ptr = strdup(optarg);
                port = port_ptr;
                free(port_ptr);
                break;
            }
            case 't': {
                char* ptr = strdup(optarg);
                parameters.collection_name_ = ptr;
                free(ptr);
                break;
            }
            case 'i': {
                char* ptr = strdup(optarg);
                parameters.index_type_ = atol(ptr);
                free(ptr);
                break;
            }
            case 'f': {
                char* ptr = strdup(optarg);
                parameters.index_file_size_ = atol(ptr);
                free(ptr);
                break;
            }
            case 'l': {
                char* ptr = strdup(optarg);
                parameters.nlist_ = atol(ptr);
                free(ptr);
                break;
            }
            case 'm': {
                char* ptr = strdup(optarg);
                parameters.metric_type_ = atol(ptr);
                free(ptr);
                break;
            }
            case 'd': {
                char* ptr = strdup(optarg);
                parameters.dimensions_ = atol(ptr);
                free(ptr);
                break;
            }
            case 'r': {
                char* ptr = strdup(optarg);
                parameters.row_count_ = atol(ptr);
                free(ptr);
                break;
            }
            case 'c': {
                char* ptr = strdup(optarg);
                parameters.concurrency_ = atol(ptr);
                free(ptr);
                break;
            }
            case 'q': {
                char* ptr = strdup(optarg);
                parameters.query_count_ = atol(ptr);
                free(ptr);
                break;
            }
            case 'n': {
                char* ptr = strdup(optarg);
                parameters.nq_ = atol(ptr);
                free(ptr);
                break;
            }
            case 'k': {
                char* ptr = strdup(optarg);
                parameters.topk_ = atol(ptr);
                free(ptr);
                break;
            }
            case 'b': {
                char* ptr = strdup(optarg);
                parameters.nprobe_ = atol(ptr);
                free(ptr);
                break;
            }
            case 'v': {
                parameters.print_result_ = true;
                break;
            }
            case 'h':
            default:
                print_help(app_name);
                return EXIT_SUCCESS;
        }
    }

    ClientTest test(address, port);
    test.Test(parameters);

    printf("Client exits ...\n");
    return 0;
}

void
print_help(const std::string& app_name) {
    printf("\n Usage: %s [OPTIONS]\n\n", app_name.c_str());
    printf("  Options:\n");
    printf("   -s --server           Server address, default:127.0.0.1\n");
    printf("   -p --port             Server port, default:19530\n");
    printf("   -t --collection_name  target collection name, specify this will ignore collection parameters, "
           "default empty\n");
    printf("   -h --help             Print help information\n");
    printf("   -i --index            "
           "Collection index type(1=IDMAP, 2=IVFLAT, 3=IVFSQ8, 5=IVFSQ8H), default:3\n");
    printf("   -f --index_file_size  Collection index file size, default:1024\n");
    printf("   -l --nlist            Collection index nlist, default:16384\n");
    printf("   -m --metric           "
           "Collection metric type(1=L2, 2=IP, 3=HAMMING, 4=JACCARD, 5=TANIMOTO, 6=SUBSTRUCTURE, 7=SUPERSTRUCTURE), "
           "default:1\n");
    printf("   -d --dimension        Collection dimension, default:128\n");
    printf("   -r --rowcount         Collection total row count(unit:million), default:1\n");
    printf("   -c --concurrency      Max client connections, default:20\n");
    printf("   -q --query_count      Query total count, default:1000\n");
    printf("   -n --nq               nq of each query, default:1\n");
    printf("   -k --topk             topk of each query, default:10\n");
    printf("   -b --nprobe           nprobe of each query, default:16\n");
    printf("   -v --print_result     Print query result, default:false\n");
    printf("\n");
}
