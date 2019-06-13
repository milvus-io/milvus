
#include "utils/Log.h"
#include "LicenseLibrary.h"
#include "utils/Error.h"

#include <iostream>
#include <getopt.h>
#include <memory.h>
// Not provide path: current work path will be used and system.info.
using namespace zilliz::milvus;

void
print_usage(const std::string &app_name) {
    printf("\n Usage: %s [OPTIONS]\n\n", app_name.c_str());
    printf("  Options:\n");
    printf("   -h --help               Print this help\n");
    printf("   -s --sysinfo filename   Generate system info file as given name\n");
    printf("\n");
}

int main(int argc, char *argv[]) {
    std::string app_name = argv[0];
    if (argc != 1 && argc != 3) {
        print_usage(app_name);
        return EXIT_FAILURE;
    }

    static struct option long_options[] = {{"system_info", required_argument, 0, 's'},
                                           {"help", no_argument, 0, 'h'},
                                           {NULL, 0, 0, 0}};
    int value = 0;
    int option_index = 0;
    std::string system_info_filename = "./system.info";
    while ((value = getopt_long(argc, argv, "s:h", long_options, &option_index)) != -1) {
        switch (value) {
            case 's': {
                char *system_info_filename_ptr = strdup(optarg);
                system_info_filename = system_info_filename_ptr;
                free(system_info_filename_ptr);
//                printf("Generate system info file: %s\n", system_info_filename.c_str());
                break;
            }
            case 'h':print_usage(app_name);
                return EXIT_SUCCESS;
            case '?':print_usage(app_name);
                return EXIT_FAILURE;
            default:print_usage(app_name);
                break;
        }
    }

    int device_count = 0;
    server::ServerError err = server::LicenseLibrary::GetDeviceCount(device_count);
    if (err != server::SERVER_SUCCESS) return -1;

    // 1. Get All GPU UUID
    std::vector<std::string> uuid_array;
    err = server::LicenseLibrary::GetUUID(device_count, uuid_array);
    if (err != server::SERVER_SUCCESS) return -1;

    // 2. Get UUID SHA256
    std::vector<std::string> uuid_sha256_array;
    err = server::LicenseLibrary::GetUUIDSHA256(device_count, uuid_array, uuid_sha256_array);
    if (err != server::SERVER_SUCCESS) return -1;

    // 3. Generate GPU ID map with GPU UUID
    std::map<int, std::string> uuid_encrption_map;
    for (int i = 0; i < device_count; ++i) {
        uuid_encrption_map[i] = uuid_sha256_array[i];
    }


    // 4. Generate GPU_info File
    err = server::LicenseLibrary::GPUinfoFileSerialization(system_info_filename,
                                                           device_count,
                                                           uuid_encrption_map);
    if (err != server::SERVER_SUCCESS) return -1;

    printf("Generate GPU_info File Success\n");


    return 0;
}