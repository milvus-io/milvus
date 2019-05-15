
#include <iostream>
#include <getopt.h>
#include <memory.h>

#include "utils/Log.h"
#include "license/LicenseLibrary.h"
#include "utils/Error.h"


using namespace zilliz::vecwise;
// Not provide path: current work path will be used and system.info.

void
print_usage(const std::string &app_name) {
    printf("\n Usage: %s [OPTIONS]\n\n", app_name.c_str());
    printf("  Options:\n");
    printf("   -h --help               Print this help\n");
    printf("   -s --sysinfo filename   sysinfo file location\n");
    printf("   -l --license filename   Generate license file as given name\n");
    printf("   -b --starting time      Set start time (format: YYYY-MM-DD)\n");
    printf("   -e --end time           Set end time (format: YYYY-MM-DD)\n");
    printf("\n");
}

int main(int argc, char *argv[]) {
    std::string app_name = argv[0];
//    if (argc != 1 && argc != 3) {
//        print_usage(app_name);
//        return EXIT_FAILURE;
//    }
    static struct option long_options[] = {{"system_info", required_argument, 0, 's'},
                                           {"license", optional_argument, 0, 'l'},
                                           {"help", no_argument, 0, 'h'},
                                           {"starting_time", required_argument, 0, 'b'},
                                           {"end_time", required_argument, 0, 'e'},
                                           {NULL, 0, 0, 0}};
    server::ServerError err;
    int value = 0;
    int option_index = 0;
    std::string system_info_filename = "./system.info";
    std::string license_filename = "./system.license";
    char *string_starting_time = NULL;
    char *string_end_time = NULL;
    time_t starting_time = 0;
    time_t end_time = 0;
    int flag_s = 1;
    int flag_b = 1;
    int flag_e = 1;
    while ((value = getopt_long(argc, argv, "hl:s:b:e:", long_options, NULL)) != -1) {
        switch (value) {
            case 's': {
                flag_s = 0;
                system_info_filename = (std::string) (optarg);
                break;
            }
            case 'b': {
                flag_b = 0;
                string_starting_time = optarg;
                break;
            }
            case 'e': {
                flag_e = 0;
                string_end_time = optarg;
                break;
            }
            case 'l': {
                license_filename = (std::string) (optarg);
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
    if (flag_s) {
        printf("Error: sysinfo file location must be entered\n");
        return 1;
    }
    if (flag_b) {
        printf("Error: start time must be entered\n");
        return 1;
    }
    if (flag_e) {
        printf("Error: end time must be entered\n");
        return 1;
    }

    err = server::LicenseLibrary::GetDateTime(string_starting_time, starting_time);
    if (err != server::SERVER_SUCCESS) return -1;

    err = server::LicenseLibrary::GetDateTime(string_end_time, end_time);
    if (err != server::SERVER_SUCCESS) return -1;


    int output_info_device_count = 0;
    std::map<int, std::string> output_info_uuid_encrption_map;


    err = server::LicenseLibrary::GPUinfoFileDeserialization(system_info_filename,
                                                             output_info_device_count,
                                                             output_info_uuid_encrption_map);
    if (err != server::SERVER_SUCCESS) return -1;


    err = server::LicenseLibrary::LicenseFileSerialization(license_filename,
                                                           output_info_device_count,
                                                           output_info_uuid_encrption_map,
                                                           starting_time,
                                                           end_time);
    if (err != server::SERVER_SUCCESS) return -1;


    printf("Generate License File Success\n");

    return 0;
}
