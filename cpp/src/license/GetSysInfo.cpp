#include <iostream>
#include <getopt.h>
#include <memory.h>

// Not provide path: current work path will be used and system.info.

void
print_usage(const std::string &app_name) {
    printf("\n Usage: %s [OPTIONS]\n\n", app_name.c_str());
    printf("  Options:\n");
    printf("   -h --help               Print this help\n");
    printf("   -d --sysinfo filename   Generate system info file as given name\n");
    printf("\n");
}

int main(int argc, char* argv[]) {
    std::string app_name = argv[0];
    if(argc != 1 && argc != 3) {
        print_usage(app_name);
        return EXIT_FAILURE;
    }

    static struct option long_options[] = {{"system_info", required_argument, 0, 'd'},
                                           {"help", no_argument, 0, 'h'},
                                           {NULL, 0, 0, 0}};
    int value = 0;
    int option_index = 0;
    std::string system_info_filename = "./system.info";
    while ((value = getopt_long(argc, argv, "d:h", long_options, &option_index)) != -1) {
        switch (value) {
            case 'd': {
                char *system_info_filename_ptr = strdup(optarg);
                system_info_filename = system_info_filename_ptr;
                free(system_info_filename_ptr);
//                printf("Generate system info file: %s\n", system_info_filename.c_str());
                break;
            }
            case 'h':
                print_usage(app_name);
                return EXIT_SUCCESS;
            case '?':
                print_usage(app_name);
                return EXIT_FAILURE;
            default:
                print_usage(app_name);
                break;
        }
    }

    return 0;
}