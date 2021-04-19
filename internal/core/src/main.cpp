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

#include "config/ConfigMgr.h"
#include "easyloggingpp/easylogging++.h"
#include "utils/SignalHandler.h"
#include "utils/Status.h"

INITIALIZE_EASYLOGGINGPP

void
print_help(const std::string& app_name) {
    std::cout << std::endl << "Usage: " << app_name << " [OPTIONS]" << std::endl;
    std::cout << R"(
  Options:
   -h --help                 Print this help.
   -c --conf_file filename   Read configuration from the file.
   -d --daemon               Daemonize this application.
   -p --pid_file  filename   PID file used by daemonized app.
)" << std::endl;
}

void
print_banner() {
    std::cout << std::endl;
    std::cout << "    __  _________ _   ____  ______    " << std::endl;
    std::cout << "   /  |/  /  _/ /| | / / / / / __/    " << std::endl;
    std::cout << "  / /|_/ // // /_| |/ / /_/ /\\ \\    " << std::endl;
    std::cout << " /_/  /_/___/____/___/\\____/___/     " << std::endl;
    std::cout << std::endl;
}

int
main(int argc, char* argv[]) {
    print_banner();
}
