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

#include <fiu/fiu-local.h>
#include <libgen.h>
#include <cctype>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/filesystem.hpp>

#include "log/LogMgr.h"
#include "utils/Status.h"
#include "value/config/ServerConfig.h"

namespace milvus {

int LogMgr::trace_idx = 0;
int LogMgr::global_idx = 0;
int LogMgr::debug_idx = 0;
int LogMgr::info_idx = 0;
int LogMgr::warning_idx = 0;
int LogMgr::error_idx = 0;
int LogMgr::fatal_idx = 0;
int64_t LogMgr::logs_delete_exceeds = 1;
bool LogMgr::enable_log_delete = false;

Status
LogMgr::InitLog(bool trace_enable,
                const std::string& level,
                const std::string& logs_path,
                const std::string& filename,
                int64_t max_log_file_size,
                int64_t log_rotate_num,
                bool log_to_stdout,
                bool log_to_file) {
    try {
        auto enables = parse_level(level);
        enables["trace"] = trace_enable;
        LogMgr log_mgr(logs_path);
        log_mgr.Default()
            .Filename(filename)
            .Level(enables)
            .To(log_to_stdout, log_to_file)
            .Rotate(max_log_file_size, log_rotate_num)
            .Setup();
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

// TODO(yzb) : change the easylogging library to get the log level from parameter rather than filename
void
LogMgr::RolloutHandler(const char* filename, std::size_t size, el::Level level) {
    char* dirc = strdup(filename);
    char* basec = strdup(filename);
    char* dir = dirname(dirc);
    char* base = basename(basec);

    std::string s(base);
    std::vector<std::string> list = {"\\", " ", "\'", "\"", "*", "\?", "{", "}", ";", "<",
                                     ">",  "|", "^",  "&",  "$", "#",  "!", "`", "~"};
    std::string::size_type position;
    for (auto& substr : list) {
        position = 0;
        while ((position = s.find_first_of(substr, position)) != std::string::npos) {
            s.insert(position, "\\");
            position += 2;
        }
    }
    std::string m(std::string(dir) + "/" + s);
    try {
        switch (level) {
            case el::Level::Trace: {
                rename_and_delete(m, ++trace_idx);
                break;
            }
            case el::Level::Global: {
                rename_and_delete(m, ++global_idx);
                break;
            }
            case el::Level::Debug: {
                rename_and_delete(m, ++debug_idx);
                break;
            }
            case el::Level::Info: {
                rename_and_delete(m, ++info_idx);
                break;
            }
            case el::Level::Warning: {
                rename_and_delete(m, ++warning_idx);
                break;
            }
            case el::Level::Error: {
                rename_and_delete(m, ++error_idx);
                break;
            }
            case el::Level::Fatal: {
                rename_and_delete(m, ++fatal_idx);
                break;
            }
            default: {
                break;
            }
        }
    } catch (const std::exception& exc) {
        std::cerr << exc.what() << ". Exception throws from RolloutHandler." << std::endl;
    }
}

LogMgr::LogMgr(std::string log_path) : logs_path_(std::move(log_path)) {
}

LogMgr&
LogMgr::Default() {
    el_config_.setToDefault();
    el_config_.setGlobally(el::ConfigurationType::Format, "[%datetime][%level]%msg");
    el_config_.setGlobally(el::ConfigurationType::SubsecondPrecision, "3");
    el_config_.setGlobally(el::ConfigurationType::PerformanceTracking, "false");

    return *this;
}

LogMgr&
LogMgr::Filename(const std::string& filename) {
    std::string logs_reg_path = logs_path_.rfind('/') == logs_path_.length() - 1 ? logs_path_ : logs_path_ + "/";

    std::string log_file = logs_reg_path + filename;

    /* Set set log file at Global level to make all level log output to the same log file*/
    el_config_.set(el::Level::Global, el::ConfigurationType::Filename, log_file.c_str());

    return *this;
}

LogMgr&
LogMgr::Level(std::unordered_map<std::string, bool>& enables) {
    fiu_do_on("LogMgr.Level.trace_enable_to_false", enables["trace"] = false);
    enable(el_config_, el::Level::Trace, enables["trace"]);

    fiu_do_on("LogMgr.Level.info_enable_to_false", enables["info"] = false);
    enable(el_config_, el::Level::Info, enables["info"]);

    fiu_do_on("LogMgr.Level.debug_enable_to_false", enables["debug"] = false);
    enable(el_config_, el::Level::Debug, enables["debug"]);

    fiu_do_on("LogMgr.Level.warning_enable_to_false", enables["warning"] = false);
    enable(el_config_, el::Level::Warning, enables["warning"]);

    fiu_do_on("LogMgr.Level.error_enable_to_false", enables["error"] = false);
    enable(el_config_, el::Level::Error, enables["error"]);

    fiu_do_on("LogMgr.Level.fatal_enable_to_false", enables["fatal"] = false);
    enable(el_config_, el::Level::Fatal, enables["fatal"]);

    return *this;
}

LogMgr&
LogMgr::To(bool log_to_stdout, bool log_to_file) {
    el_config_.setGlobally(el::ConfigurationType::ToStandardOutput, (log_to_stdout ? "true" : "false"));
    el_config_.setGlobally(el::ConfigurationType::ToFile, (log_to_file ? "true" : "false"));

    return *this;
}

LogMgr&
LogMgr::Rotate(int64_t max_log_file_size, int64_t log_rotate_num) {
    fiu_do_on("LogMgr.Rotate.set_max_log_size_small_than_min", max_log_file_size = MAX_LOG_FILE_SIZE_MIN - 1);
    if (max_log_file_size < MAX_LOG_FILE_SIZE_MIN || max_log_file_size > MAX_LOG_FILE_SIZE_MAX) {
        std::string msg = "max_log_file_size must in range[" + std::to_string(MAX_LOG_FILE_SIZE_MIN) + ", " +
                          std::to_string(MAX_LOG_FILE_SIZE_MAX) + "], now is " + std::to_string(max_log_file_size);
        throw std::runtime_error(msg);
    }

    el_config_.setGlobally(el::ConfigurationType::MaxLogFileSize, std::to_string(max_log_file_size));
    el::Loggers::addFlag(el::LoggingFlag::StrictLogFileSizeCheck);
    el::Helpers::installPreRollOutCallback(LogMgr::RolloutHandler);
    el::Loggers::addFlag(el::LoggingFlag::DisableApplicationAbortOnFatalLog);

    // set delete_exceeds = 0 means disable throw away log file even they reach certain limit.
    if (log_rotate_num != 0) {
        fiu_do_on("LogMgr.Rotate.delete_exceeds_small_than_min", log_rotate_num = LOG_ROTATE_NUM_MIN - 1);
        if (log_rotate_num < LOG_ROTATE_NUM_MIN || log_rotate_num > LOG_ROTATE_NUM_MAX) {
            std::string msg = "log_rotate_num must in range[" + std::to_string(LOG_ROTATE_NUM_MIN) + ", " +
                              std::to_string(LOG_ROTATE_NUM_MAX) + "], now is " + std::to_string(log_rotate_num);
            throw std::runtime_error(msg);
        }

        /* global variable */
        enable_log_delete = true;
        logs_delete_exceeds = log_rotate_num;
    }

    return *this;
}

void
LogMgr::Setup() {
    el::Loggers::reconfigureLogger("default", el_config_);
}

void
LogMgr::rename_and_delete(const std::string& filename, int64_t idx) {
    std::string target_filename = filename + "." + std::to_string(idx);
    rename(filename.c_str(), target_filename.c_str());
    if (enable_log_delete && idx - logs_delete_exceeds > 0) {
        std::string to_delete = filename + "." + std::to_string(trace_idx - logs_delete_exceeds);
        boost::filesystem::remove(to_delete);
    }
}

std::unordered_map<std::string, bool>
LogMgr::parse_level(const std::string& level) {
    std::unordered_map<std::string, bool> enables{
        {"debug", false}, {"info", false}, {"warning", false}, {"error", false}, {"fatal", false},
    };

    std::unordered_map<std::string, int64_t> level_to_int{
        {"debug", 5}, {"info", 4}, {"warning", 3}, {"error", 2}, {"fatal", 1},
    };

    switch (level_to_int[level]) {
        case 5:
            enables["debug"] = true;
        case 4:
            enables["info"] = true;
        case 3:
            enables["warning"] = true;
        case 2:
            enables["error"] = true;
        case 1:
            enables["fatal"] = true;
            break;
        default: {
            std::string msg = "Cannot parse level " + level +
                              ": invalid log level, must be one of debug, info, warning, error, fatal.";
            throw std::runtime_error(msg);
        }
    }
    return enables;
}

void
LogMgr::enable(el::Configurations& default_conf, el::Level level, bool enable) {
    std::string enable_str = enable ? "true" : "false";
    default_conf.set(level, el::ConfigurationType::Enabled, enable_str);
}

}  // namespace milvus
