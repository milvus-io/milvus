//
// This header is used across tests to include all the valid headers
//

#ifndef TEST_HELPERS_H_
#define TEST_HELPERS_H_

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wundef"
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <gtest/gtest.h>
#pragma GCC diagnostic pop

#include "easylogging++.h"

using namespace el;
using namespace el::base;
using namespace el::base::utils;

INITIALIZE_EASYLOGGINGPP

static const char* logfile = "/tmp/logs/el.gtest.log";

static void reconfigureLoggersForTest(void) {
    Configurations c;
    c.setGlobally(ConfigurationType::Format, "%datetime{%a %b %d, %H:%m} %msg");
    c.setGlobally(ConfigurationType::Filename, "/tmp/logs/el.gtest.log");
    c.setGlobally(ConfigurationType::MaxLogFileSize, "2097152"); // 2MB
    c.setGlobally(ConfigurationType::ToStandardOutput, "false");
    c.setGlobally(ConfigurationType::PerformanceTracking, "true");
    c.setGlobally(ConfigurationType::LogFlushThreshold, "1");
    Loggers::setDefaultConfigurations(c, true);
    // We do not want to reconfgure syslog with date/time
    Loggers::reconfigureLogger(consts::kSysLogLoggerId, ConfigurationType::Format, "%level: %msg");

    Loggers::addFlag(LoggingFlag::DisableApplicationAbortOnFatalLog);
    Loggers::addFlag(LoggingFlag::ImmediateFlush);
    Loggers::addFlag(LoggingFlag::StrictLogFileSizeCheck);
    Loggers::addFlag(LoggingFlag::DisableVModulesExtensions);
}

static std::string tail(unsigned int n, const char* filename = logfile) {
    if (n == 0) return std::string();
    std::ifstream fstr(filename);
    if (!fstr.is_open()) {
        return std::string();
    }
    fstr.seekg(0, fstr.end);
    int size = static_cast<int>(fstr.tellg());
    int ncopy = n + 1;
    for (int i = (size - 1); i >= 0; --i) {
        fstr.seekg(i);
        char c = fstr.get();
        if (c == '\n' && --ncopy == 0) {
            break;
        }
        if (i == 0) {
            fstr.seekg(i); // fstr.get() increments buff, so we reset it
        }
    }
    std::stringstream ss;
    char c = fstr.get();
    while (fstr.good()) {
        ss << c;
        c = fstr.get();
    }
    fstr.close();
    return ss.str();
}

static std::string getDate(const char* format = "%a %b %d, %H:%m") {
    SubsecondPrecision ssPrec(3);
    return DateTime::getDateTime(format, &ssPrec);
}

static bool fileExists(const char* filename) {
    el::base::type::fstream_t fstr(filename, el::base::type::fstream_t::in);
    return fstr.is_open();
}

static void cleanFile(const char* filename = logfile, el::base::type::fstream_t* fs = nullptr) {
    if (fs != nullptr && fs->is_open()) {
        fs->close();
        fs->open(filename, el::base::type::fstream_t::out);
    } else {
        el::base::type::fstream_t f(filename, el::base::type::fstream_t::out);
        if (f.is_open()) {
            f.close();
        }
        ELPP_UNUSED(f);
    }
}

#undef BUILD_STR
#define BUILD_STR(strb) [&]() -> std::string { std::stringstream ssb; ssb << strb; return ssb.str(); }()

static void removeFile(const char* path) {
        (void)(system(BUILD_STR("rm -rf " << path).c_str()) + 1); // (void)(...+1) -> ignore result for gcc 4.6+
}

static const char* kSysLogIdent = "easylogging++ unit test";
#endif // TEST_HELPERS_H_
