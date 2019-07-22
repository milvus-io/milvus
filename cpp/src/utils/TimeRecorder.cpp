////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include "TimeRecorder.h"
#include "utils/Log.h"


namespace zilliz {
namespace milvus {
namespace server {

TimeRecorder::TimeRecorder(const std::string &header,
                           int64_t log_level) :
    header_(header),
    log_level_(log_level) {
    start_ = last_ = stdclock::now();
}

TimeRecorder::~TimeRecorder() {
}

std::string
TimeRecorder::GetTimeSpanStr(double span) {
    std::string str_sec = std::to_string(span * 0.000001) + ((span > 1000000) ? " seconds" : " second");
    std::string str_ms = std::to_string(span * 0.001) + " ms";

    return str_sec + " [" + str_ms + "]";
}

void
TimeRecorder::PrintTimeRecord(const std::string &msg, double span) {
    std::string str_log;
    if (!header_.empty()) str_log += header_ + ": ";
    str_log += msg;
    str_log += " (";
    str_log += TimeRecorder::GetTimeSpanStr(span);
    str_log += ")";

    switch (log_level_) {
        case 0: {
            SERVER_LOG_TRACE << str_log;
            break;
        }
        case 1: {
            SERVER_LOG_DEBUG << str_log;
            break;
        }
        case 2: {
            SERVER_LOG_INFO << str_log;
            break;
        }
        case 3: {
            SERVER_LOG_WARNING << str_log;
            break;
        }
        case 4: {
            SERVER_LOG_ERROR << str_log;
            break;
        }
        case 5: {
            SERVER_LOG_FATAL << str_log;
            break;
        }
        default: {
            SERVER_LOG_INFO << str_log;
            break;
        }
    }
}

double
TimeRecorder::RecordSection(const std::string &msg) {
    stdclock::time_point curr = stdclock::now();
    double span = (std::chrono::duration<double, std::micro>(curr - last_)).count();
    last_ = curr;

    PrintTimeRecord(msg, span);
    return span;
}

double
TimeRecorder::ElapseFromBegin(const std::string &msg) {
    stdclock::time_point curr = stdclock::now();
    double span = (std::chrono::duration<double, std::micro>(curr - start_)).count();

    PrintTimeRecord(msg, span);
    return span;
}

}
}
}
