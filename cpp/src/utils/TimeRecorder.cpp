////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include "TimeRecorder.h"
#include "utils/Log.h"


namespace zilliz {
namespace vecwise {
namespace server {

TimeRecorder::TimeRecorder(const std::string &header,
                           TimeRecorder::TimeDisplayUnit unit,
                           int64_t log_level) :
        header_(header),
        time_unit_(unit),
        log_level_(log_level) {
    start_ = last_ = stdclock::now();
    span_ = 0.0;
}

std::string
TimeRecorder::GetTimeSpanStr(TimeRecorder::TimeDisplayUnit &unit, double span) const {
    std::string spanStr;
    std::string unitStr;

    switch (unit) {
        case TimeRecorder::eTimeAutoUnit: {
            if (span >= 1000000) {
                int64_t t = (int64_t) span;
                int64_t hour, minute;
                double second;
                hour = t / 1000000 / 3600;
                t -= hour * 3600 * 1000000;
                minute = t / 1000000 / 60;
                t -= minute * 60 * 1000000;
                second = t * 0.000001;
                spanStr += (hour < 10 ? "0" : "") + std::to_string(hour) + ":";
                spanStr += (minute < 10 ? "0" : "") + std::to_string(minute) + ":";
                spanStr += (second < 10 ? "0" : "") + std::to_string(second);
                unitStr = "";
            } else if (span >= 1000) {
                spanStr = std::to_string(span * 0.001);
                unitStr = " ms";
            } else {
                spanStr = std::to_string(span);
                unitStr = " us";
            }
        }
            break;
        case TimeRecorder::eTimeHourUnit:
            spanStr = std::to_string((span * 0.000001) / 3600);
            unitStr = " hour";
            break;
        case TimeRecorder::eTimeMinuteUnit:
            spanStr = std::to_string((span * 0.000001) / 60);
            unitStr = " min";
            break;
        case TimeRecorder::eTimeSecondUnit:
            spanStr = std::to_string(span * 0.000001);
            unitStr = " sec";
            break;
        case TimeRecorder::eTimeMilliSecUnit:
            spanStr = std::to_string(span * 0.001);
            unitStr = " ms";
            break;
        case TimeRecorder::eTimeMicroSecUnit:
        default:
            spanStr = std::to_string(span);
            unitStr = " us";
            break;
    }

    return spanStr + unitStr;
}

void
TimeRecorder::PrintTimeRecord(const std::string &msg, double span) {
    std::string strLog;
    if (!header_.empty()) strLog += header_ + ": ";
    strLog += msg;
    strLog += " (";
    strLog += GetTimeSpanStr(time_unit_, span);
    strLog += ")";

    switch (log_level_) {
        case 0: {
            SERVER_LOG_TRACE << strLog;
            break;
        }
        case 1: {
            SERVER_LOG_DEBUG << strLog;
            break;
        }
        case 2: {
            SERVER_LOG_INFO << strLog;
            break;
        }
        case 3: {
            SERVER_LOG_WARNING << strLog;
            break;
        }
        case 4: {
            SERVER_LOG_ERROR << strLog;
            break;
        }
        case 5: {
            SERVER_LOG_FATAL << strLog;
            break;
        }
        default: {
            SERVER_LOG_INFO << strLog;
            break;
        }
    }
}

void
TimeRecorder::Record(const std::string &msg) {
    stdclock::time_point curr = stdclock::now();
    span_ = (std::chrono::duration<double, std::micro>(curr - last_)).count();
    last_ = curr;

    PrintTimeRecord(msg, span_);
}

void
TimeRecorder::Elapse(const std::string &msg) {
    stdclock::time_point curr = stdclock::now();
    span_ = (std::chrono::duration<double, std::micro>(curr - start_)).count();

    PrintTimeRecord(msg, span_);
}

double
TimeRecorder::Span() {
    return span_;
}

}
}
}
