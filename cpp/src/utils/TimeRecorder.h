/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <string>
#include <chrono>


namespace zilliz {
namespace milvus {
namespace server {

class TimeRecorder {
    using stdclock = std::chrono::high_resolution_clock;

public:
    enum TimeDisplayUnit {
        eTimeAutoUnit = 0,
        eTimeHourUnit,
        eTimeMinuteUnit,
        eTimeSecondUnit,
        eTimeMilliSecUnit,
        eTimeMicroSecUnit,
    };

    TimeRecorder(const std::string &header,
                 TimeRecorder::TimeDisplayUnit unit = TimeRecorder::eTimeAutoUnit,
                 int64_t log_level = 1); //trace = 0, debug = 1, info = 2, warn = 3, error = 4, critical = 5

    void Record(const std::string &msg);

    void Elapse(const std::string &msg);

    double Span();

private:
    std::string GetTimeSpanStr(TimeRecorder::TimeDisplayUnit &unit, double span) const;

    void PrintTimeRecord(const std::string &msg, double span);

private:
    std::string header_;
    TimeRecorder::TimeDisplayUnit time_unit_;
    stdclock::time_point start_;
    stdclock::time_point last_;
    double span_;
    int64_t log_level_;
};

}
}
}
