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
    TimeRecorder(const std::string &header,
                 int64_t log_level = 1);

    ~TimeRecorder();//trace = 0, debug = 1, info = 2, warn = 3, error = 4, critical = 5

    double RecordSection(const std::string &msg);

    double ElapseFromBegin(const std::string &msg);

    static std::string GetTimeSpanStr(double span);

private:
    void PrintTimeRecord(const std::string &msg, double span);

private:
    std::string header_;
    stdclock::time_point start_;
    stdclock::time_point last_;
    int64_t log_level_;
};

}
}
}
