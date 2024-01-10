#pragma once

#include <chrono>
#include <iostream>
#include <string>

class TimeRecorder {
    using stdclock = std::chrono::high_resolution_clock;

 public:
    // trace = 0, debug = 1, info = 2, warn = 3, error = 4, critical = 5
    explicit TimeRecorder(std::string hdr, int64_t log_level = 0)
        : header_(std::move(hdr)), log_level_(log_level) {
        start_ = last_ = stdclock::now();
    }
    virtual ~TimeRecorder() = default;

    double
    RecordSection(const std::string& msg) {
        stdclock::time_point curr = stdclock::now();
        double span =
            (std::chrono::duration<double, std::micro>(curr - last_)).count();
        last_ = curr;

        PrintTimeRecord(msg, span);
        return span;
    }

    double
    ElapseFromBegin(const std::string& msg) {
        stdclock::time_point curr = stdclock::now();
        double span =
            (std::chrono::duration<double, std::micro>(curr - start_)).count();

        PrintTimeRecord(msg, span);
        return span;
    }

    static std::string
    GetTimeSpanStr(double span) {
        std::string str_ms = std::to_string(span * 0.001) + " ms";
        return str_ms;
    }

 private:
    void
    PrintTimeRecord(const std::string& msg, double span) {
        std::string str_log;
        if (!header_.empty()) {
            str_log += header_ + ": ";
        }
        str_log += msg;
        str_log += " (";
        str_log += TimeRecorder::GetTimeSpanStr(span);
        str_log += ")";

        std::cout << str_log << std::endl;
    }

 private:
    std::string header_;
    stdclock::time_point start_;
    stdclock::time_point last_;
    int64_t log_level_;
};
