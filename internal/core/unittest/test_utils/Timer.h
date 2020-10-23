#pragma once
#include <chrono>

class Timer {
 public:
    Timer() {
        reset();
    }

    double
    get_overall_seconds() {
        using namespace std::chrono;
        auto now = high_resolution_clock::now();
        auto diff = now - init_record;
        step_record = now;
        return (double)duration_cast<microseconds>(diff).count() * 1e-6;
    }

    double
    get_step_seconds() {
        using namespace std::chrono;
        auto now = high_resolution_clock::now();
        auto diff = now - step_record;
        step_record = now;
        return (double)duration_cast<microseconds>(diff).count() * 1e-6;
    }

    void
    reset() {
        using namespace std::chrono;
        step_record = init_record = high_resolution_clock::now();
    }

 private:
    using nanosecond_t = std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds>;

 private:
    nanosecond_t init_record;
    nanosecond_t step_record;
};