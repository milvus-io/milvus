
#ifndef DATE_UTILS_TEST_H_
#define DATE_UTILS_TEST_H_

#include "test.h"

#include <thread>
#include <chrono>


TEST(DateUtilsTest, TimeFormatTest) {
    auto f = [](unsigned long long v) {
        return DateTime::formatTime(v, base::TimestampUnit::Millisecond);
    };

    ASSERT_EQ("2 ms", f(2));
    ASSERT_EQ("999 ms", f(999));
    ASSERT_EQ("1007 ms", f(1007));
    ASSERT_EQ("1899 ms", f(1899));
    ASSERT_EQ("1 seconds", f(1999));
    ASSERT_EQ("16 minutes", f(999000));
    ASSERT_EQ("24 hours", f(1 * 24 * 60 * 60 * 1000));
    ASSERT_EQ("2 days", f(2 * 24 * 60 * 60 * 1000));
    ASSERT_EQ("7 days", f(7 * 24 * 60 * 60 * 1000));
    ASSERT_EQ("15 days", f(15 * 24 * 60 * 60 * 1000));
}

TEST(DateUtilsTest, PerformanceTrackerTest) {
    {
        TIMED_SCOPE(timer, "1200 milliseconds wait");
        std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    }
    {
        TIMED_SCOPE(timer, "20 milliseconds wait");
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    {
        TIMED_SCOPE(timer, "20 microseconds wait");
        std::this_thread::sleep_for(std::chrono::microseconds(20));
    }
    {
        TIMED_SCOPE(timer, "886 milliseconds wait");
        std::this_thread::sleep_for(std::chrono::milliseconds(886));
    }
    {
        TIMED_SCOPE(timer, "1500 milliseconds wait");
        std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    }
    {
        TIMED_SCOPE(timer, "1400 milliseconds wait");
        std::this_thread::sleep_for(std::chrono::milliseconds(1400));
    }
    {
        TIMED_SCOPE(timer, "1600 milliseconds wait");
        std::this_thread::sleep_for(std::chrono::milliseconds(1600));
    }
}

#endif // DATE_UTILS_TEST_H_
