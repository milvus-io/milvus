
#ifndef HITCOUNTER_TESTS_H_
#define HITCOUNTER_TESTS_H_

#include "test.h"

TEST(RegisteredHitCountersTest, ValidationEveryN) {
    RegisteredHitCounters r;

    // Ensure no hit counters are registered yet
    EXPECT_TRUE(r.empty());

    unsigned long int line = __LINE__;
    r.validateEveryN(__FILE__, line, 2);

    // Confirm size
    EXPECT_EQ(1, r.size());

    // Confirm hit count
    EXPECT_EQ(1, r.getCounter(__FILE__, line)->hitCounts());

    // Confirm validations
    EXPECT_TRUE(r.validateEveryN(__FILE__, line, 2));
    EXPECT_FALSE(r.validateEveryN(__FILE__, line, 2));

    EXPECT_TRUE(r.validateEveryN(__FILE__, line, 2));
    EXPECT_FALSE(r.validateEveryN(__FILE__, line, 2));

    EXPECT_TRUE(r.validateEveryN(__FILE__, line, 2));
    EXPECT_FALSE(r.validateEveryN(__FILE__, line, 2));

    line = __LINE__;
    r.validateEveryN(__FILE__, line, 3);
    // Confirm size
    EXPECT_EQ(2, r.size());
    // Confirm hitcounts
    EXPECT_EQ(1, r.getCounter(__FILE__, line)->hitCounts());

    // Confirm validations
    EXPECT_FALSE(r.validateEveryN(__FILE__, line, 3));
    EXPECT_TRUE(r.validateEveryN(__FILE__, line, 3));
    EXPECT_FALSE(r.validateEveryN(__FILE__, line, 3));

    EXPECT_FALSE(r.validateEveryN(__FILE__, line, 3));
    EXPECT_TRUE(r.validateEveryN(__FILE__, line, 3));
    EXPECT_FALSE(r.validateEveryN(__FILE__, line, 3));

    EXPECT_FALSE(r.validateEveryN(__FILE__, line, 3));
    EXPECT_TRUE(r.validateEveryN(__FILE__, line, 3));
    EXPECT_FALSE(r.validateEveryN(__FILE__, line, 3));

    EXPECT_FALSE(r.validateEveryN(__FILE__, line, 3));
    EXPECT_TRUE(r.validateEveryN(__FILE__, line, 3));
    EXPECT_FALSE(r.validateEveryN(__FILE__, line, 3));

    EXPECT_FALSE(r.validateEveryN(__FILE__, line, 3));
    EXPECT_TRUE(r.validateEveryN(__FILE__, line, 3));
    EXPECT_FALSE(r.validateEveryN(__FILE__, line, 3));

    // Confirm size once again
    EXPECT_EQ(2, r.size());
}

TEST(RegisteredHitCountersTest, ValidationAfterN) {
    RegisteredHitCounters r;

    // Ensure no hit counters are registered yet
    EXPECT_TRUE(r.empty());

    unsigned long int line = __LINE__;
    unsigned int n = 2;
    
    // Register
    r.validateAfterN(__FILE__, line, n); // 1

    // Confirm size
    EXPECT_EQ(1, r.size());

    // Confirm hit count
    EXPECT_EQ(1, r.getCounter(__FILE__, line)->hitCounts());

    // Confirm validations
    EXPECT_FALSE(r.validateAfterN(__FILE__, line, n)); // 2
    EXPECT_TRUE(r.validateAfterN(__FILE__, line, n)); // 3
    EXPECT_TRUE(r.validateAfterN(__FILE__, line, n)); // 4
    EXPECT_TRUE(r.validateAfterN(__FILE__, line, n)); // 5
    EXPECT_TRUE(r.validateAfterN(__FILE__, line, n)); // 6
}

TEST(RegisteredHitCountersTest, ValidationNTimes) {
    RegisteredHitCounters r;

    // Ensure no hit counters are registered yet
    EXPECT_TRUE(r.empty());

    unsigned long int line = __LINE__;
    unsigned int n = 5;
    
    // Register
    r.validateNTimes(__FILE__, line, n); // 1

    // Confirm size
    EXPECT_EQ(1, r.size());

    // Confirm hit count
    EXPECT_EQ(1, r.getCounter(__FILE__, line)->hitCounts());

    // Confirm validations
    EXPECT_TRUE(r.validateNTimes(__FILE__, line, n)); // 2
    EXPECT_TRUE(r.validateNTimes(__FILE__, line, n)); // 3
    EXPECT_TRUE(r.validateNTimes(__FILE__, line, n)); // 4
    EXPECT_TRUE(r.validateNTimes(__FILE__, line, n)); // 5
    EXPECT_FALSE(r.validateNTimes(__FILE__, line, n)); // 6
    EXPECT_FALSE(r.validateNTimes(__FILE__, line, n)); // 7
    EXPECT_FALSE(r.validateNTimes(__FILE__, line, n)); // 8
    EXPECT_FALSE(r.validateNTimes(__FILE__, line, n)); // 9
}
#endif /* HITCOUNTER_TESTS_H_ */
