#ifndef LOGGABLETEST_H_
#define LOGGABLETEST_H_

#include "test.h"

class Integer : public el::Loggable {
public:
    Integer(int i) : m_underlyingInt(i) {
    }
    Integer& operator=(const Integer& integer) {
        m_underlyingInt = integer.m_underlyingInt;
        return *this;
    }
    virtual ~Integer(void) {
    }
    inline operator int() const {
        return m_underlyingInt;
    }
    inline void operator++() {
        ++m_underlyingInt;
    }
    inline void operator--() {
        --m_underlyingInt;
    }
    inline bool operator==(const Integer& integer) const {
        return m_underlyingInt == integer.m_underlyingInt;
    }
    void inline log(el::base::type::ostream_t& os) const {
        os << m_underlyingInt;
    }
private:
    int m_underlyingInt;
};


TEST(LoggableTest, TestValidLog) {
    Integer myint = 5;
    LOG(INFO) << "My integer = " << myint;
    std::string expected = BUILD_STR(getDate() << " My integer = 5\n");
    EXPECT_EQ(expected, tail(1));
    ++myint;
    LOG(INFO) << "My integer = " << myint;
    expected = BUILD_STR(getDate() << " My integer = 6\n");
    EXPECT_EQ(expected, tail(1));
}

class String {
public:
    String(const char* s) : m_str(s) {}
    const char* c_str(void) const { return m_str; }
private:
    const char* m_str;
};

inline MAKE_LOGGABLE(String, str, os) {
    os << str.c_str();
    return os;
}

TEST(LoggableTest, MakeLoggable) {
    LOG(INFO) << String("this is my string");
    std::string expected = BUILD_STR(getDate() << " this is my string\n");
    EXPECT_EQ(expected, tail(1));
}

#endif // LOGGABLETEST_H_
