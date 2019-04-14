 //
 // This file is part of Easylogging++ samples
 //
 // Usage of MAKE_LOGGABLE to make class log-friendly
 //
 // Revision 1.1
 // @author mkhan3189
 //

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

class Integer {
public:
    Integer(int i) : m_underlyingInt(i) {}
    Integer& operator=(const Integer& integer) { m_underlyingInt = integer.m_underlyingInt; return *this; }
    virtual ~Integer(void) { m_underlyingInt = -1; }
    int getInt(void) const { return m_underlyingInt; }
    inline operator int() const { return m_underlyingInt; }
private:
    int m_underlyingInt;
};

// Lets say Integer class is in some third party library

// We use MAKE_LOGGABLE(class, instance, outputStream) to make it loggable
inline MAKE_LOGGABLE(Integer, integer, os) {
    os << integer.getInt();
    return os;
}


int main(void) {
    
    Integer count = 5;
    LOG(INFO) << "Integer count = " << count;
    int reverse = count;
    LOG(INFO) << "int reverse = " << reverse;
    return 0;
}
