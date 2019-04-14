#include "mylib.hh"

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

Mylib::Mylib(void)
{
    LOG(INFO) << "Mylib has been constructed";
}

Mylib::~Mylib()
{
    LOG(INFO) << "Destroying Mylib";
}

float Mylib::add(float x, float y) const
{
    LOG(INFO) << "Adding " << x << " and " << y;
    return x + y;
}

float Mylib::sub(float x, float y) const
{
    LOG(INFO) << "Subtracting " << y << " from " << x;
    return y - x;
}

float Mylib::mul(float x, float y) const
{
    LOG(INFO) << "Multiplying " << x << " and " << y;
    return x * y;
}

float Mylib::div(float x, float y) const
{
    CHECK_NE(y, 0) << "Division by zero!";
    LOG(INFO) << "Dividing " << x << " by " << y;
    return x / y;
}
