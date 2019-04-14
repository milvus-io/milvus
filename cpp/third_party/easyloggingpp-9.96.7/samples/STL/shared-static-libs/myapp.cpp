#include <mylib.hpp>
#include "easylogging++.h"
INITIALIZE_EASYLOGGINGPP
int main(int argc, char** argv) {
    int result = 0;
    LOG(INFO) << "Log from app";
    //
    // You can choose MyLib() constructor
    // but be aware this will cause vlog to not work because Easylogging++
    // does not know verbose logging level
    //
    // For your peace of mind, you may pass on const_cast<const char**>(argv) instead
    //
    MyLib lib(argc, argv);
    lib.event(1);
    return result;
}
