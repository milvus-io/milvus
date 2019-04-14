#include "mylib.hpp"
#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

MyLib::MyLib() {
    LOG(INFO) << "---MyLib Constructor () ---";
}

MyLib::MyLib(int argc, char** argv) {
    START_EASYLOGGINGPP(argc, argv);
    LOG(INFO) << "---MyLib Constructor(int, char**) ---";
}

MyLib::~MyLib() {
    LOG(INFO) << "---MyLib Destructor---";
}


void MyLib::event(int a) {
    VLOG(1) << "MyLib::event start";
    LOG(INFO) << "Processing event [" << a << "]";
    VLOG(1) << "MyLib::event end";
}
