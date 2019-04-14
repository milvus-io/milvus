 //
 // This file is part of Easylogging++ samples
 //
 // Demonstration on how locale gives output
 //
 // Revision 1.1
 // @author mkhan3189
 //
#ifndef ELPP_UNICODE
#   define ELPP_UNICODE
#endif

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

int main(int argc, const char** argv) {
    START_EASYLOGGINGPP(argc, argv);

    LOG(INFO) << L"世界，你好";
    return 0;
}
