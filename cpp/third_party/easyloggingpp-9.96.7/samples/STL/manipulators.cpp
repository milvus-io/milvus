 //
 // This file is part of Easylogging++ samples
 //
 // Demonstration of manipulators usages and how they behave
 //
 // Revision 1.1
 // @author mkhan3189
 //

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

int main(void) {
    
    LOG(INFO) << "std::endl" << std::endl;
    LOG(INFO) << "std::flush" << std::flush;
    LOG(INFO) << "std::uppercase ";

    double i = 1.23e100;
    LOG(INFO) << i;
    LOG(INFO) << std::uppercase << i;

    int j = 10;
    LOG(INFO) << std::hex << std::nouppercase << j;
    LOG(INFO) << std::hex << std::uppercase << j;

    return 0;
}
