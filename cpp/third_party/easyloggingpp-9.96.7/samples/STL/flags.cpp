 //
 // This file is part of Easylogging++ samples
 // Demonstration of STL flags, e.g, std::boolalpha
 //
 // Revision 1.1
 // @author mkhan3189
 //

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

int main(void) {
    
    bool v = true;
    LOG(INFO) << std::boolalpha << v;
    LOG(INFO) << std::noboolalpha << v;

    return 0;
}
