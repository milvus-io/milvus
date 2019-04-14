 //
 // This file is part of Easylogging++ samples
 // Very basic sample
 //
 // Revision 1.2
 // @author mkhan3189
 //

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

int main(void) {
    
    LOG(INFO) << "My first ultimate log message";

    LOG(INFO) << "This" << "is" << "log" << "without" << "spaces";
    el::Loggers::addFlag(el::LoggingFlag::AutoSpacing);
    LOG(INFO) << "This" << "is" << "log" << "with" << "spaces";
    return 0;
}
