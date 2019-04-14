 //
 // This file is part of Easylogging++ samples
 // Very basic sample - log using multiple loggers
 //
 // Revision 1.0
 // @author mkhan3189
 //

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

int main(void) {

    el::Loggers::addFlag(el::LoggingFlag::MultiLoggerSupport); // Enables support for multiple loggers

    el::Loggers::getLogger("network"); // Register 'network' logger
    
    CLOG(INFO, "default", "network") << "My first log message that writes with network and default loggers";


    // Another way of doing this may be
    #define _LOGGER "default", "network"
    CLOG(INFO, _LOGGER) << "This is done by _LOGGER";

    // More practical way of doing this
    #define NETWORK_LOG(LEVEL) CLOG(LEVEL, _LOGGER)
    NETWORK_LOG(INFO) << "This is achieved by NETWORK_LOG macro";

    return 0;
}
