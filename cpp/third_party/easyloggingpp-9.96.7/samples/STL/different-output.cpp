//
//  different-output.cpp
//  v1.0
//
//  Multiple loggers to have different output for file and console
//

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

#define MY_CUSTOM_LOGGER(LEVEL) CLOG(LEVEL, "default", "fileLogger")

int main() {
    
    el::Configurations fileConf("../file.conf");
    el::Configurations consoleConf("../console.conf");
    
    
    el::Loggers::addFlag(el::LoggingFlag::MultiLoggerSupport); // Enables support for multiple loggers
    el::Logger* fileLogger = el::Loggers::getLogger("fileLogger"); // Register new logger
    
    el::Loggers::reconfigureLogger("default", consoleConf);
    el::Loggers::reconfigureLogger(fileLogger, fileConf);
    
    
    
    MY_CUSTOM_LOGGER(INFO) << "This is how we do it.";
    
    return 0;
}
