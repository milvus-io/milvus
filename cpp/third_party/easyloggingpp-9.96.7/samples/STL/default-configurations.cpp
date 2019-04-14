 //
 // This file is part of Easylogging++ samples
 //
 // Demonstrates setting default configurations for existing and future loggers
 //
 // Revision 1.1
 // @author mkhan3189
 //

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

using namespace el;

int main(void) {
    
    Configurations c;
    c.setGlobally(ConfigurationType::Format, "[%logger] %level: %msg");
    c.setGlobally(ConfigurationType::Filename, "/tmp/logs/custom.log");
    // Set default configuration for any future logger - existing logger will not use this configuration unless
    // either true is passed in second argument or set explicitly using Loggers::reconfigureAllLoggers(c);
    Loggers::setDefaultConfigurations(c);
    LOG(INFO) << "Set default configuration but existing loggers not updated yet"; // Logging using trivial logger
    Loggers::getLogger("testDefaultConf");
    CLOG(INFO, "testDefaultConf") << "Logging using new logger 1"; // You can also use CINFO << "..."
    // Now setting default and also resetting existing loggers
    Loggers::setDefaultConfigurations(c, true);
    LOG(INFO) << "Existing loggers updated as well";
    Loggers::getLogger("testDefaultConf2");
    CLOG(INFO, "testDefaultConf2") << "Logging using new logger 2";

    return 0;
}
