 //
 // This file is part of Easylogging++ samples
 //
 // Very basic sample to configure using el::Configuration and configuration file
 //
 // Revision 1.1
 // @author mkhan3189
 //

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

int main(int argc, char** argv) {
    START_EASYLOGGINGPP(argc, argv);
    LOG(INFO) << "Info log using 'default' logger before using configuration";
   
    el::Configurations confFromFile("../default-logger.conf");
 
    el::Loggers::reconfigureAllLoggers(confFromFile); 
     
    LOG(INFO) << "Info log after manually configuring 'default' logger";
    el::Loggers::getLogger("default")->reconfigure();
    LOG(ERROR) << "Error log";
    LOG(WARNING) << "WARNING! log";
    VLOG(1) << "Verbose log 1";
    VLOG(2) << "Verbose log 2";
    return 0;
}
