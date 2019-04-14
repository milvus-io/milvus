 //
 // This file is part of Easylogging++ samples
 //
 // Very basic sample to configure using global configuration (el::Loggers::configureFromGlobal)
 //
 // Revision 1.1
 // @author mkhan3189
 //

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

int main(void) {

    LOG(INFO) << "Info log before using global configuration";
    
    el::Loggers::configureFromGlobal("../global.conf"); 
    
    LOG(INFO) << "Info log AFTER using global configuration";
    LOG(ERROR) << "Error log AFTER using global configuration";

    return 0;
}
