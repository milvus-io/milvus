 //
 // This file is part of Easylogging++ samples
 //
 // Helpers sample - this sample contains methods with explanation in comments on how to use them
 //
 // Revision 1.2
 // @author mkhan3189
 //

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

void configureFromArg() {
    // Configures globally using "--logging" param value
    // example: ./prog --logging-conf=/tmp/myglobal.conf
    el::Loggers::configureFromArg("--logging-conf");
}

void flush() {
    // Flush all levels of default logger
    el::Loggers::getLogger("default")->flush();
    
    // Flush all loggers all levels
    el::Loggers::flushAll();
}

int main(int argc, char** argv) {
    START_EASYLOGGINGPP(argc, argv);

    configureFromArg();
    
    flush();
    return 0;
}
