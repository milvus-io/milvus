 //
 // This file is part of Easylogging++ samples
 // Default log file using '--default-log-file' arg
 //
 // Revision 1.0
 // @author mkhan3189
 //

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

int main(int argc, char** argv) {
    START_EASYLOGGINGPP(argc, argv);
 
    LOG(INFO) << "My log message - hopefully you have reconfigured log file by using"
        << " --default-log-file=blah.log and defined ELPP_NO_DEFAULT_LOG_FILE";

    return 0;
}
