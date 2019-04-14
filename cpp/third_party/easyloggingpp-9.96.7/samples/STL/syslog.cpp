 //
 // This file is part of Easylogging++ samples
 // Syslog sample
 //
 // Revision 1.0
 // @author mkhan3189
 //

#define ELPP_SYSLOG
#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

int main(void) {
   
    ELPP_INITIALIZE_SYSLOG("syslog_sample", LOG_PID | LOG_CONS | LOG_PERROR, LOG_USER);
 
    SYSLOG(INFO) << "My first easylogging++ syslog message";

    SYSLOG_IF(true, INFO) << "This is conditional info syslog";
    for (int i = 1; i <= 10; ++i)
        SYSLOG_EVERY_N(2, INFO) << "This is [" << i << "] iter of SYSLOG_EVERY_N";

    return 0;
}
