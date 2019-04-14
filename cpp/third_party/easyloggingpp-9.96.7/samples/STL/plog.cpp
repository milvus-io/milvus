 //
 // This file is part of Easylogging++ samples
 //
 // Log using PLOG and family. PLOG is same as perror() with c++-styled stream
 //
 // Revision 1.1
 // @author mkhan3189
 //

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

int main(void) {
    el::Loggers::addFlag(el::LoggingFlag::DisableApplicationAbortOnFatalLog);
    std::fstream f("a file that does not exist", std::ifstream::in);    
    PLOG(INFO) << "A message with plog";
    PLOG_IF(true, INFO) << "A message with plog";
    PLOG_IF(false, INFO) << "A message with plog";
    PCHECK(true) << "This is good";
    LOG(INFO) << "This is normal info log after plog";
    DPCHECK(true) << "Wow";
    DPCHECK(false) << "Wow failed";
    return 0;
}
