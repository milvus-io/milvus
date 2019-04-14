 //
 // This file is part of Easylogging++ samples 
 //
 // Demonstration on possible usage of pre-rollout handler
 // 
 // Revision: 1.1
 // @author mkhan3189
 //

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

static unsigned int idx;

void rolloutHandler(const char* filename, std::size_t size) {
   // SHOULD NOT LOG ANYTHING HERE BECAUSE LOG FILE IS CLOSED!
   std::cout << "************** Rolling out [" << filename << "] because it reached [" << size << " bytes]" << std::endl;

   // BACK IT UP
   std::stringstream ss;
   ss << "mv " << filename << " bin/log-backup-" << ++idx;
   system(ss.str().c_str());
}

int main(int, char**) {
   idx = 0;
   el::Loggers::addFlag(el::LoggingFlag::StrictLogFileSizeCheck);
   el::Loggers::reconfigureAllLoggers(el::ConfigurationType::Filename, "/tmp/logs/max-size.log");
   el::Loggers::reconfigureAllLoggers(el::ConfigurationType::MaxLogFileSize, "100");
   el::Helpers::installPreRollOutCallback(rolloutHandler);

   for (int i = 0; i < 100; ++i)
       LOG(INFO) << "Test";
       
   el::Helpers::uninstallPreRollOutCallback();
   return 0;
}
