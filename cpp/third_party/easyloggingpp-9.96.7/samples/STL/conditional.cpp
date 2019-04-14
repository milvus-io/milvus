 //
 // This file is part of EasyLogging++ samples
 //
 // Conditional logging using LOG_IF, you can use CLOG_IF(condition, loggerID) macro to use your own logger if you 
 // don't want to use default logger
 //
 // Revision 1.1
 // @author mkhan3189
 //

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

int main(void) {

  LOG_IF(1 == 1, INFO) << "1 is equal to 1";

  LOG_IF(1 > 2, INFO) << "1 is greater than 2";

  LOG_IF(1 == 2, DEBUG) << "1 is equal to 2";

  return 0;
}
