 //
 // This file is part of EasyLogging++ samples
 // Demonstration of verbose logging
 //
 // Revision 1.0
 // @author mkhan3189
 //

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

int main(int argc, char** argv) {
  START_EASYLOGGINGPP(argc, argv);
  LOG(INFO) << "This is demo for verbose logs";
  VLOG(1) << "This will be printed when program is started using argument --v=1";
  VLOG(2) << "This will be printed when program is started using argument --v=2";
  VLOG(1) << "This will be printed when program is started using argument --v=1";
  VLOG_IF(true, 1) << "Always verbose for level 1";

  VLOG_EVERY_N(1, 3) << "Verbose every N";

  VLOG(4) << "Command line arguments provided " << *el::Helpers::commandLineArgs();
  return 0;
}
