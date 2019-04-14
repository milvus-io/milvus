 // This file is part of Easylogging++ samples
 //
 // Sample to demonstrate using occasionals and other hit counts based logging
 //
 // Revision 1.2
 // @author mkhan3189
 //

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

int main(int argc, char** argv) {
  START_EASYLOGGINGPP(argc, argv);

  for (int i = 1;i < 1000; ++i) {
     LOG_EVERY_N(20, INFO) << "LOG_EVERY_N i = " << i;
     LOG_EVERY_N(100, INFO) << "LOG_EVERY_N Current position is " << ELPP_COUNTER_POS;
  }
  for (int i = 1;i <= 10; ++i) {
     LOG_AFTER_N(6, INFO) << "LOG_AFTER_N i = " << i;
  }
  for (int i = 1;i < 100; ++i) {
     LOG_N_TIMES(50, INFO) << "LOG_N_TIMES i = " << i;
  }
  return 0;
}
