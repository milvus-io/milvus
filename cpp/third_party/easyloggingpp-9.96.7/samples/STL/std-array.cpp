 //
 // This file is part of Easylogging++ samples
 // Demonstration of STL array (std::array) logging, this requires ELPP_LOG_STD_ARRAY macro (recommended to define it in Makefile)
 //
 // Revision 1.1
 // @author mkhan3189
 //

#define ELPP_LOG_STD_ARRAY
#include "easylogging++.h"
#include <array>

INITIALIZE_EASYLOGGINGPP

int main (void) {

  std::array<int, 5> arr;
  arr[0] = 1;
  arr[1] = 2;
  arr[2] = 3;
  arr[3] = 4;
  arr[4] = 5;

  LOG(INFO) << arr;

  return 0;
}
