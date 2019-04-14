#include <boost/container/string.hpp>

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

int main(void) {
   boost::container::string s = "This is boost::container::string";
   LOG(INFO) << s;
}
