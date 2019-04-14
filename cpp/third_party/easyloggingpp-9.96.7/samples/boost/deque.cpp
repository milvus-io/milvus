#include <boost/container/deque.hpp>

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

int main(void) {
   boost::container::deque<int> d(3, 100);
   d.at(1) = 200;
   d.at(2) = 20;
   LOG(INFO) << d;
}
