#include <boost/container/set.hpp>
#include <boost/container/flat_set.hpp>

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

int main(void) {
   boost::container::set<int> s;
   s.insert(4);
   s.insert(5);
   LOG(INFO) << s;

   boost::container::flat_set<int> fs;
   fs.insert(1);
   fs.insert(2);
   LOG(INFO) << fs;
}
