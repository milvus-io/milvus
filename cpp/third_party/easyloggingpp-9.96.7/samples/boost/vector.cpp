#include <boost/container/vector.hpp>
#include <boost/container/stable_vector.hpp>

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

int main(void) {
   boost::container::vector<int> v;
   v.push_back(2);
   LOG(INFO) << v;

   boost::container::stable_vector<int> sv;
   sv.push_back(3);
   LOG(INFO) << sv;
}
