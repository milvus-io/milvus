#include <boost/container/map.hpp>
#include <boost/container/flat_map.hpp>

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

int main(void) {
   boost::container::map<int, float> m;
   m[0] =  1.0f;
   m[5] =  3.3f;
   LOG(INFO) << m;

   boost::container::flat_map<int, float> fm;
   fm[1] = 2.5f;
   fm[2] = 5.0f;
   LOG(INFO) << fm;
}
