#include <boost/container/list.hpp>

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

int main(void) {
   boost::container::list<int> l;
   l.insert(l.cbegin(), 3);
   LOG(INFO) << l;
}
