#include <boost/dynamic_bitset.hpp>

namespace boost_ext {
const char* get_data(const boost::dynamic_bitset<>& bitset);
char* get_data(boost::dynamic_bitset<>& bitset);
}    // namespace boost_ext
