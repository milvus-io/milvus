#include <iostream>
#include "dynamic_bitset_ext.hpp"

namespace {
struct PtrWrapper {
    explicit PtrWrapper(char*& ptr) : ptr_(ptr) {}
    char*& ptr_;
};

struct ConstPtrWrapper {
    explicit ConstPtrWrapper(const char*& ptr) : ptr_(ptr) {}
    const char*& ptr_;
};

using Block = unsigned long;
using Allocator = std::allocator<Block>;

}    // namespace


namespace boost {
// a language lawyer's way to steal original pointer from boost::dynamic_bitset
// salute to http://www.gotw.ca/gotw/076.htm
template<>
void
from_block_range<PtrWrapper, Block, Allocator>(PtrWrapper result,
                                              PtrWrapper resultB,
                                              dynamic_bitset<>& bitset) {
    (void)resultB;
    result.ptr_ = reinterpret_cast<char*>(bitset.m_bits.data());
}

template<>
void
to_block_range<Block, Allocator, ConstPtrWrapper>(const dynamic_bitset<>& bitset,
                                                  ConstPtrWrapper result) {
    result.ptr_ = reinterpret_cast<const char*>(bitset.m_bits.data());
}
}    // namespace boost


namespace boost_ext {

char*
get_data(boost::dynamic_bitset<>& bitset) {
    char* ptr = nullptr;
    PtrWrapper wrapper{ptr};
    boost::from_block_range(wrapper, wrapper, bitset);
    assert(ptr);
    return ptr;
}

const char*
get_data(const boost::dynamic_bitset<>& bitset) {
    const char* ptr = nullptr;
    ConstPtrWrapper wrapper{ptr};
    boost::to_block_range(bitset, wrapper);
    assert(ptr);
    return ptr;
}


}    // namespace boost_ext
