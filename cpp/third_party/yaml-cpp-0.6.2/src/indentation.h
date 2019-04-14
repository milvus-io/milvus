#ifndef INDENTATION_H_62B23520_7C8E_11DE_8A39_0800200C9A66
#define INDENTATION_H_62B23520_7C8E_11DE_8A39_0800200C9A66

#if defined(_MSC_VER) ||                                            \
    (defined(__GNUC__) && (__GNUC__ == 3 && __GNUC_MINOR__ >= 4) || \
     (__GNUC__ >= 4))  // GCC supports "pragma once" correctly since 3.4
#pragma once
#endif

#include <iostream>
#include <cstddef>

#include "yaml-cpp/ostream_wrapper.h"

namespace YAML {
struct Indentation {
  Indentation(std::size_t n_) : n(n_) {}
  std::size_t n;
};

inline ostream_wrapper& operator<<(ostream_wrapper& out,
                                   const Indentation& indent) {
  for (std::size_t i = 0; i < indent.n; i++)
    out << ' ';
  return out;
}

struct IndentTo {
  IndentTo(std::size_t n_) : n(n_) {}
  std::size_t n;
};

inline ostream_wrapper& operator<<(ostream_wrapper& out,
                                   const IndentTo& indent) {
  while (out.col() < indent.n)
    out << ' ';
  return out;
}
}

#endif  // INDENTATION_H_62B23520_7C8E_11DE_8A39_0800200C9A66
