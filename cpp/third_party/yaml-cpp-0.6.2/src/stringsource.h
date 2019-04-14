#ifndef STRINGSOURCE_H_62B23520_7C8E_11DE_8A39_0800200C9A66
#define STRINGSOURCE_H_62B23520_7C8E_11DE_8A39_0800200C9A66

#if defined(_MSC_VER) ||                                            \
    (defined(__GNUC__) && (__GNUC__ == 3 && __GNUC_MINOR__ >= 4) || \
     (__GNUC__ >= 4))  // GCC supports "pragma once" correctly since 3.4
#pragma once
#endif

#include <cstddef>

namespace YAML {
class StringCharSource {
 public:
  StringCharSource(const char* str, std::size_t size)
      : m_str(str), m_size(size), m_offset(0) {}

  operator bool() const { return m_offset < m_size; }
  char operator[](std::size_t i) const { return m_str[m_offset + i]; }
  bool operator!() const { return !static_cast<bool>(*this); }

  const StringCharSource operator+(int i) const {
    StringCharSource source(*this);
    if (static_cast<int>(source.m_offset) + i >= 0)
      source.m_offset += i;
    else
      source.m_offset = 0;
    return source;
  }

  StringCharSource& operator++() {
    ++m_offset;
    return *this;
  }

  StringCharSource& operator+=(std::size_t offset) {
    m_offset += offset;
    return *this;
  }

 private:
  const char* m_str;
  std::size_t m_size;
  std::size_t m_offset;
};
}

#endif  // STRINGSOURCE_H_62B23520_7C8E_11DE_8A39_0800200C9A66
