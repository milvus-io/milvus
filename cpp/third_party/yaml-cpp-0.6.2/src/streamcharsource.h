#ifndef STREAMCHARSOURCE_H_62B23520_7C8E_11DE_8A39_0800200C9A66
#define STREAMCHARSOURCE_H_62B23520_7C8E_11DE_8A39_0800200C9A66

#if defined(_MSC_VER) ||                                            \
    (defined(__GNUC__) && (__GNUC__ == 3 && __GNUC_MINOR__ >= 4) || \
     (__GNUC__ >= 4))  // GCC supports "pragma once" correctly since 3.4
#pragma once
#endif

#include "yaml-cpp/noncopyable.h"
#include <cstddef>

namespace YAML {
class StreamCharSource {
 public:
  StreamCharSource(const Stream& stream) : m_offset(0), m_stream(stream) {}
  StreamCharSource(const StreamCharSource& source)
      : m_offset(source.m_offset), m_stream(source.m_stream) {}
  ~StreamCharSource() {}

  operator bool() const;
  char operator[](std::size_t i) const { return m_stream.CharAt(m_offset + i); }
  bool operator!() const { return !static_cast<bool>(*this); }

  const StreamCharSource operator+(int i) const;

 private:
  std::size_t m_offset;
  const Stream& m_stream;

  StreamCharSource& operator=(const StreamCharSource&);  // non-assignable
};

inline StreamCharSource::operator bool() const {
  return m_stream.ReadAheadTo(m_offset);
}

inline const StreamCharSource StreamCharSource::operator+(int i) const {
  StreamCharSource source(*this);
  if (static_cast<int>(source.m_offset) + i >= 0)
    source.m_offset += i;
  else
    source.m_offset = 0;
  return source;
}
}

#endif  // STREAMCHARSOURCE_H_62B23520_7C8E_11DE_8A39_0800200C9A66
