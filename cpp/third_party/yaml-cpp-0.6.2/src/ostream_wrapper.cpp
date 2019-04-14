#include "yaml-cpp/ostream_wrapper.h"

#include <algorithm>
#include <cstring>
#include <iostream>

namespace YAML {
ostream_wrapper::ostream_wrapper()
    : m_buffer(1, '\0'),
      m_pStream(0),
      m_pos(0),
      m_row(0),
      m_col(0),
      m_comment(false) {}

ostream_wrapper::ostream_wrapper(std::ostream& stream)
    : m_pStream(&stream), m_pos(0), m_row(0), m_col(0), m_comment(false) {}

ostream_wrapper::~ostream_wrapper() {}

void ostream_wrapper::write(const std::string& str) {
  if (m_pStream) {
    m_pStream->write(str.c_str(), str.size());
  } else {
    m_buffer.resize(std::max(m_buffer.size(), m_pos + str.size() + 1));
    std::copy(str.begin(), str.end(), m_buffer.begin() + m_pos);
  }

  for (std::size_t i = 0; i < str.size(); i++) {
    update_pos(str[i]);
  }
}

void ostream_wrapper::write(const char* str, std::size_t size) {
  if (m_pStream) {
    m_pStream->write(str, size);
  } else {
    m_buffer.resize(std::max(m_buffer.size(), m_pos + size + 1));
    std::copy(str, str + size, m_buffer.begin() + m_pos);
  }

  for (std::size_t i = 0; i < size; i++) {
    update_pos(str[i]);
  }
}

void ostream_wrapper::update_pos(char ch) {
  m_pos++;
  m_col++;

  if (ch == '\n') {
    m_row++;
    m_col = 0;
    m_comment = false;
  }
}
}
