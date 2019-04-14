#ifndef REGEXIMPL_H_62B23520_7C8E_11DE_8A39_0800200C9A66
#define REGEXIMPL_H_62B23520_7C8E_11DE_8A39_0800200C9A66

#if defined(_MSC_VER) ||                                            \
    (defined(__GNUC__) && (__GNUC__ == 3 && __GNUC_MINOR__ >= 4) || \
     (__GNUC__ >= 4))  // GCC supports "pragma once" correctly since 3.4
#pragma once
#endif

#include "stream.h"
#include "stringsource.h"
#include "streamcharsource.h"

namespace YAML {
// query matches
inline bool RegEx::Matches(char ch) const {
  std::string str;
  str += ch;
  return Matches(str);
}

inline bool RegEx::Matches(const std::string& str) const {
  return Match(str) >= 0;
}

inline bool RegEx::Matches(const Stream& in) const { return Match(in) >= 0; }

template <typename Source>
inline bool RegEx::Matches(const Source& source) const {
  return Match(source) >= 0;
}

// Match
// . Matches the given string against this regular expression.
// . Returns the number of characters matched.
// . Returns -1 if no characters were matched (the reason for
//   not returning zero is that we may have an empty regex
//   which is ALWAYS successful at matching zero characters).
// . REMEMBER that we only match from the start of the buffer!
inline int RegEx::Match(const std::string& str) const {
  StringCharSource source(str.c_str(), str.size());
  return Match(source);
}

inline int RegEx::Match(const Stream& in) const {
  StreamCharSource source(in);
  return Match(source);
}

template <typename Source>
inline bool RegEx::IsValidSource(const Source& source) const {
  return source;
}

template <>
inline bool RegEx::IsValidSource<StringCharSource>(
    const StringCharSource& source) const {
  switch (m_op) {
    case REGEX_MATCH:
    case REGEX_RANGE:
      return source;
    default:
      return true;
  }
}

template <typename Source>
inline int RegEx::Match(const Source& source) const {
  return IsValidSource(source) ? MatchUnchecked(source) : -1;
}

template <typename Source>
inline int RegEx::MatchUnchecked(const Source& source) const {
  switch (m_op) {
    case REGEX_EMPTY:
      return MatchOpEmpty(source);
    case REGEX_MATCH:
      return MatchOpMatch(source);
    case REGEX_RANGE:
      return MatchOpRange(source);
    case REGEX_OR:
      return MatchOpOr(source);
    case REGEX_AND:
      return MatchOpAnd(source);
    case REGEX_NOT:
      return MatchOpNot(source);
    case REGEX_SEQ:
      return MatchOpSeq(source);
  }

  return -1;
}

//////////////////////////////////////////////////////////////////////////////
// Operators
// Note: the convention MatchOp*<Source> is that we can assume
// IsSourceValid(source).
//       So we do all our checks *before* we call these functions

// EmptyOperator
template <typename Source>
inline int RegEx::MatchOpEmpty(const Source& source) const {
  return source[0] == Stream::eof() ? 0 : -1;
}

template <>
inline int RegEx::MatchOpEmpty<StringCharSource>(
    const StringCharSource& source) const {
  return !source
             ? 0
             : -1;  // the empty regex only is successful on the empty string
}

// MatchOperator
template <typename Source>
inline int RegEx::MatchOpMatch(const Source& source) const {
  if (source[0] != m_a)
    return -1;
  return 1;
}

// RangeOperator
template <typename Source>
inline int RegEx::MatchOpRange(const Source& source) const {
  if (m_a > source[0] || m_z < source[0])
    return -1;
  return 1;
}

// OrOperator
template <typename Source>
inline int RegEx::MatchOpOr(const Source& source) const {
  for (std::size_t i = 0; i < m_params.size(); i++) {
    int n = m_params[i].MatchUnchecked(source);
    if (n >= 0)
      return n;
  }
  return -1;
}

// AndOperator
// Note: 'AND' is a little funny, since we may be required to match things
//       of different lengths. If we find a match, we return the length of
//       the FIRST entry on the list.
template <typename Source>
inline int RegEx::MatchOpAnd(const Source& source) const {
  int first = -1;
  for (std::size_t i = 0; i < m_params.size(); i++) {
    int n = m_params[i].MatchUnchecked(source);
    if (n == -1)
      return -1;
    if (i == 0)
      first = n;
  }
  return first;
}

// NotOperator
template <typename Source>
inline int RegEx::MatchOpNot(const Source& source) const {
  if (m_params.empty())
    return -1;
  if (m_params[0].MatchUnchecked(source) >= 0)
    return -1;
  return 1;
}

// SeqOperator
template <typename Source>
inline int RegEx::MatchOpSeq(const Source& source) const {
  int offset = 0;
  for (std::size_t i = 0; i < m_params.size(); i++) {
    int n = m_params[i].Match(source + offset);  // note Match, not
                                                 // MatchUnchecked because we
                                                 // need to check validity after
                                                 // the offset
    if (n == -1)
      return -1;
    offset += n;
  }

  return offset;
}
}

#endif  // REGEXIMPL_H_62B23520_7C8E_11DE_8A39_0800200C9A66
