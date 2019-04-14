#ifndef REGEX_H_62B23520_7C8E_11DE_8A39_0800200C9A66
#define REGEX_H_62B23520_7C8E_11DE_8A39_0800200C9A66

#if defined(_MSC_VER) ||                                            \
    (defined(__GNUC__) && (__GNUC__ == 3 && __GNUC_MINOR__ >= 4) || \
     (__GNUC__ >= 4))  // GCC supports "pragma once" correctly since 3.4
#pragma once
#endif

#include <string>
#include <vector>

#include "yaml-cpp/dll.h"

namespace YAML {
class Stream;

enum REGEX_OP {
  REGEX_EMPTY,
  REGEX_MATCH,
  REGEX_RANGE,
  REGEX_OR,
  REGEX_AND,
  REGEX_NOT,
  REGEX_SEQ
};

// simplified regular expressions
// . Only straightforward matches (no repeated characters)
// . Only matches from start of string
class YAML_CPP_API RegEx {
 public:
  RegEx();
  RegEx(char ch);
  RegEx(char a, char z);
  RegEx(const std::string& str, REGEX_OP op = REGEX_SEQ);
  ~RegEx() {}

  friend YAML_CPP_API RegEx operator!(const RegEx& ex);
  friend YAML_CPP_API RegEx operator||(const RegEx& ex1, const RegEx& ex2);
  friend YAML_CPP_API RegEx operator&&(const RegEx& ex1, const RegEx& ex2);
  friend YAML_CPP_API RegEx operator+(const RegEx& ex1, const RegEx& ex2);

  bool Matches(char ch) const;
  bool Matches(const std::string& str) const;
  bool Matches(const Stream& in) const;
  template <typename Source>
  bool Matches(const Source& source) const;

  int Match(const std::string& str) const;
  int Match(const Stream& in) const;
  template <typename Source>
  int Match(const Source& source) const;

 private:
  RegEx(REGEX_OP op);

  template <typename Source>
  bool IsValidSource(const Source& source) const;
  template <typename Source>
  int MatchUnchecked(const Source& source) const;

  template <typename Source>
  int MatchOpEmpty(const Source& source) const;
  template <typename Source>
  int MatchOpMatch(const Source& source) const;
  template <typename Source>
  int MatchOpRange(const Source& source) const;
  template <typename Source>
  int MatchOpOr(const Source& source) const;
  template <typename Source>
  int MatchOpAnd(const Source& source) const;
  template <typename Source>
  int MatchOpNot(const Source& source) const;
  template <typename Source>
  int MatchOpSeq(const Source& source) const;

 private:
  REGEX_OP m_op;
  char m_a, m_z;
  std::vector<RegEx> m_params;
};
}

#include "regeximpl.h"

#endif  // REGEX_H_62B23520_7C8E_11DE_8A39_0800200C9A66
