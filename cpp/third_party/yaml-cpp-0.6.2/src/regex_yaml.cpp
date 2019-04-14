#include "regex_yaml.h"

namespace YAML {
// constructors
RegEx::RegEx() : m_op(REGEX_EMPTY) {}

RegEx::RegEx(REGEX_OP op) : m_op(op) {}

RegEx::RegEx(char ch) : m_op(REGEX_MATCH), m_a(ch) {}

RegEx::RegEx(char a, char z) : m_op(REGEX_RANGE), m_a(a), m_z(z) {}

RegEx::RegEx(const std::string& str, REGEX_OP op) : m_op(op) {
  for (std::size_t i = 0; i < str.size(); i++)
    m_params.push_back(RegEx(str[i]));
}

// combination constructors
RegEx operator!(const RegEx& ex) {
  RegEx ret(REGEX_NOT);
  ret.m_params.push_back(ex);
  return ret;
}

RegEx operator||(const RegEx& ex1, const RegEx& ex2) {
  RegEx ret(REGEX_OR);
  ret.m_params.push_back(ex1);
  ret.m_params.push_back(ex2);
  return ret;
}

RegEx operator&&(const RegEx& ex1, const RegEx& ex2) {
  RegEx ret(REGEX_AND);
  ret.m_params.push_back(ex1);
  ret.m_params.push_back(ex2);
  return ret;
}

RegEx operator+(const RegEx& ex1, const RegEx& ex2) {
  RegEx ret(REGEX_SEQ);
  ret.m_params.push_back(ex1);
  ret.m_params.push_back(ex2);
  return ret;
}
}
