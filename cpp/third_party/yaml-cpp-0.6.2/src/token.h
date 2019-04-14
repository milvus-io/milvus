#ifndef TOKEN_H_62B23520_7C8E_11DE_8A39_0800200C9A66
#define TOKEN_H_62B23520_7C8E_11DE_8A39_0800200C9A66

#if defined(_MSC_VER) ||                                            \
    (defined(__GNUC__) && (__GNUC__ == 3 && __GNUC_MINOR__ >= 4) || \
     (__GNUC__ >= 4))  // GCC supports "pragma once" correctly since 3.4
#pragma once
#endif

#include "yaml-cpp/mark.h"
#include <iostream>
#include <string>
#include <vector>

namespace YAML {
const std::string TokenNames[] = {
    "DIRECTIVE", "DOC_START", "DOC_END", "BLOCK_SEQ_START", "BLOCK_MAP_START",
    "BLOCK_SEQ_END", "BLOCK_MAP_END", "BLOCK_ENTRY", "FLOW_SEQ_START",
    "FLOW_MAP_START", "FLOW_SEQ_END", "FLOW_MAP_END", "FLOW_MAP_COMPACT",
    "FLOW_ENTRY", "KEY", "VALUE", "ANCHOR", "ALIAS", "TAG", "SCALAR"};

struct Token {
  // enums
  enum STATUS { VALID, INVALID, UNVERIFIED };
  enum TYPE {
    DIRECTIVE,
    DOC_START,
    DOC_END,
    BLOCK_SEQ_START,
    BLOCK_MAP_START,
    BLOCK_SEQ_END,
    BLOCK_MAP_END,
    BLOCK_ENTRY,
    FLOW_SEQ_START,
    FLOW_MAP_START,
    FLOW_SEQ_END,
    FLOW_MAP_END,
    FLOW_MAP_COMPACT,
    FLOW_ENTRY,
    KEY,
    VALUE,
    ANCHOR,
    ALIAS,
    TAG,
    PLAIN_SCALAR,
    NON_PLAIN_SCALAR
  };

  // data
  Token(TYPE type_, const Mark& mark_)
      : status(VALID), type(type_), mark(mark_), data(0) {}

  friend std::ostream& operator<<(std::ostream& out, const Token& token) {
    out << TokenNames[token.type] << std::string(": ") << token.value;
    for (std::size_t i = 0; i < token.params.size(); i++)
      out << std::string(" ") << token.params[i];
    return out;
  }

  STATUS status;
  TYPE type;
  Mark mark;
  std::string value;
  std::vector<std::string> params;
  int data;
};
}

#endif  // TOKEN_H_62B23520_7C8E_11DE_8A39_0800200C9A66
