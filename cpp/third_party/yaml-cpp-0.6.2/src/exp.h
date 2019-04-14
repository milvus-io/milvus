#ifndef EXP_H_62B23520_7C8E_11DE_8A39_0800200C9A66
#define EXP_H_62B23520_7C8E_11DE_8A39_0800200C9A66

#if defined(_MSC_VER) ||                                            \
    (defined(__GNUC__) && (__GNUC__ == 3 && __GNUC_MINOR__ >= 4) || \
     (__GNUC__ >= 4))  // GCC supports "pragma once" correctly since 3.4
#pragma once
#endif

#include <ios>
#include <string>

#include "regex_yaml.h"
#include "stream.h"

namespace YAML {
////////////////////////////////////////////////////////////////////////////////
// Here we store a bunch of expressions for matching different parts of the
// file.

namespace Exp {
// misc
inline const RegEx& Empty() {
  static const RegEx e;
  return e;
}
inline const RegEx& Space() {
  static const RegEx e = RegEx(' ');
  return e;
}
inline const RegEx& Tab() {
  static const RegEx e = RegEx('\t');
  return e;
}
inline const RegEx& Blank() {
  static const RegEx e = Space() || Tab();
  return e;
}
inline const RegEx& Break() {
  static const RegEx e = RegEx('\n') || RegEx("\r\n");
  return e;
}
inline const RegEx& BlankOrBreak() {
  static const RegEx e = Blank() || Break();
  return e;
}
inline const RegEx& Digit() {
  static const RegEx e = RegEx('0', '9');
  return e;
}
inline const RegEx& Alpha() {
  static const RegEx e = RegEx('a', 'z') || RegEx('A', 'Z');
  return e;
}
inline const RegEx& AlphaNumeric() {
  static const RegEx e = Alpha() || Digit();
  return e;
}
inline const RegEx& Word() {
  static const RegEx e = AlphaNumeric() || RegEx('-');
  return e;
}
inline const RegEx& Hex() {
  static const RegEx e = Digit() || RegEx('A', 'F') || RegEx('a', 'f');
  return e;
}
// Valid Unicode code points that are not part of c-printable (YAML 1.2, sec.
// 5.1)
inline const RegEx& NotPrintable() {
  static const RegEx e =
      RegEx(0) ||
      RegEx("\x01\x02\x03\x04\x05\x06\x07\x08\x0B\x0C\x7F", REGEX_OR) ||
      RegEx(0x0E, 0x1F) ||
      (RegEx('\xC2') + (RegEx('\x80', '\x84') || RegEx('\x86', '\x9F')));
  return e;
}
inline const RegEx& Utf8_ByteOrderMark() {
  static const RegEx e = RegEx("\xEF\xBB\xBF");
  return e;
}

// actual tags

inline const RegEx& DocStart() {
  static const RegEx e = RegEx("---") + (BlankOrBreak() || RegEx());
  return e;
}
inline const RegEx& DocEnd() {
  static const RegEx e = RegEx("...") + (BlankOrBreak() || RegEx());
  return e;
}
inline const RegEx& DocIndicator() {
  static const RegEx e = DocStart() || DocEnd();
  return e;
}
inline const RegEx& BlockEntry() {
  static const RegEx e = RegEx('-') + (BlankOrBreak() || RegEx());
  return e;
}
inline const RegEx& Key() {
  static const RegEx e = RegEx('?') + BlankOrBreak();
  return e;
}
inline const RegEx& KeyInFlow() {
  static const RegEx e = RegEx('?') + BlankOrBreak();
  return e;
}
inline const RegEx& Value() {
  static const RegEx e = RegEx(':') + (BlankOrBreak() || RegEx());
  return e;
}
inline const RegEx& ValueInFlow() {
  static const RegEx e = RegEx(':') + (BlankOrBreak() || RegEx(",}", REGEX_OR));
  return e;
}
inline const RegEx& ValueInJSONFlow() {
  static const RegEx e = RegEx(':');
  return e;
}
inline const RegEx Comment() {
  static const RegEx e = RegEx('#');
  return e;
}
inline const RegEx& Anchor() {
  static const RegEx e = !(RegEx("[]{},", REGEX_OR) || BlankOrBreak());
  return e;
}
inline const RegEx& AnchorEnd() {
  static const RegEx e = RegEx("?:,]}%@`", REGEX_OR) || BlankOrBreak();
  return e;
}
inline const RegEx& URI() {
  static const RegEx e = Word() || RegEx("#;/?:@&=+$,_.!~*'()[]", REGEX_OR) ||
                         (RegEx('%') + Hex() + Hex());
  return e;
}
inline const RegEx& Tag() {
  static const RegEx e = Word() || RegEx("#;/?:@&=+$_.~*'()", REGEX_OR) ||
                         (RegEx('%') + Hex() + Hex());
  return e;
}

// Plain scalar rules:
// . Cannot start with a blank.
// . Can never start with any of , [ ] { } # & * ! | > \' \" % @ `
// . In the block context - ? : must be not be followed with a space.
// . In the flow context ? is illegal and : and - must not be followed with a
// space.
inline const RegEx& PlainScalar() {
  static const RegEx e =
      !(BlankOrBreak() || RegEx(",[]{}#&*!|>\'\"%@`", REGEX_OR) ||
        (RegEx("-?:", REGEX_OR) + (BlankOrBreak() || RegEx())));
  return e;
}
inline const RegEx& PlainScalarInFlow() {
  static const RegEx e =
      !(BlankOrBreak() || RegEx("?,[]{}#&*!|>\'\"%@`", REGEX_OR) ||
        (RegEx("-:", REGEX_OR) + Blank()));
  return e;
}
inline const RegEx& EndScalar() {
  static const RegEx e = RegEx(':') + (BlankOrBreak() || RegEx());
  return e;
}
inline const RegEx& EndScalarInFlow() {
  static const RegEx e =
      (RegEx(':') + (BlankOrBreak() || RegEx() || RegEx(",]}", REGEX_OR))) ||
      RegEx(",?[]{}", REGEX_OR);
  return e;
}

inline const RegEx& ScanScalarEndInFlow() {
  static const RegEx e = (EndScalarInFlow() || (BlankOrBreak() + Comment()));
  return e;
}

inline const RegEx& ScanScalarEnd() {
  static const RegEx e = EndScalar() || (BlankOrBreak() + Comment());
  return e;
}
inline const RegEx& EscSingleQuote() {
  static const RegEx e = RegEx("\'\'");
  return e;
}
inline const RegEx& EscBreak() {
  static const RegEx e = RegEx('\\') + Break();
  return e;
}

inline const RegEx& ChompIndicator() {
  static const RegEx e = RegEx("+-", REGEX_OR);
  return e;
}
inline const RegEx& Chomp() {
  static const RegEx e = (ChompIndicator() + Digit()) ||
                         (Digit() + ChompIndicator()) || ChompIndicator() ||
                         Digit();
  return e;
}

// and some functions
std::string Escape(Stream& in);
}  // namespace Exp

namespace Keys {
const char Directive = '%';
const char FlowSeqStart = '[';
const char FlowSeqEnd = ']';
const char FlowMapStart = '{';
const char FlowMapEnd = '}';
const char FlowEntry = ',';
const char Alias = '*';
const char Anchor = '&';
const char Tag = '!';
const char LiteralScalar = '|';
const char FoldedScalar = '>';
const char VerbatimTagStart = '<';
const char VerbatimTagEnd = '>';
}  // namespace Keys
}  // namespace YAML

#endif  // EXP_H_62B23520_7C8E_11DE_8A39_0800200C9A66
