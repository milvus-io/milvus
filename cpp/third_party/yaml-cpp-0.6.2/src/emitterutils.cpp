#include <iomanip>
#include <sstream>

#include "emitterutils.h"
#include "exp.h"
#include "indentation.h"
#include "regex_yaml.h"
#include "regeximpl.h"
#include "stringsource.h"
#include "yaml-cpp/binary.h"  // IWYU pragma: keep
#include "yaml-cpp/ostream_wrapper.h"
#include "yaml-cpp/null.h"

namespace YAML {
namespace Utils {
namespace {
enum { REPLACEMENT_CHARACTER = 0xFFFD };

bool IsAnchorChar(int ch) {  // test for ns-anchor-char
  switch (ch) {
    case ',':
    case '[':
    case ']':
    case '{':
    case '}':  // c-flow-indicator
    case ' ':
    case '\t':    // s-white
    case 0xFEFF:  // c-byte-order-mark
    case 0xA:
    case 0xD:  // b-char
      return false;
    case 0x85:
      return true;
  }

  if (ch < 0x20) {
    return false;
  }

  if (ch < 0x7E) {
    return true;
  }

  if (ch < 0xA0) {
    return false;
  }
  if (ch >= 0xD800 && ch <= 0xDFFF) {
    return false;
  }
  if ((ch & 0xFFFE) == 0xFFFE) {
    return false;
  }
  if ((ch >= 0xFDD0) && (ch <= 0xFDEF)) {
    return false;
  }
  if (ch > 0x10FFFF) {
    return false;
  }

  return true;
}

int Utf8BytesIndicated(char ch) {
  int byteVal = static_cast<unsigned char>(ch);
  switch (byteVal >> 4) {
    case 0:
    case 1:
    case 2:
    case 3:
    case 4:
    case 5:
    case 6:
    case 7:
      return 1;
    case 12:
    case 13:
      return 2;
    case 14:
      return 3;
    case 15:
      return 4;
    default:
      return -1;
  }
}

bool IsTrailingByte(char ch) { return (ch & 0xC0) == 0x80; }

bool GetNextCodePointAndAdvance(int& codePoint,
                                std::string::const_iterator& first,
                                std::string::const_iterator last) {
  if (first == last)
    return false;

  int nBytes = Utf8BytesIndicated(*first);
  if (nBytes < 1) {
    // Bad lead byte
    ++first;
    codePoint = REPLACEMENT_CHARACTER;
    return true;
  }

  if (nBytes == 1) {
    codePoint = *first++;
    return true;
  }

  // Gather bits from trailing bytes
  codePoint = static_cast<unsigned char>(*first) & ~(0xFF << (7 - nBytes));
  ++first;
  --nBytes;
  for (; nBytes > 0; ++first, --nBytes) {
    if ((first == last) || !IsTrailingByte(*first)) {
      codePoint = REPLACEMENT_CHARACTER;
      break;
    }
    codePoint <<= 6;
    codePoint |= *first & 0x3F;
  }

  // Check for illegal code points
  if (codePoint > 0x10FFFF)
    codePoint = REPLACEMENT_CHARACTER;
  else if (codePoint >= 0xD800 && codePoint <= 0xDFFF)
    codePoint = REPLACEMENT_CHARACTER;
  else if ((codePoint & 0xFFFE) == 0xFFFE)
    codePoint = REPLACEMENT_CHARACTER;
  else if (codePoint >= 0xFDD0 && codePoint <= 0xFDEF)
    codePoint = REPLACEMENT_CHARACTER;
  return true;
}

void WriteCodePoint(ostream_wrapper& out, int codePoint) {
  if (codePoint < 0 || codePoint > 0x10FFFF) {
    codePoint = REPLACEMENT_CHARACTER;
  }
  if (codePoint < 0x7F) {
    out << static_cast<char>(codePoint);
  } else if (codePoint < 0x7FF) {
    out << static_cast<char>(0xC0 | (codePoint >> 6))
        << static_cast<char>(0x80 | (codePoint & 0x3F));
  } else if (codePoint < 0xFFFF) {
    out << static_cast<char>(0xE0 | (codePoint >> 12))
        << static_cast<char>(0x80 | ((codePoint >> 6) & 0x3F))
        << static_cast<char>(0x80 | (codePoint & 0x3F));
  } else {
    out << static_cast<char>(0xF0 | (codePoint >> 18))
        << static_cast<char>(0x80 | ((codePoint >> 12) & 0x3F))
        << static_cast<char>(0x80 | ((codePoint >> 6) & 0x3F))
        << static_cast<char>(0x80 | (codePoint & 0x3F));
  }
}

bool IsValidPlainScalar(const std::string& str, FlowType::value flowType,
                        bool allowOnlyAscii) {
  // check against null
  if (IsNullString(str)) {
    return false;
  }

  // check the start
  const RegEx& start = (flowType == FlowType::Flow ? Exp::PlainScalarInFlow()
                                                   : Exp::PlainScalar());
  if (!start.Matches(str)) {
    return false;
  }

  // and check the end for plain whitespace (which can't be faithfully kept in a
  // plain scalar)
  if (!str.empty() && *str.rbegin() == ' ') {
    return false;
  }

  // then check until something is disallowed
  static const RegEx& disallowed_flow =
      Exp::EndScalarInFlow() || (Exp::BlankOrBreak() + Exp::Comment()) ||
      Exp::NotPrintable() || Exp::Utf8_ByteOrderMark() || Exp::Break() ||
      Exp::Tab();
  static const RegEx& disallowed_block =
      Exp::EndScalar() || (Exp::BlankOrBreak() + Exp::Comment()) ||
      Exp::NotPrintable() || Exp::Utf8_ByteOrderMark() || Exp::Break() ||
      Exp::Tab();
  const RegEx& disallowed =
      flowType == FlowType::Flow ? disallowed_flow : disallowed_block;

  StringCharSource buffer(str.c_str(), str.size());
  while (buffer) {
    if (disallowed.Matches(buffer)) {
      return false;
    }
    if (allowOnlyAscii && (0x80 <= static_cast<unsigned char>(buffer[0]))) {
      return false;
    }
    ++buffer;
  }

  return true;
}

bool IsValidSingleQuotedScalar(const std::string& str, bool escapeNonAscii) {
  // TODO: check for non-printable characters?
  for (std::size_t i = 0; i < str.size(); i++) {
    if (escapeNonAscii && (0x80 <= static_cast<unsigned char>(str[i]))) {
      return false;
    }
    if (str[i] == '\n') {
      return false;
    }
  }
  return true;
}

bool IsValidLiteralScalar(const std::string& str, FlowType::value flowType,
                          bool escapeNonAscii) {
  if (flowType == FlowType::Flow) {
    return false;
  }

  // TODO: check for non-printable characters?
  for (std::size_t i = 0; i < str.size(); i++) {
    if (escapeNonAscii && (0x80 <= static_cast<unsigned char>(str[i]))) {
      return false;
    }
  }
  return true;
}

void WriteDoubleQuoteEscapeSequence(ostream_wrapper& out, int codePoint) {
  static const char hexDigits[] = "0123456789abcdef";

  out << "\\";
  int digits = 8;
  if (codePoint < 0xFF) {
    out << "x";
    digits = 2;
  } else if (codePoint < 0xFFFF) {
    out << "u";
    digits = 4;
  } else {
    out << "U";
    digits = 8;
  }

  // Write digits into the escape sequence
  for (; digits > 0; --digits)
    out << hexDigits[(codePoint >> (4 * (digits - 1))) & 0xF];
}

bool WriteAliasName(ostream_wrapper& out, const std::string& str) {
  int codePoint;
  for (std::string::const_iterator i = str.begin();
       GetNextCodePointAndAdvance(codePoint, i, str.end());) {
    if (!IsAnchorChar(codePoint)) {
      return false;
    }

    WriteCodePoint(out, codePoint);
  }
  return true;
}
}

StringFormat::value ComputeStringFormat(const std::string& str,
                                        EMITTER_MANIP strFormat,
                                        FlowType::value flowType,
                                        bool escapeNonAscii) {
  switch (strFormat) {
    case Auto:
      if (IsValidPlainScalar(str, flowType, escapeNonAscii)) {
        return StringFormat::Plain;
      }
      return StringFormat::DoubleQuoted;
    case SingleQuoted:
      if (IsValidSingleQuotedScalar(str, escapeNonAscii)) {
        return StringFormat::SingleQuoted;
      }
      return StringFormat::DoubleQuoted;
    case DoubleQuoted:
      return StringFormat::DoubleQuoted;
    case Literal:
      if (IsValidLiteralScalar(str, flowType, escapeNonAscii)) {
        return StringFormat::Literal;
      }
      return StringFormat::DoubleQuoted;
    default:
      break;
  }

  return StringFormat::DoubleQuoted;
}

bool WriteSingleQuotedString(ostream_wrapper& out, const std::string& str) {
  out << "'";
  int codePoint;
  for (std::string::const_iterator i = str.begin();
       GetNextCodePointAndAdvance(codePoint, i, str.end());) {
    if (codePoint == '\n') {
      return false;  // We can't handle a new line and the attendant indentation
                     // yet
    }

    if (codePoint == '\'') {
      out << "''";
    } else {
      WriteCodePoint(out, codePoint);
    }
  }
  out << "'";
  return true;
}

bool WriteDoubleQuotedString(ostream_wrapper& out, const std::string& str,
                             bool escapeNonAscii) {
  out << "\"";
  int codePoint;
  for (std::string::const_iterator i = str.begin();
       GetNextCodePointAndAdvance(codePoint, i, str.end());) {
    switch (codePoint) {
      case '\"':
        out << "\\\"";
        break;
      case '\\':
        out << "\\\\";
        break;
      case '\n':
        out << "\\n";
        break;
      case '\t':
        out << "\\t";
        break;
      case '\r':
        out << "\\r";
        break;
      case '\b':
        out << "\\b";
        break;
      default:
        if (codePoint < 0x20 ||
            (codePoint >= 0x80 &&
             codePoint <= 0xA0)) {  // Control characters and non-breaking space
          WriteDoubleQuoteEscapeSequence(out, codePoint);
        } else if (codePoint == 0xFEFF) {  // Byte order marks (ZWNS) should be
                                           // escaped (YAML 1.2, sec. 5.2)
          WriteDoubleQuoteEscapeSequence(out, codePoint);
        } else if (escapeNonAscii && codePoint > 0x7E) {
          WriteDoubleQuoteEscapeSequence(out, codePoint);
        } else {
          WriteCodePoint(out, codePoint);
        }
    }
  }
  out << "\"";
  return true;
}

bool WriteLiteralString(ostream_wrapper& out, const std::string& str,
                        std::size_t indent) {
  out << "|\n";
  out << IndentTo(indent);
  int codePoint;
  for (std::string::const_iterator i = str.begin();
       GetNextCodePointAndAdvance(codePoint, i, str.end());) {
    if (codePoint == '\n') {
      out << "\n" << IndentTo(indent);
    } else {
      WriteCodePoint(out, codePoint);
    }
  }
  return true;
}

bool WriteChar(ostream_wrapper& out, char ch) {
  if (('a' <= ch && ch <= 'z') || ('A' <= ch && ch <= 'Z')) {
    out << ch;
  } else if (ch == '\"') {
    out << "\"\\\"\"";
  } else if (ch == '\t') {
    out << "\"\\t\"";
  } else if (ch == '\n') {
    out << "\"\\n\"";
  } else if (ch == '\b') {
    out << "\"\\b\"";
  } else if (ch == '\\') {
    out << "\"\\\\\"";
  } else if ((0x20 <= ch && ch <= 0x7e) || ch == ' ') {
    out << "\"" << ch << "\"";
  } else {
    out << "\"";
    WriteDoubleQuoteEscapeSequence(out, ch);
    out << "\"";
  }
  return true;
}

bool WriteComment(ostream_wrapper& out, const std::string& str,
                  std::size_t postCommentIndent) {
  const std::size_t curIndent = out.col();
  out << "#" << Indentation(postCommentIndent);
  out.set_comment();
  int codePoint;
  for (std::string::const_iterator i = str.begin();
       GetNextCodePointAndAdvance(codePoint, i, str.end());) {
    if (codePoint == '\n') {
      out << "\n" << IndentTo(curIndent) << "#"
          << Indentation(postCommentIndent);
      out.set_comment();
    } else {
      WriteCodePoint(out, codePoint);
    }
  }
  return true;
}

bool WriteAlias(ostream_wrapper& out, const std::string& str) {
  out << "*";
  return WriteAliasName(out, str);
}

bool WriteAnchor(ostream_wrapper& out, const std::string& str) {
  out << "&";
  return WriteAliasName(out, str);
}

bool WriteTag(ostream_wrapper& out, const std::string& str, bool verbatim) {
  out << (verbatim ? "!<" : "!");
  StringCharSource buffer(str.c_str(), str.size());
  const RegEx& reValid = verbatim ? Exp::URI() : Exp::Tag();
  while (buffer) {
    int n = reValid.Match(buffer);
    if (n <= 0) {
      return false;
    }

    while (--n >= 0) {
      out << buffer[0];
      ++buffer;
    }
  }
  if (verbatim) {
    out << ">";
  }
  return true;
}

bool WriteTagWithPrefix(ostream_wrapper& out, const std::string& prefix,
                        const std::string& tag) {
  out << "!";
  StringCharSource prefixBuffer(prefix.c_str(), prefix.size());
  while (prefixBuffer) {
    int n = Exp::URI().Match(prefixBuffer);
    if (n <= 0) {
      return false;
    }

    while (--n >= 0) {
      out << prefixBuffer[0];
      ++prefixBuffer;
    }
  }

  out << "!";
  StringCharSource tagBuffer(tag.c_str(), tag.size());
  while (tagBuffer) {
    int n = Exp::Tag().Match(tagBuffer);
    if (n <= 0) {
      return false;
    }

    while (--n >= 0) {
      out << tagBuffer[0];
      ++tagBuffer;
    }
  }
  return true;
}

bool WriteBinary(ostream_wrapper& out, const Binary& binary) {
  WriteDoubleQuotedString(out, EncodeBase64(binary.data(), binary.size()),
                          false);
  return true;
}
}
}
