#include "exp.h"
#include "regex_yaml.h"
#include "regeximpl.h"
#include "stream.h"
#include "yaml-cpp/exceptions.h"  // IWYU pragma: keep
#include "yaml-cpp/mark.h"

namespace YAML {
const std::string ScanVerbatimTag(Stream& INPUT) {
  std::string tag;

  // eat the start character
  INPUT.get();

  while (INPUT) {
    if (INPUT.peek() == Keys::VerbatimTagEnd) {
      // eat the end character
      INPUT.get();
      return tag;
    }

    int n = Exp::URI().Match(INPUT);
    if (n <= 0)
      break;

    tag += INPUT.get(n);
  }

  throw ParserException(INPUT.mark(), ErrorMsg::END_OF_VERBATIM_TAG);
}

const std::string ScanTagHandle(Stream& INPUT, bool& canBeHandle) {
  std::string tag;
  canBeHandle = true;
  Mark firstNonWordChar;

  while (INPUT) {
    if (INPUT.peek() == Keys::Tag) {
      if (!canBeHandle)
        throw ParserException(firstNonWordChar, ErrorMsg::CHAR_IN_TAG_HANDLE);
      break;
    }

    int n = 0;
    if (canBeHandle) {
      n = Exp::Word().Match(INPUT);
      if (n <= 0) {
        canBeHandle = false;
        firstNonWordChar = INPUT.mark();
      }
    }

    if (!canBeHandle)
      n = Exp::Tag().Match(INPUT);

    if (n <= 0)
      break;

    tag += INPUT.get(n);
  }

  return tag;
}

const std::string ScanTagSuffix(Stream& INPUT) {
  std::string tag;

  while (INPUT) {
    int n = Exp::Tag().Match(INPUT);
    if (n <= 0)
      break;

    tag += INPUT.get(n);
  }

  if (tag.empty())
    throw ParserException(INPUT.mark(), ErrorMsg::TAG_WITH_NO_SUFFIX);

  return tag;
}
}
