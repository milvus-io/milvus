#include "scanscalar.h"

#include <algorithm>

#include "exp.h"
#include "regeximpl.h"
#include "stream.h"
#include "yaml-cpp/exceptions.h"  // IWYU pragma: keep

namespace YAML {
// ScanScalar
// . This is where the scalar magic happens.
//
// . We do the scanning in three phases:
//   1. Scan until newline
//   2. Eat newline
//   3. Scan leading blanks.
//
// . Depending on the parameters given, we store or stop
//   and different places in the above flow.
std::string ScanScalar(Stream& INPUT, ScanScalarParams& params) {
  bool foundNonEmptyLine = false;
  bool pastOpeningBreak = (params.fold == FOLD_FLOW);
  bool emptyLine = false, moreIndented = false;
  int foldedNewlineCount = 0;
  bool foldedNewlineStartedMoreIndented = false;
  std::size_t lastEscapedChar = std::string::npos;
  std::string scalar;
  params.leadingSpaces = false;

  if (!params.end) {
    params.end = &Exp::Empty();
  }

  while (INPUT) {
    // ********************************
    // Phase #1: scan until line ending

    std::size_t lastNonWhitespaceChar = scalar.size();
    bool escapedNewline = false;
    while (!params.end->Matches(INPUT) && !Exp::Break().Matches(INPUT)) {
      if (!INPUT) {
        break;
      }

      // document indicator?
      if (INPUT.column() == 0 && Exp::DocIndicator().Matches(INPUT)) {
        if (params.onDocIndicator == BREAK) {
          break;
        } else if (params.onDocIndicator == THROW) {
          throw ParserException(INPUT.mark(), ErrorMsg::DOC_IN_SCALAR);
        }
      }

      foundNonEmptyLine = true;
      pastOpeningBreak = true;

      // escaped newline? (only if we're escaping on slash)
      if (params.escape == '\\' && Exp::EscBreak().Matches(INPUT)) {
        // eat escape character and get out (but preserve trailing whitespace!)
        INPUT.get();
        lastNonWhitespaceChar = scalar.size();
        lastEscapedChar = scalar.size();
        escapedNewline = true;
        break;
      }

      // escape this?
      if (INPUT.peek() == params.escape) {
        scalar += Exp::Escape(INPUT);
        lastNonWhitespaceChar = scalar.size();
        lastEscapedChar = scalar.size();
        continue;
      }

      // otherwise, just add the damn character
      char ch = INPUT.get();
      scalar += ch;
      if (ch != ' ' && ch != '\t') {
        lastNonWhitespaceChar = scalar.size();
      }
    }

    // eof? if we're looking to eat something, then we throw
    if (!INPUT) {
      if (params.eatEnd) {
        throw ParserException(INPUT.mark(), ErrorMsg::EOF_IN_SCALAR);
      }
      break;
    }

    // doc indicator?
    if (params.onDocIndicator == BREAK && INPUT.column() == 0 &&
        Exp::DocIndicator().Matches(INPUT)) {
      break;
    }

    // are we done via character match?
    int n = params.end->Match(INPUT);
    if (n >= 0) {
      if (params.eatEnd) {
        INPUT.eat(n);
      }
      break;
    }

    // do we remove trailing whitespace?
    if (params.fold == FOLD_FLOW)
      scalar.erase(lastNonWhitespaceChar);

    // ********************************
    // Phase #2: eat line ending
    n = Exp::Break().Match(INPUT);
    INPUT.eat(n);

    // ********************************
    // Phase #3: scan initial spaces

    // first the required indentation
    while (INPUT.peek() == ' ' &&
           (INPUT.column() < params.indent ||
            (params.detectIndent && !foundNonEmptyLine)) &&
           !params.end->Matches(INPUT)) {
      INPUT.eat(1);
    }

    // update indent if we're auto-detecting
    if (params.detectIndent && !foundNonEmptyLine) {
      params.indent = std::max(params.indent, INPUT.column());
    }

    // and then the rest of the whitespace
    while (Exp::Blank().Matches(INPUT)) {
      // we check for tabs that masquerade as indentation
      if (INPUT.peek() == '\t' && INPUT.column() < params.indent &&
          params.onTabInIndentation == THROW) {
        throw ParserException(INPUT.mark(), ErrorMsg::TAB_IN_INDENTATION);
      }

      if (!params.eatLeadingWhitespace) {
        break;
      }

      if (params.end->Matches(INPUT)) {
        break;
      }

      INPUT.eat(1);
    }

    // was this an empty line?
    bool nextEmptyLine = Exp::Break().Matches(INPUT);
    bool nextMoreIndented = Exp::Blank().Matches(INPUT);
    if (params.fold == FOLD_BLOCK && foldedNewlineCount == 0 && nextEmptyLine)
      foldedNewlineStartedMoreIndented = moreIndented;

    // for block scalars, we always start with a newline, so we should ignore it
    // (not fold or keep)
    if (pastOpeningBreak) {
      switch (params.fold) {
        case DONT_FOLD:
          scalar += "\n";
          break;
        case FOLD_BLOCK:
          if (!emptyLine && !nextEmptyLine && !moreIndented &&
              !nextMoreIndented && INPUT.column() >= params.indent) {
            scalar += " ";
          } else if (nextEmptyLine) {
            foldedNewlineCount++;
          } else {
            scalar += "\n";
          }

          if (!nextEmptyLine && foldedNewlineCount > 0) {
            scalar += std::string(foldedNewlineCount - 1, '\n');
            if (foldedNewlineStartedMoreIndented ||
                nextMoreIndented | !foundNonEmptyLine) {
              scalar += "\n";
            }
            foldedNewlineCount = 0;
          }
          break;
        case FOLD_FLOW:
          if (nextEmptyLine) {
            scalar += "\n";
          } else if (!emptyLine && !nextEmptyLine && !escapedNewline) {
            scalar += " ";
          }
          break;
      }
    }

    emptyLine = nextEmptyLine;
    moreIndented = nextMoreIndented;
    pastOpeningBreak = true;

    // are we done via indentation?
    if (!emptyLine && INPUT.column() < params.indent) {
      params.leadingSpaces = true;
      break;
    }
  }

  // post-processing
  if (params.trimTrailingSpaces) {
    std::size_t pos = scalar.find_last_not_of(' ');
    if (lastEscapedChar != std::string::npos) {
      if (pos < lastEscapedChar || pos == std::string::npos) {
        pos = lastEscapedChar;
      }
    }
    if (pos < scalar.size()) {
      scalar.erase(pos + 1);
    }
  }

  switch (params.chomp) {
    case CLIP: {
      std::size_t pos = scalar.find_last_not_of('\n');
      if (lastEscapedChar != std::string::npos) {
        if (pos < lastEscapedChar || pos == std::string::npos) {
          pos = lastEscapedChar;
        }
      }
      if (pos == std::string::npos) {
        scalar.erase();
      } else if (pos + 1 < scalar.size()) {
        scalar.erase(pos + 2);
      }
    } break;
    case STRIP: {
      std::size_t pos = scalar.find_last_not_of('\n');
      if (lastEscapedChar != std::string::npos) {
        if (pos < lastEscapedChar || pos == std::string::npos) {
          pos = lastEscapedChar;
        }
      }
      if (pos == std::string::npos) {
        scalar.erase();
      } else if (pos < scalar.size()) {
        scalar.erase(pos + 1);
      }
    } break;
    default:
      break;
  }

  return scalar;
}
}
