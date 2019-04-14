#ifndef SCANSCALAR_H_62B23520_7C8E_11DE_8A39_0800200C9A66
#define SCANSCALAR_H_62B23520_7C8E_11DE_8A39_0800200C9A66

#if defined(_MSC_VER) ||                                            \
    (defined(__GNUC__) && (__GNUC__ == 3 && __GNUC_MINOR__ >= 4) || \
     (__GNUC__ >= 4))  // GCC supports "pragma once" correctly since 3.4
#pragma once
#endif

#include <string>

#include "regex_yaml.h"
#include "stream.h"

namespace YAML {
enum CHOMP { STRIP = -1, CLIP, KEEP };
enum ACTION { NONE, BREAK, THROW };
enum FOLD { DONT_FOLD, FOLD_BLOCK, FOLD_FLOW };

struct ScanScalarParams {
  ScanScalarParams()
      : end(nullptr),
        eatEnd(false),
        indent(0),
        detectIndent(false),
        eatLeadingWhitespace(0),
        escape(0),
        fold(DONT_FOLD),
        trimTrailingSpaces(0),
        chomp(CLIP),
        onDocIndicator(NONE),
        onTabInIndentation(NONE),
        leadingSpaces(false) {}

  // input:
  const RegEx* end;   // what condition ends this scalar?
                      // unowned.
  bool eatEnd;        // should we eat that condition when we see it?
  int indent;         // what level of indentation should be eaten and ignored?
  bool detectIndent;  // should we try to autodetect the indent?
  bool eatLeadingWhitespace;  // should we continue eating this delicious
                              // indentation after 'indent' spaces?
  char escape;  // what character do we escape on (i.e., slash or single quote)
                // (0 for none)
  FOLD fold;    // how do we fold line ends?
  bool trimTrailingSpaces;  // do we remove all trailing spaces (at the very
                            // end)
  CHOMP chomp;  // do we strip, clip, or keep trailing newlines (at the very
                // end)
  //   Note: strip means kill all, clip means keep at most one, keep means keep
  // all
  ACTION onDocIndicator;      // what do we do if we see a document indicator?
  ACTION onTabInIndentation;  // what do we do if we see a tab where we should
                              // be seeing indentation spaces

  // output:
  bool leadingSpaces;
};

std::string ScanScalar(Stream& INPUT, ScanScalarParams& info);
}

#endif  // SCANSCALAR_H_62B23520_7C8E_11DE_8A39_0800200C9A66
