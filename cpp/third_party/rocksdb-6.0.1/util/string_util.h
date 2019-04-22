//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#pragma once

#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

namespace rocksdb {

class Slice;

extern std::vector<std::string> StringSplit(const std::string& arg, char delim);

template <typename T>
inline std::string ToString(T value) {
#if !(defined OS_ANDROID) && !(defined CYGWIN) && !(defined OS_FREEBSD)
  return std::to_string(value);
#else
  // Andorid or cygwin doesn't support all of C++11, std::to_string() being
  // one of the not supported features.
  std::ostringstream os;
  os << value;
  return os.str();
#endif
}

// Append a human-readable printout of "num" to *str
extern void AppendNumberTo(std::string* str, uint64_t num);

// Append a human-readable printout of "value" to *str.
// Escapes any non-printable characters found in "value".
extern void AppendEscapedStringTo(std::string* str, const Slice& value);

// Return a string printout of "num"
extern std::string NumberToString(uint64_t num);

// Return a human-readable version of num.
// for num >= 10.000, prints "xxK"
// for num >= 10.000.000, prints "xxM"
// for num >= 10.000.000.000, prints "xxG"
extern std::string NumberToHumanString(int64_t num);

// Return a human-readable version of bytes
// ex: 1048576 -> 1.00 GB
extern std::string BytesToHumanString(uint64_t bytes);

// Append a human-readable time in micros.
int AppendHumanMicros(uint64_t micros, char* output, int len,
                      bool fixed_format);

// Append a human-readable size in bytes
int AppendHumanBytes(uint64_t bytes, char* output, int len);

// Return a human-readable version of "value".
// Escapes any non-printable characters found in "value".
extern std::string EscapeString(const Slice& value);

// Parse a human-readable number from "*in" into *value.  On success,
// advances "*in" past the consumed number and sets "*val" to the
// numeric value.  Otherwise, returns false and leaves *in in an
// unspecified state.
extern bool ConsumeDecimalNumber(Slice* in, uint64_t* val);

// Returns true if the input char "c" is considered as a special character
// that will be escaped when EscapeOptionString() is called.
//
// @param c the input char
// @return true if the input char "c" is considered as a special character.
// @see EscapeOptionString
bool isSpecialChar(const char c);

// If the input char is an escaped char, it will return the its
// associated raw-char.  Otherwise, the function will simply return
// the original input char.
char UnescapeChar(const char c);

// If the input char is a control char, it will return the its
// associated escaped char.  Otherwise, the function will simply return
// the original input char.
char EscapeChar(const char c);

// Converts a raw string to an escaped string.  Escaped-characters are
// defined via the isSpecialChar() function.  When a char in the input
// string "raw_string" is classified as a special characters, then it
// will be prefixed by '\' in the output.
//
// It's inverse function is UnescapeOptionString().
// @param raw_string the input string
// @return the '\' escaped string of the input "raw_string"
// @see isSpecialChar, UnescapeOptionString
std::string EscapeOptionString(const std::string& raw_string);

// The inverse function of EscapeOptionString.  It converts
// an '\' escaped string back to a raw string.
//
// @param escaped_string the input '\' escaped string
// @return the raw string of the input "escaped_string"
std::string UnescapeOptionString(const std::string& escaped_string);

std::string trim(const std::string& str);

#ifndef ROCKSDB_LITE
bool ParseBoolean(const std::string& type, const std::string& value);

uint32_t ParseUint32(const std::string& value);
#endif

uint64_t ParseUint64(const std::string& value);

int ParseInt(const std::string& value);

double ParseDouble(const std::string& value);

size_t ParseSizeT(const std::string& value);

std::vector<int> ParseVectorInt(const std::string& value);

bool SerializeIntVector(const std::vector<int>& vec, std::string* value);

extern const std::string kNullptrString;

}  // namespace rocksdb
