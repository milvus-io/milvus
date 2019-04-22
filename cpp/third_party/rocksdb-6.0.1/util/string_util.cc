//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "util/string_util.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <errno.h>
#include <inttypes.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <algorithm>
#include <cmath>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include "rocksdb/env.h"
#include "rocksdb/slice.h"

namespace rocksdb {

const std::string kNullptrString = "nullptr";

std::vector<std::string> StringSplit(const std::string& arg, char delim) {
  std::vector<std::string> splits;
  std::stringstream ss(arg);
  std::string item;
  while (std::getline(ss, item, delim)) {
    splits.push_back(item);
  }
  return splits;
}

// for micros < 10ms, print "XX us".
// for micros < 10sec, print "XX ms".
// for micros >= 10 sec, print "XX sec".
// for micros <= 1 hour, print Y:X M:S".
// for micros > 1 hour, print Z:Y:X H:M:S".
int AppendHumanMicros(uint64_t micros, char* output, int len,
                      bool fixed_format) {
  if (micros < 10000 && !fixed_format) {
    return snprintf(output, len, "%" PRIu64 " us", micros);
  } else if (micros < 10000000 && !fixed_format) {
    return snprintf(output, len, "%.3lf ms",
                    static_cast<double>(micros) / 1000);
  } else if (micros < 1000000l * 60 && !fixed_format) {
    return snprintf(output, len, "%.3lf sec",
                    static_cast<double>(micros) / 1000000);
  } else if (micros < 1000000ll * 60 * 60 && !fixed_format) {
    return snprintf(output, len, "%02" PRIu64 ":%05.3f M:S",
                    micros / 1000000 / 60,
                    static_cast<double>(micros % 60000000) / 1000000);
  } else {
    return snprintf(output, len, "%02" PRIu64 ":%02" PRIu64 ":%05.3f H:M:S",
                    micros / 1000000 / 3600, (micros / 1000000 / 60) % 60,
                    static_cast<double>(micros % 60000000) / 1000000);
  }
}

// for sizes >=10TB, print "XXTB"
// for sizes >=10GB, print "XXGB"
// etc.
// append file size summary to output and return the len
int AppendHumanBytes(uint64_t bytes, char* output, int len) {
  const uint64_t ull10 = 10;
  if (bytes >= ull10 << 40) {
    return snprintf(output, len, "%" PRIu64 "TB", bytes >> 40);
  } else if (bytes >= ull10 << 30) {
    return snprintf(output, len, "%" PRIu64 "GB", bytes >> 30);
  } else if (bytes >= ull10 << 20) {
    return snprintf(output, len, "%" PRIu64 "MB", bytes >> 20);
  } else if (bytes >= ull10 << 10) {
    return snprintf(output, len, "%" PRIu64 "KB", bytes >> 10);
  } else {
    return snprintf(output, len, "%" PRIu64 "B", bytes);
  }
}

void AppendNumberTo(std::string* str, uint64_t num) {
  char buf[30];
  snprintf(buf, sizeof(buf), "%" PRIu64, num);
  str->append(buf);
}

void AppendEscapedStringTo(std::string* str, const Slice& value) {
  for (size_t i = 0; i < value.size(); i++) {
    char c = value[i];
    if (c >= ' ' && c <= '~') {
      str->push_back(c);
    } else {
      char buf[10];
      snprintf(buf, sizeof(buf), "\\x%02x",
               static_cast<unsigned int>(c) & 0xff);
      str->append(buf);
    }
  }
}

std::string NumberToString(uint64_t num) {
  std::string r;
  AppendNumberTo(&r, num);
  return r;
}

std::string NumberToHumanString(int64_t num) {
  char buf[19];
  int64_t absnum = num < 0 ? -num : num;
  if (absnum < 10000) {
    snprintf(buf, sizeof(buf), "%" PRIi64, num);
  } else if (absnum < 10000000) {
    snprintf(buf, sizeof(buf), "%" PRIi64 "K", num / 1000);
  } else if (absnum < 10000000000LL) {
    snprintf(buf, sizeof(buf), "%" PRIi64 "M", num / 1000000);
  } else {
    snprintf(buf, sizeof(buf), "%" PRIi64 "G", num / 1000000000);
  }
  return std::string(buf);
}

std::string BytesToHumanString(uint64_t bytes) {
  const char* size_name[] = {"KB", "MB", "GB", "TB"};
  double final_size = static_cast<double>(bytes);
  size_t size_idx;

  // always start with KB
  final_size /= 1024;
  size_idx = 0;

  while (size_idx < 3 && final_size >= 1024) {
    final_size /= 1024;
    size_idx++;
  }

  char buf[20];
  snprintf(buf, sizeof(buf), "%.2f %s", final_size, size_name[size_idx]);
  return std::string(buf);
}

std::string EscapeString(const Slice& value) {
  std::string r;
  AppendEscapedStringTo(&r, value);
  return r;
}

bool ConsumeDecimalNumber(Slice* in, uint64_t* val) {
  uint64_t v = 0;
  int digits = 0;
  while (!in->empty()) {
    char c = (*in)[0];
    if (c >= '0' && c <= '9') {
      ++digits;
      const unsigned int delta = (c - '0');
      static const uint64_t kMaxUint64 = ~static_cast<uint64_t>(0);
      if (v > kMaxUint64 / 10 ||
          (v == kMaxUint64 / 10 && delta > kMaxUint64 % 10)) {
        // Overflow
        return false;
      }
      v = (v * 10) + delta;
      in->remove_prefix(1);
    } else {
      break;
    }
  }
  *val = v;
  return (digits > 0);
}

bool isSpecialChar(const char c) {
  if (c == '\\' || c == '#' || c == ':' || c == '\r' || c == '\n') {
    return true;
  }
  return false;
}

namespace {
using CharMap = std::pair<char, char>;
}

char UnescapeChar(const char c) {
  static const CharMap convert_map[] = {{'r', '\r'}, {'n', '\n'}};

  auto iter = std::find_if(std::begin(convert_map), std::end(convert_map),
                           [c](const CharMap& p) { return p.first == c; });

  if (iter == std::end(convert_map)) {
    return c;
  }
  return iter->second;
}

char EscapeChar(const char c) {
  static const CharMap convert_map[] = {{'\n', 'n'}, {'\r', 'r'}};

  auto iter = std::find_if(std::begin(convert_map), std::end(convert_map),
                           [c](const CharMap& p) { return p.first == c; });

  if (iter == std::end(convert_map)) {
    return c;
  }
  return iter->second;
}

std::string EscapeOptionString(const std::string& raw_string) {
  std::string output;
  for (auto c : raw_string) {
    if (isSpecialChar(c)) {
      output += '\\';
      output += EscapeChar(c);
    } else {
      output += c;
    }
  }

  return output;
}

std::string UnescapeOptionString(const std::string& escaped_string) {
  bool escaped = false;
  std::string output;

  for (auto c : escaped_string) {
    if (escaped) {
      output += UnescapeChar(c);
      escaped = false;
    } else {
      if (c == '\\') {
        escaped = true;
        continue;
      }
      output += c;
    }
  }
  return output;
}

std::string trim(const std::string& str) {
  if (str.empty()) return std::string();
  size_t start = 0;
  size_t end = str.size() - 1;
  while (isspace(str[start]) != 0 && start < end) {
    ++start;
  }
  while (isspace(str[end]) != 0 && start < end) {
    --end;
  }
  if (start <= end) {
    return str.substr(start, end - start + 1);
  }
  return std::string();
}

#ifndef ROCKSDB_LITE

bool ParseBoolean(const std::string& type, const std::string& value) {
  if (value == "true" || value == "1") {
    return true;
  } else if (value == "false" || value == "0") {
    return false;
  }
  throw std::invalid_argument(type);
}

uint32_t ParseUint32(const std::string& value) {
  uint64_t num = ParseUint64(value);
  if ((num >> 32LL) == 0) {
    return static_cast<uint32_t>(num);
  } else {
    throw std::out_of_range(value);
  }
}

#endif

uint64_t ParseUint64(const std::string& value) {
  size_t endchar;
#ifndef CYGWIN
  uint64_t num = std::stoull(value.c_str(), &endchar);
#else
  char* endptr;
  uint64_t num = std::strtoul(value.c_str(), &endptr, 0);
  endchar = endptr - value.c_str();
#endif

  if (endchar < value.length()) {
    char c = value[endchar];
    if (c == 'k' || c == 'K')
      num <<= 10LL;
    else if (c == 'm' || c == 'M')
      num <<= 20LL;
    else if (c == 'g' || c == 'G')
      num <<= 30LL;
    else if (c == 't' || c == 'T')
      num <<= 40LL;
  }

  return num;
}

int ParseInt(const std::string& value) {
  size_t endchar;
#ifndef CYGWIN
  int num = std::stoi(value.c_str(), &endchar);
#else
  char* endptr;
  int num = std::strtoul(value.c_str(), &endptr, 0);
  endchar = endptr - value.c_str();
#endif

  if (endchar < value.length()) {
    char c = value[endchar];
    if (c == 'k' || c == 'K')
      num <<= 10;
    else if (c == 'm' || c == 'M')
      num <<= 20;
    else if (c == 'g' || c == 'G')
      num <<= 30;
  }

  return num;
}

double ParseDouble(const std::string& value) {
#ifndef CYGWIN
  return std::stod(value);
#else
  return std::strtod(value.c_str(), 0);
#endif
}

size_t ParseSizeT(const std::string& value) {
  return static_cast<size_t>(ParseUint64(value));
}

std::vector<int> ParseVectorInt(const std::string& value) {
  std::vector<int> result;
  size_t start = 0;
  while (start < value.size()) {
    size_t end = value.find(':', start);
    if (end == std::string::npos) {
      result.push_back(ParseInt(value.substr(start)));
      break;
    } else {
      result.push_back(ParseInt(value.substr(start, end - start)));
      start = end + 1;
    }
  }
  return result;
}

bool SerializeIntVector(const std::vector<int>& vec, std::string* value) {
  *value = "";
  for (size_t i = 0; i < vec.size(); ++i) {
    if (i > 0) {
      *value += ":";
    }
    *value += ToString(vec[i]);
  }
  return true;
}

}  // namespace rocksdb
