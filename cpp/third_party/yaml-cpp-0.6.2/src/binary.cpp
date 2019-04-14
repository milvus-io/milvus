#include "yaml-cpp/binary.h"

namespace YAML {
static const char encoding[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

std::string EncodeBase64(const unsigned char *data, std::size_t size) {
  const char PAD = '=';

  std::string ret;
  ret.resize(4 * size / 3 + 3);
  char *out = &ret[0];

  std::size_t chunks = size / 3;
  std::size_t remainder = size % 3;

  for (std::size_t i = 0; i < chunks; i++, data += 3) {
    *out++ = encoding[data[0] >> 2];
    *out++ = encoding[((data[0] & 0x3) << 4) | (data[1] >> 4)];
    *out++ = encoding[((data[1] & 0xf) << 2) | (data[2] >> 6)];
    *out++ = encoding[data[2] & 0x3f];
  }

  switch (remainder) {
    case 0:
      break;
    case 1:
      *out++ = encoding[data[0] >> 2];
      *out++ = encoding[((data[0] & 0x3) << 4)];
      *out++ = PAD;
      *out++ = PAD;
      break;
    case 2:
      *out++ = encoding[data[0] >> 2];
      *out++ = encoding[((data[0] & 0x3) << 4) | (data[1] >> 4)];
      *out++ = encoding[((data[1] & 0xf) << 2)];
      *out++ = PAD;
      break;
  }

  ret.resize(out - &ret[0]);
  return ret;
}

static const unsigned char decoding[] = {
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 62,  255,
    255, 255, 63,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  255, 255,
    255, 0,   255, 255, 255, 0,   1,   2,   3,   4,   5,   6,   7,   8,   9,
    10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,
    25,  255, 255, 255, 255, 255, 255, 26,  27,  28,  29,  30,  31,  32,  33,
    34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,  45,  46,  47,  48,
    49,  50,  51,  255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255,
};

std::vector<unsigned char> DecodeBase64(const std::string &input) {
  typedef std::vector<unsigned char> ret_type;
  if (input.empty())
    return ret_type();

  ret_type ret(3 * input.size() / 4 + 1);
  unsigned char *out = &ret[0];

  unsigned value = 0;
  for (std::size_t i = 0; i < input.size(); i++) {
    unsigned char d = decoding[static_cast<unsigned>(input[i])];
    if (d == 255)
      return ret_type();

    value = (value << 6) | d;
    if (i % 4 == 3) {
      *out++ = value >> 16;
      if (i > 0 && input[i - 1] != '=')
        *out++ = value >> 8;
      if (input[i] != '=')
        *out++ = value;
    }
  }

  ret.resize(out - &ret[0]);
  return ret;
}
}
