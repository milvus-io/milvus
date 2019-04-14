#include <iostream>

#include "stream.h"

#ifndef YAML_PREFETCH_SIZE
#define YAML_PREFETCH_SIZE 2048
#endif

#define S_ARRAY_SIZE(A) (sizeof(A) / sizeof(*(A)))
#define S_ARRAY_END(A) ((A) + S_ARRAY_SIZE(A))

#define CP_REPLACEMENT_CHARACTER (0xFFFD)

namespace YAML {
enum UtfIntroState {
  uis_start,
  uis_utfbe_b1,
  uis_utf32be_b2,
  uis_utf32be_bom3,
  uis_utf32be,
  uis_utf16be,
  uis_utf16be_bom1,
  uis_utfle_bom1,
  uis_utf16le_bom2,
  uis_utf32le_bom3,
  uis_utf16le,
  uis_utf32le,
  uis_utf8_imp,
  uis_utf16le_imp,
  uis_utf32le_imp3,
  uis_utf8_bom1,
  uis_utf8_bom2,
  uis_utf8,
  uis_error
};

enum UtfIntroCharType {
  uict00,
  uictBB,
  uictBF,
  uictEF,
  uictFE,
  uictFF,
  uictAscii,
  uictOther,
  uictMax
};

static bool s_introFinalState[] = {
    false,  // uis_start
    false,  // uis_utfbe_b1
    false,  // uis_utf32be_b2
    false,  // uis_utf32be_bom3
    true,   // uis_utf32be
    true,   // uis_utf16be
    false,  // uis_utf16be_bom1
    false,  // uis_utfle_bom1
    false,  // uis_utf16le_bom2
    false,  // uis_utf32le_bom3
    true,   // uis_utf16le
    true,   // uis_utf32le
    false,  // uis_utf8_imp
    false,  // uis_utf16le_imp
    false,  // uis_utf32le_imp3
    false,  // uis_utf8_bom1
    false,  // uis_utf8_bom2
    true,   // uis_utf8
    true,   // uis_error
};

static UtfIntroState s_introTransitions[][uictMax] = {
    // uict00,           uictBB,           uictBF,           uictEF,
    // uictFE,           uictFF,           uictAscii,        uictOther
    {uis_utfbe_b1, uis_utf8, uis_utf8, uis_utf8_bom1, uis_utf16be_bom1,
     uis_utfle_bom1, uis_utf8_imp, uis_utf8},
    {uis_utf32be_b2, uis_utf8, uis_utf8, uis_utf8, uis_utf8, uis_utf8,
     uis_utf16be, uis_utf8},
    {uis_utf32be, uis_utf8, uis_utf8, uis_utf8, uis_utf32be_bom3, uis_utf8,
     uis_utf8, uis_utf8},
    {uis_utf8, uis_utf8, uis_utf8, uis_utf8, uis_utf8, uis_utf32be, uis_utf8,
     uis_utf8},
    {uis_utf32be, uis_utf32be, uis_utf32be, uis_utf32be, uis_utf32be,
     uis_utf32be, uis_utf32be, uis_utf32be},
    {uis_utf16be, uis_utf16be, uis_utf16be, uis_utf16be, uis_utf16be,
     uis_utf16be, uis_utf16be, uis_utf16be},
    {uis_utf8, uis_utf8, uis_utf8, uis_utf8, uis_utf8, uis_utf16be, uis_utf8,
     uis_utf8},
    {uis_utf8, uis_utf8, uis_utf8, uis_utf8, uis_utf16le_bom2, uis_utf8,
     uis_utf8, uis_utf8},
    {uis_utf32le_bom3, uis_utf16le, uis_utf16le, uis_utf16le, uis_utf16le,
     uis_utf16le, uis_utf16le, uis_utf16le},
    {uis_utf32le, uis_utf16le, uis_utf16le, uis_utf16le, uis_utf16le,
     uis_utf16le, uis_utf16le, uis_utf16le},
    {uis_utf16le, uis_utf16le, uis_utf16le, uis_utf16le, uis_utf16le,
     uis_utf16le, uis_utf16le, uis_utf16le},
    {uis_utf32le, uis_utf32le, uis_utf32le, uis_utf32le, uis_utf32le,
     uis_utf32le, uis_utf32le, uis_utf32le},
    {uis_utf16le_imp, uis_utf8, uis_utf8, uis_utf8, uis_utf8, uis_utf8,
     uis_utf8, uis_utf8},
    {uis_utf32le_imp3, uis_utf16le, uis_utf16le, uis_utf16le, uis_utf16le,
     uis_utf16le, uis_utf16le, uis_utf16le},
    {uis_utf32le, uis_utf16le, uis_utf16le, uis_utf16le, uis_utf16le,
     uis_utf16le, uis_utf16le, uis_utf16le},
    {uis_utf8, uis_utf8_bom2, uis_utf8, uis_utf8, uis_utf8, uis_utf8, uis_utf8,
     uis_utf8},
    {uis_utf8, uis_utf8, uis_utf8, uis_utf8, uis_utf8, uis_utf8, uis_utf8,
     uis_utf8},
    {uis_utf8, uis_utf8, uis_utf8, uis_utf8, uis_utf8, uis_utf8, uis_utf8,
     uis_utf8},
};

static char s_introUngetCount[][uictMax] = {
    // uict00, uictBB, uictBF, uictEF, uictFE, uictFF, uictAscii, uictOther
    {0, 1, 1, 0, 0, 0, 0, 1},
    {0, 2, 2, 2, 2, 2, 2, 2},
    {3, 3, 3, 3, 0, 3, 3, 3},
    {4, 4, 4, 4, 4, 0, 4, 4},
    {1, 1, 1, 1, 1, 1, 1, 1},
    {1, 1, 1, 1, 1, 1, 1, 1},
    {2, 2, 2, 2, 2, 0, 2, 2},
    {2, 2, 2, 2, 0, 2, 2, 2},
    {0, 1, 1, 1, 1, 1, 1, 1},
    {0, 2, 2, 2, 2, 2, 2, 2},
    {1, 1, 1, 1, 1, 1, 1, 1},
    {1, 1, 1, 1, 1, 1, 1, 1},
    {0, 2, 2, 2, 2, 2, 2, 2},
    {0, 3, 3, 3, 3, 3, 3, 3},
    {4, 4, 4, 4, 4, 4, 4, 4},
    {2, 0, 2, 2, 2, 2, 2, 2},
    {3, 3, 0, 3, 3, 3, 3, 3},
    {1, 1, 1, 1, 1, 1, 1, 1},
};

inline UtfIntroCharType IntroCharTypeOf(std::istream::int_type ch) {
  if (std::istream::traits_type::eof() == ch) {
    return uictOther;
  }

  switch (ch) {
    case 0:
      return uict00;
    case 0xBB:
      return uictBB;
    case 0xBF:
      return uictBF;
    case 0xEF:
      return uictEF;
    case 0xFE:
      return uictFE;
    case 0xFF:
      return uictFF;
  }

  if ((ch > 0) && (ch < 0xFF)) {
    return uictAscii;
  }

  return uictOther;
}

inline char Utf8Adjust(unsigned long ch, unsigned char lead_bits,
                       unsigned char rshift) {
  const unsigned char header = ((1 << lead_bits) - 1) << (8 - lead_bits);
  const unsigned char mask = (0xFF >> (lead_bits + 1));
  return static_cast<char>(
      static_cast<unsigned char>(header | ((ch >> rshift) & mask)));
}

inline void QueueUnicodeCodepoint(std::deque<char>& q, unsigned long ch) {
  // We are not allowed to queue the Stream::eof() codepoint, so
  // replace it with CP_REPLACEMENT_CHARACTER
  if (static_cast<unsigned long>(Stream::eof()) == ch) {
    ch = CP_REPLACEMENT_CHARACTER;
  }

  if (ch < 0x80) {
    q.push_back(Utf8Adjust(ch, 0, 0));
  } else if (ch < 0x800) {
    q.push_back(Utf8Adjust(ch, 2, 6));
    q.push_back(Utf8Adjust(ch, 1, 0));
  } else if (ch < 0x10000) {
    q.push_back(Utf8Adjust(ch, 3, 12));
    q.push_back(Utf8Adjust(ch, 1, 6));
    q.push_back(Utf8Adjust(ch, 1, 0));
  } else {
    q.push_back(Utf8Adjust(ch, 4, 18));
    q.push_back(Utf8Adjust(ch, 1, 12));
    q.push_back(Utf8Adjust(ch, 1, 6));
    q.push_back(Utf8Adjust(ch, 1, 0));
  }
}

Stream::Stream(std::istream& input)
    : m_input(input),
      m_pPrefetched(new unsigned char[YAML_PREFETCH_SIZE]),
      m_nPrefetchedAvailable(0),
      m_nPrefetchedUsed(0) {
  typedef std::istream::traits_type char_traits;

  if (!input)
    return;

  // Determine (or guess) the character-set by reading the BOM, if any.  See
  // the YAML specification for the determination algorithm.
  char_traits::int_type intro[4];
  int nIntroUsed = 0;
  UtfIntroState state = uis_start;
  for (; !s_introFinalState[state];) {
    std::istream::int_type ch = input.get();
    intro[nIntroUsed++] = ch;
    UtfIntroCharType charType = IntroCharTypeOf(ch);
    UtfIntroState newState = s_introTransitions[state][charType];
    int nUngets = s_introUngetCount[state][charType];
    if (nUngets > 0) {
      input.clear();
      for (; nUngets > 0; --nUngets) {
        if (char_traits::eof() != intro[--nIntroUsed])
          input.putback(char_traits::to_char_type(intro[nIntroUsed]));
      }
    }
    state = newState;
  }

  switch (state) {
    case uis_utf8:
      m_charSet = utf8;
      break;
    case uis_utf16le:
      m_charSet = utf16le;
      break;
    case uis_utf16be:
      m_charSet = utf16be;
      break;
    case uis_utf32le:
      m_charSet = utf32le;
      break;
    case uis_utf32be:
      m_charSet = utf32be;
      break;
    default:
      m_charSet = utf8;
      break;
  }

  ReadAheadTo(0);
}

Stream::~Stream() { delete[] m_pPrefetched; }

char Stream::peek() const {
  if (m_readahead.empty()) {
    return Stream::eof();
  }

  return m_readahead[0];
}

Stream::operator bool() const {
  return m_input.good() ||
         (!m_readahead.empty() && m_readahead[0] != Stream::eof());
}

// get
// . Extracts a character from the stream and updates our position
char Stream::get() {
  char ch = peek();
  AdvanceCurrent();
  m_mark.column++;

  if (ch == '\n') {
    m_mark.column = 0;
    m_mark.line++;
  }

  return ch;
}

// get
// . Extracts 'n' characters from the stream and updates our position
std::string Stream::get(int n) {
  std::string ret;
  ret.reserve(n);
  for (int i = 0; i < n; i++)
    ret += get();
  return ret;
}

// eat
// . Eats 'n' characters and updates our position.
void Stream::eat(int n) {
  for (int i = 0; i < n; i++)
    get();
}

void Stream::AdvanceCurrent() {
  if (!m_readahead.empty()) {
    m_readahead.pop_front();
    m_mark.pos++;
  }

  ReadAheadTo(0);
}

bool Stream::_ReadAheadTo(size_t i) const {
  while (m_input.good() && (m_readahead.size() <= i)) {
    switch (m_charSet) {
      case utf8:
        StreamInUtf8();
        break;
      case utf16le:
        StreamInUtf16();
        break;
      case utf16be:
        StreamInUtf16();
        break;
      case utf32le:
        StreamInUtf32();
        break;
      case utf32be:
        StreamInUtf32();
        break;
    }
  }

  // signal end of stream
  if (!m_input.good())
    m_readahead.push_back(Stream::eof());

  return m_readahead.size() > i;
}

void Stream::StreamInUtf8() const {
  unsigned char b = GetNextByte();
  if (m_input.good()) {
    m_readahead.push_back(b);
  }
}

void Stream::StreamInUtf16() const {
  unsigned long ch = 0;
  unsigned char bytes[2];
  int nBigEnd = (m_charSet == utf16be) ? 0 : 1;

  bytes[0] = GetNextByte();
  bytes[1] = GetNextByte();
  if (!m_input.good()) {
    return;
  }
  ch = (static_cast<unsigned long>(bytes[nBigEnd]) << 8) |
       static_cast<unsigned long>(bytes[1 ^ nBigEnd]);

  if (ch >= 0xDC00 && ch < 0xE000) {
    // Trailing (low) surrogate...ugh, wrong order
    QueueUnicodeCodepoint(m_readahead, CP_REPLACEMENT_CHARACTER);
    return;
  } else if (ch >= 0xD800 && ch < 0xDC00) {
    // ch is a leading (high) surrogate

    // Four byte UTF-8 code point

    // Read the trailing (low) surrogate
    for (;;) {
      bytes[0] = GetNextByte();
      bytes[1] = GetNextByte();
      if (!m_input.good()) {
        QueueUnicodeCodepoint(m_readahead, CP_REPLACEMENT_CHARACTER);
        return;
      }
      unsigned long chLow = (static_cast<unsigned long>(bytes[nBigEnd]) << 8) |
                            static_cast<unsigned long>(bytes[1 ^ nBigEnd]);
      if (chLow < 0xDC00 || chLow >= 0xE000) {
        // Trouble...not a low surrogate.  Dump a REPLACEMENT CHARACTER into the
        // stream.
        QueueUnicodeCodepoint(m_readahead, CP_REPLACEMENT_CHARACTER);

        // Deal with the next UTF-16 unit
        if (chLow < 0xD800 || chLow >= 0xE000) {
          // Easiest case: queue the codepoint and return
          QueueUnicodeCodepoint(m_readahead, ch);
          return;
        } else {
          // Start the loop over with the new high surrogate
          ch = chLow;
          continue;
        }
      }

      // Select the payload bits from the high surrogate
      ch &= 0x3FF;
      ch <<= 10;

      // Include bits from low surrogate
      ch |= (chLow & 0x3FF);

      // Add the surrogacy offset
      ch += 0x10000;
      break;
    }
  }

  QueueUnicodeCodepoint(m_readahead, ch);
}

inline char* ReadBuffer(unsigned char* pBuffer) {
  return reinterpret_cast<char*>(pBuffer);
}

unsigned char Stream::GetNextByte() const {
  if (m_nPrefetchedUsed >= m_nPrefetchedAvailable) {
    std::streambuf* pBuf = m_input.rdbuf();
    m_nPrefetchedAvailable = static_cast<std::size_t>(
        pBuf->sgetn(ReadBuffer(m_pPrefetched), YAML_PREFETCH_SIZE));
    m_nPrefetchedUsed = 0;
    if (!m_nPrefetchedAvailable) {
      m_input.setstate(std::ios_base::eofbit);
    }

    if (0 == m_nPrefetchedAvailable) {
      return 0;
    }
  }

  return m_pPrefetched[m_nPrefetchedUsed++];
}

void Stream::StreamInUtf32() const {
  static int indexes[2][4] = {{3, 2, 1, 0}, {0, 1, 2, 3}};

  unsigned long ch = 0;
  unsigned char bytes[4];
  int* pIndexes = (m_charSet == utf32be) ? indexes[1] : indexes[0];

  bytes[0] = GetNextByte();
  bytes[1] = GetNextByte();
  bytes[2] = GetNextByte();
  bytes[3] = GetNextByte();
  if (!m_input.good()) {
    return;
  }

  for (int i = 0; i < 4; ++i) {
    ch <<= 8;
    ch |= bytes[pIndexes[i]];
  }

  QueueUnicodeCodepoint(m_readahead, ch);
}
}
