#include <sstream>

#include "handler_test.h"
#include "yaml-cpp/yaml.h"  // IWYU pragma: keep

#include "gtest/gtest.h"

using ::testing::_;
using ::testing::InSequence;

namespace YAML {
namespace {
typedef void (*EncodingFn)(std::ostream&, int);

inline char Byte(int ch) {
  return static_cast<char>(
      static_cast<unsigned char>(static_cast<unsigned int>(ch)));
}

void EncodeToUtf8(std::ostream& stream, int ch) {
  if (ch <= 0x7F) {
    stream << Byte(ch);
  } else if (ch <= 0x7FF) {
    stream << Byte(0xC0 | (ch >> 6));
    stream << Byte(0x80 | (ch & 0x3F));
  } else if (ch <= 0xFFFF) {
    stream << Byte(0xE0 | (ch >> 12));
    stream << Byte(0x80 | ((ch >> 6) & 0x3F));
    stream << Byte(0x80 | (ch & 0x3F));
  } else if (ch <= 0x1FFFFF) {
    stream << Byte(0xF0 | (ch >> 18));
    stream << Byte(0x80 | ((ch >> 12) & 0x3F));
    stream << Byte(0x80 | ((ch >> 6) & 0x3F));
    stream << Byte(0x80 | (ch & 0x3F));
  }
}

bool SplitUtf16HighChar(std::ostream& stream, EncodingFn encoding, int ch) {
  int biasedValue = ch - 0x10000;
  if (biasedValue < 0) {
    return false;
  }
  int high = 0xD800 | (biasedValue >> 10);
  int low = 0xDC00 | (biasedValue & 0x3FF);
  encoding(stream, high);
  encoding(stream, low);
  return true;
}

void EncodeToUtf16LE(std::ostream& stream, int ch) {
  if (!SplitUtf16HighChar(stream, &EncodeToUtf16LE, ch)) {
    stream << Byte(ch & 0xFF) << Byte(ch >> 8);
  }
}

void EncodeToUtf16BE(std::ostream& stream, int ch) {
  if (!SplitUtf16HighChar(stream, &EncodeToUtf16BE, ch)) {
    stream << Byte(ch >> 8) << Byte(ch & 0xFF);
  }
}

void EncodeToUtf32LE(std::ostream& stream, int ch) {
  stream << Byte(ch & 0xFF) << Byte((ch >> 8) & 0xFF) << Byte((ch >> 16) & 0xFF)
         << Byte((ch >> 24) & 0xFF);
}

void EncodeToUtf32BE(std::ostream& stream, int ch) {
  stream << Byte((ch >> 24) & 0xFF) << Byte((ch >> 16) & 0xFF)
         << Byte((ch >> 8) & 0xFF) << Byte(ch & 0xFF);
}

class EncodingTest : public HandlerTest {
 protected:
  void SetUpEncoding(EncodingFn encoding, bool declareEncoding) {
    if (declareEncoding) {
      encoding(m_yaml, 0xFEFF);
    }

    AddEntry(encoding, 0x0021, 0x007E);  // Basic Latin
    AddEntry(encoding, 0x00A1, 0x00FF);  // Latin-1 Supplement
    AddEntry(encoding, 0x0660, 0x06FF);  // Arabic (largest contiguous block)

    // CJK unified ideographs (multiple lines)
    AddEntry(encoding, 0x4E00, 0x4EFF);
    AddEntry(encoding, 0x4F00, 0x4FFF);
    AddEntry(encoding, 0x5000, 0x51FF);  // 512 character line
    AddEntry(encoding, 0x5200, 0x54FF);  // 768 character line
    AddEntry(encoding, 0x5500, 0x58FF);  // 1024 character line

    AddEntry(encoding, 0x103A0, 0x103C3);  // Old Persian

    m_yaml.seekg(0, std::ios::beg);
  }

  void Run() {
    InSequence sequence;
    EXPECT_CALL(handler, OnDocumentStart(_));
    EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
    for (std::size_t i = 0; i < m_entries.size(); i++) {
      EXPECT_CALL(handler, OnScalar(_, "!", 0, m_entries[i]));
    }
    EXPECT_CALL(handler, OnSequenceEnd());
    EXPECT_CALL(handler, OnDocumentEnd());

    Parse(m_yaml.str());
  }

 private:
  std::stringstream m_yaml;
  std::vector<std::string> m_entries;

  void AddEntry(EncodingFn encoding, int startCh, int endCh) {
    encoding(m_yaml, '-');
    encoding(m_yaml, ' ');
    encoding(m_yaml, '|');
    encoding(m_yaml, '\n');
    encoding(m_yaml, ' ');
    encoding(m_yaml, ' ');

    std::stringstream entry;
    for (int ch = startCh; ch <= endCh; ++ch) {
      encoding(m_yaml, ch);
      EncodeToUtf8(entry, ch);
    }
    encoding(m_yaml, '\n');
    EncodeToUtf8(entry, '\n');

    m_entries.push_back(entry.str());
  }
};

TEST_F(EncodingTest, UTF8_noBOM) {
  SetUpEncoding(&EncodeToUtf8, false);
  Run();
}

TEST_F(EncodingTest, UTF8_BOM) {
  SetUpEncoding(&EncodeToUtf8, true);
  Run();
}

TEST_F(EncodingTest, UTF16LE_noBOM) {
  SetUpEncoding(&EncodeToUtf16LE, false);
  Run();
}

TEST_F(EncodingTest, UTF16LE_BOM) {
  SetUpEncoding(&EncodeToUtf16LE, true);
  Run();
}

TEST_F(EncodingTest, UTF16BE_noBOM) {
  SetUpEncoding(&EncodeToUtf16BE, false);
  Run();
}

TEST_F(EncodingTest, UTF16BE_BOM) {
  SetUpEncoding(&EncodeToUtf16BE, true);
  Run();
}

TEST_F(EncodingTest, UTF32LE_noBOM) {
  SetUpEncoding(&EncodeToUtf32LE, false);
  Run();
}

TEST_F(EncodingTest, UTF32LE_BOM) {
  SetUpEncoding(&EncodeToUtf32LE, true);
  Run();
}

TEST_F(EncodingTest, UTF32BE_noBOM) {
  SetUpEncoding(&EncodeToUtf32BE, false);
  Run();
}

TEST_F(EncodingTest, UTF32BE_BOM) {
  SetUpEncoding(&EncodeToUtf32BE, true);
  Run();
}
}
}
