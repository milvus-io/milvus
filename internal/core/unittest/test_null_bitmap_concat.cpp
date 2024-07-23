#include <gtest/gtest.h>
#include "common/NullBitmapConcatenater.h"

TEST(null_bitmap_concat_test, test_concatenate) {
    milvus::NullBitmapConcatenater c;
    uint8_t bitmap1[1] = {0b00000001};
    size_t bitmap1_size = 8;
    c.AddBitmap(bitmap1, bitmap1_size);
    c.AddBitmap(bitmap1, bitmap1_size);
    c.ConcatenateBitmaps();
    auto c_bitmap = c.GetConcatenatedBitmap();
    EXPECT_EQ(2 * bitmap1_size, c_bitmap.second);
    std::string s1(reinterpret_cast<char*>(c_bitmap.first),
                   (c_bitmap.second + 7) / 8);
    std::string s2 = "\x01\x01";
    EXPECT_EQ(s1, s2);

    c.Clear();
    uint8_t bitmap2[2] = {0b00000001, 0b11111111};
    size_t bitmap2_size = 10;
    c.AddBitmap(bitmap2, bitmap2_size);
    c.AddBitmap(bitmap1, bitmap1_size);
    c.ConcatenateBitmaps();
    c_bitmap = c.GetConcatenatedBitmap();
    EXPECT_EQ(bitmap2_size + bitmap1_size, c_bitmap.second);
    s1 = {reinterpret_cast<char*>(c_bitmap.first), (c_bitmap.second + 7) / 8};
    s2 = "\x01\xc0\x40";  // 00000001 11000000 01000000
    EXPECT_EQ(s1, s2);
}