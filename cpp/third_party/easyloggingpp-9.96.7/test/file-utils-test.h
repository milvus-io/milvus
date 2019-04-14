#ifndef FILE_UTILS_TEST_H
#define FILE_UTILS_TEST_H

#include "test.h"

static const char* filename = "/tmp/files_utils_test";
static el::base::type::fstream_t* fs;

TEST(FileUtilsTest, NewFileStream) {
    fs = File::newFileStream(filename);
    EXPECT_NE(nullptr, fs);
    EXPECT_TRUE(fs->is_open());
    cleanFile(filename, fs);
}

TEST(FileUtilsTest, GetSizeOfFile) {
    EXPECT_EQ(File::getSizeOfFile(fs), 0);
    const char* data = "123";
    (*fs) << data;
    fs->flush();
    EXPECT_EQ(File::getSizeOfFile(fs), strlen(data));
}

#if !ELPP_OS_EMSCRIPTEN
// this doesn't work as expected under emscripten's filesystem emulation
TEST(FileUtilsTest, PathExists) {
    EXPECT_TRUE(File::pathExists(filename));
    removeFile(filename);
    EXPECT_FALSE(File::pathExists(filename));
}
#endif

TEST(FileUtilsTest, ExtractPathFromFilename) {
    EXPECT_EQ("/this/is/path/on/unix/", File::extractPathFromFilename("/this/is/path/on/unix/file.txt"));
    EXPECT_EQ("C:\\this\\is\\path\\on\\win\\", File::extractPathFromFilename("C:\\this\\is\\path\\on\\win\\file.txt", "\\"));
}

TEST(FileUtilsTest, CreatePath) {
    const char* path = "/tmp/my/one/long/path";
#if !ELPP_OS_EMSCRIPTEN
    // it'll be reported as existing in emscripten
    EXPECT_FALSE(File::pathExists(path));
#endif
    EXPECT_TRUE(File::createPath(path));
    EXPECT_TRUE(File::pathExists(path));
    removeFile(path);

#if !ELPP_OS_EMSCRIPTEN
    EXPECT_FALSE(File::pathExists(path));
#endif
}


TEST(FileUtilsTest, BuildStrippedFilename) {

    char buf[50] = "";

    File::buildStrippedFilename("this_is_myfile.cc", buf, 50);
    EXPECT_STREQ("this_is_myfile.cc", buf);

    Str::clearBuff(buf, 20);
    EXPECT_STREQ("", buf);

    File::buildStrippedFilename("this_is_myfilename_with_more_than_50_characters.cc", buf, 50);
    EXPECT_STREQ("..s_is_myfilename_with_more_than_50_characters.cc", buf);
}

#endif // FILE_UTILS_TEST_H
