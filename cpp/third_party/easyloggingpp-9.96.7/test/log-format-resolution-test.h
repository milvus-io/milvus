#ifndef LOG_FORMAT_RESOLUTION_TEST_H
#define LOG_FORMAT_RESOLUTION_TEST_H

TEST(LogFormatResolutionTest, NormalFormat) {

    LogFormat format(Level::Info, ELPP_LITERAL("%logger %thread"));
    EXPECT_EQ(ELPP_LITERAL("%logger %thread"), format.userFormat());
    EXPECT_EQ(ELPP_LITERAL("%logger %thread"), format.format());
    EXPECT_EQ("", format.dateTimeFormat());

    LogFormat format2(Level::Info, ELPP_LITERAL("%logger %datetime{%Y-%M-%d %h:%m:%s  } %thread"));
    EXPECT_EQ(ELPP_LITERAL("%logger %datetime{%Y-%M-%d %h:%m:%s  } %thread"), format2.userFormat());
    EXPECT_EQ(ELPP_LITERAL("%logger %datetime %thread"), format2.format());
    EXPECT_EQ("%Y-%M-%d %h:%m:%s  ", format2.dateTimeFormat());

    LogFormat format3(Level::Info, ELPP_LITERAL("%logger %datetime{%Y-%M-%d} %thread"));
    EXPECT_EQ(ELPP_LITERAL("%logger %datetime{%Y-%M-%d} %thread"), format3.userFormat());
    EXPECT_EQ(ELPP_LITERAL("%logger %datetime %thread"), format3.format());
    EXPECT_EQ("%Y-%M-%d", format3.dateTimeFormat());
}

TEST(LogFormatResolutionTest, DefaultFormat) {

    LogFormat defaultFormat(Level::Info, ELPP_LITERAL("%logger %datetime %thread"));
    EXPECT_EQ(ELPP_LITERAL("%logger %datetime %thread"), defaultFormat.userFormat());
    EXPECT_EQ(ELPP_LITERAL("%logger %datetime %thread"), defaultFormat.format());
    EXPECT_EQ("%Y-%M-%d %H:%m:%s,%g", defaultFormat.dateTimeFormat());

    LogFormat defaultFormat2(Level::Info, ELPP_LITERAL("%logger %datetime %thread"));
    EXPECT_EQ(ELPP_LITERAL("%logger %datetime %thread"), defaultFormat2.userFormat());
    EXPECT_EQ(ELPP_LITERAL("%logger %datetime %thread"), defaultFormat2.format());
    EXPECT_EQ("%Y-%M-%d %H:%m:%s,%g", defaultFormat2.dateTimeFormat());

    LogFormat defaultFormat4(Level::Verbose, ELPP_LITERAL("%logger %level-%vlevel %datetime %thread"));
    EXPECT_EQ(ELPP_LITERAL("%logger %level-%vlevel %datetime %thread"), defaultFormat4.userFormat());
    EXPECT_EQ(ELPP_LITERAL("%logger VERBOSE-%vlevel %datetime %thread"), defaultFormat4.format());
    EXPECT_EQ("%Y-%M-%d %H:%m:%s,%g", defaultFormat4.dateTimeFormat());
}

TEST(LogFormatResolutionTest, EscapedFormat) {

    SubsecondPrecision ssPrec(3);

    LogFormat escapeTest(Level::Info, ELPP_LITERAL("%logger %datetime{%%H %H} %thread"));
    EXPECT_EQ(ELPP_LITERAL("%logger %datetime{%%H %H} %thread"), escapeTest.userFormat());
    EXPECT_EQ(ELPP_LITERAL("%logger %datetime %thread"), escapeTest.format());
    EXPECT_EQ("%%H %H", escapeTest.dateTimeFormat());
    EXPECT_TRUE(Str::startsWith(DateTime::getDateTime(escapeTest.dateTimeFormat().c_str(), &ssPrec), "%H"));

    LogFormat escapeTest2(Level::Info, ELPP_LITERAL("%%logger %%datetime{%%H %H %%H} %%thread %thread %%thread"));
    EXPECT_EQ(ELPP_LITERAL("%%logger %%datetime{%%H %H %%H} %%thread %thread %%thread"), escapeTest2.userFormat());
    EXPECT_EQ(ELPP_LITERAL("%%logger %%datetime{%%H %H %%H} %%thread %thread %thread"), escapeTest2.format());
    EXPECT_EQ("", escapeTest2.dateTimeFormat()); // Date/time escaped
    EXPECT_TRUE(Str::startsWith(DateTime::getDateTime(escapeTest.dateTimeFormat().c_str(), &ssPrec), "%H"));

    LogFormat escapeTest3(Level::Info, ELPP_LITERAL("%%logger %datetime{%%H %H %%H} %%thread %thread %%thread"));
    EXPECT_EQ(ELPP_LITERAL("%%logger %datetime{%%H %H %%H} %%thread %thread %%thread"), escapeTest3.userFormat());
    EXPECT_EQ(ELPP_LITERAL("%%logger %datetime %%thread %thread %thread"), escapeTest3.format());
    EXPECT_EQ("%%H %H %%H", escapeTest3.dateTimeFormat()); // Date/time escaped
    EXPECT_TRUE(Str::startsWith(DateTime::getDateTime(escapeTest.dateTimeFormat().c_str(), &ssPrec), "%H"));
}

#endif // LOG_FORMAT_RESOLUTION_TEST_H
