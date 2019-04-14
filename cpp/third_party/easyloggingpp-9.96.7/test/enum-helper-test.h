#ifndef ENUM_HELPER_TESTS_H
#define ENUM_HELPER_TESTS_H
#include "test.h"

TEST(LevelTest, ConvertFromString) {
    EXPECT_EQ(Level::Global, LevelHelper::convertFromString("GLOBAL"));
    EXPECT_EQ(Level::Info, LevelHelper::convertFromString("INFO"));
    EXPECT_EQ(Level::Debug, LevelHelper::convertFromString("DEBUG"));
    EXPECT_EQ(Level::Warning, LevelHelper::convertFromString("WARNING"));
    EXPECT_EQ(Level::Error, LevelHelper::convertFromString("ERROR"));
    EXPECT_EQ(Level::Fatal, LevelHelper::convertFromString("FATAL"));
    EXPECT_EQ(Level::Trace, LevelHelper::convertFromString("TRACE"));
    EXPECT_EQ(Level::Verbose, LevelHelper::convertFromString("VERBOSE"));
    EXPECT_EQ(Level::Unknown, LevelHelper::convertFromString("QA"));
}

TEST(LevelTest, ConvertToString) {
    EXPECT_STRCASEEQ("GLOBAL", LevelHelper::convertToString(Level::Global));
    EXPECT_STRCASEEQ("INFO", LevelHelper::convertToString(Level::Info));
    EXPECT_STRCASEEQ("DEBUG", LevelHelper::convertToString(Level::Debug));
    EXPECT_STRCASEEQ("WARNING", LevelHelper::convertToString(Level::Warning));
    EXPECT_STRCASEEQ("ERROR", LevelHelper::convertToString(Level::Error));
    EXPECT_STRCASEEQ("FATAL", LevelHelper::convertToString(Level::Fatal));
    EXPECT_STRCASEEQ("TRACE", LevelHelper::convertToString(Level::Trace));
    EXPECT_STRCASEEQ("VERBOSE", LevelHelper::convertToString(Level::Verbose));
}

TEST(ConfigurationTypeTest, ConvertFromString) {
    EXPECT_EQ(ConfigurationType::Enabled, ConfigurationTypeHelper::convertFromString("ENABLED"));
    EXPECT_EQ(ConfigurationType::ToFile, ConfigurationTypeHelper::convertFromString("TO_FILE"));
    EXPECT_EQ(ConfigurationType::ToStandardOutput, ConfigurationTypeHelper::convertFromString("TO_STANDARD_OUTPUT"));
    EXPECT_EQ(ConfigurationType::Format, ConfigurationTypeHelper::convertFromString("FORMAT"));
    EXPECT_EQ(ConfigurationType::Filename, ConfigurationTypeHelper::convertFromString("FILENAME"));
    EXPECT_EQ(ConfigurationType::SubsecondPrecision, ConfigurationTypeHelper::convertFromString("SUBSECOND_PRECISION"));
    EXPECT_EQ(ConfigurationType::PerformanceTracking, ConfigurationTypeHelper::convertFromString("PERFORMANCE_TRACKING"));
    EXPECT_EQ(ConfigurationType::MaxLogFileSize, ConfigurationTypeHelper::convertFromString("MAX_LOG_FILE_SIZE"));
    EXPECT_EQ(ConfigurationType::LogFlushThreshold, ConfigurationTypeHelper::convertFromString("LOG_FLUSH_THRESHOLD"));
}

TEST(ConfigurationTypeTest, ConvertToString) {
    EXPECT_STRCASEEQ("ENABLED", ConfigurationTypeHelper::convertToString(ConfigurationType::Enabled));
    EXPECT_STRCASEEQ("TO_FILE", ConfigurationTypeHelper::convertToString(ConfigurationType::ToFile));
    EXPECT_STRCASEEQ("TO_STANDARD_OUTPUT", ConfigurationTypeHelper::convertToString(ConfigurationType::ToStandardOutput));
    EXPECT_STRCASEEQ("FORMAT", ConfigurationTypeHelper::convertToString(ConfigurationType::Format));
    EXPECT_STRCASEEQ("FILENAME", ConfigurationTypeHelper::convertToString(ConfigurationType::Filename));
    EXPECT_STRCASEEQ("SUBSECOND_PRECISION", ConfigurationTypeHelper::convertToString(ConfigurationType::SubsecondPrecision));
    EXPECT_STRCASEEQ("PERFORMANCE_TRACKING", ConfigurationTypeHelper::convertToString(ConfigurationType::PerformanceTracking));
    EXPECT_STRCASEEQ("MAX_LOG_FILE_SIZE", ConfigurationTypeHelper::convertToString(ConfigurationType::MaxLogFileSize));
    EXPECT_STRCASEEQ("LOG_FLUSH_THRESHOLD", ConfigurationTypeHelper::convertToString(ConfigurationType::LogFlushThreshold));
}
#endif // ENUM_HELPER_TESTS_H
