#include "AwsSegcoreLogger.h"
#include "log/Log.h"

AwsSegcoreLogger::AwsSegcoreLogger(LogLevel log_level) : log_level_(log_level) {
}

LogLevel
AwsSegcoreLogger::GetLogLevel() const {
    return log_level_;
}

void
AwsSegcoreLogger::Log(LogLevel logLevel,
                      const char* tag,
                      const char* formatStr,
                      ...) {
    switch (logLevel) {
        case LogLevel::Trace:
            LOG_SEGCORE_TRACE_ << tag << " " << formatStr << std::endl;
            break;
        case LogLevel::Debug:
            LOG_SEGCORE_DEBUG_ << tag << " " << formatStr << std::endl;
            break;
        case LogLevel::Info:
            LOG_SEGCORE_INFO_ << tag << " " << formatStr << std::endl;
            break;
        case LogLevel::Warn:
            LOG_SEGCORE_WARNING_ << tag << " " << formatStr << std::endl;
            break;
        case LogLevel::Error:
            LOG_SEGCORE_ERROR_ << tag << " " << formatStr << std::endl;
            break;
        case LogLevel::Fatal:
            LOG_SEGCORE_FATAL_ << tag << " " << formatStr << std::endl;
            break;
        default:
            break;
    }
}

void
AwsSegcoreLogger::LogStream(LogLevel logLevel,
                            const char* tag,
                            const Aws::OStringStream& messageStream) {
    switch (logLevel) {
        case LogLevel::Trace:
            LOG_SEGCORE_TRACE_ << tag << " " << messageStream.str()
                               << std::endl;
            break;
        case LogLevel::Debug:
            LOG_SEGCORE_DEBUG_ << tag << " " << messageStream.str()
                               << std::endl;
            break;
        case LogLevel::Info:
            LOG_SEGCORE_INFO_ << tag << " " << messageStream.str() << std::endl;
            break;
        case LogLevel::Warn:
            LOG_SEGCORE_WARNING_ << tag << " " << messageStream.str()
                                 << std::endl;
            break;
        case LogLevel::Error:
            LOG_SEGCORE_ERROR_ << tag << " " << messageStream.str()
                               << std::endl;
            break;
        case LogLevel::Fatal:
            LOG_SEGCORE_FATAL_ << tag << " " << messageStream.str()
                               << std::endl;
            break;
        default:
            break;
    }
}

void
AwsSegcoreLogger::Flush() {
}