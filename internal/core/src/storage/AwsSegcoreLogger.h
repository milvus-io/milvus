#pragma once

#include <aws/core/utils/logging/LogSystemInterface.h>
#include <aws/core/utils/logging/LogLevel.h>

using namespace Aws::Utils::Logging;
class AwsSegcoreLogger : public LogSystemInterface {
 public:
    AwsSegcoreLogger(LogLevel log_level);
    virtual LogLevel
    GetLogLevel(void) const override;
    virtual void
    Log(LogLevel logLevel,
        const char* tag,
        const char* formatStr,
        ...) override;
    virtual void
    LogStream(LogLevel logLevel,
              const char* tag,
              const Aws::OStringStream& messageStream) override;
    virtual void
    Flush() override;

 private:
    LogLevel log_level_;
};
