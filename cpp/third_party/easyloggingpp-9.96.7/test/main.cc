// gtest based unit tests

#include "test.h"

// Included in alphabetical order

#include "command-line-args-test.h"
#include "configurations-test.h"
#include "custom-format-specifier-test.h"
#include "enum-helper-test.h"
#include "date-utils-test.h"
#include "file-utils-test.h"
#include "format-specifier-test.h"
#include "global-configurations-test.h"
#include "helpers-test.h"
#include "hit-counter-test.h"
#include "log-format-resolution-test.h"
#include "loggable-test.h"
#include "logger-test.h"
#include "macros-test.h"
#include "os-utils-test.h"
#include "plog-test.h"
#include "post-log-dispatch-handler-test.h"
#include "registry-test.h"
#include "strict-file-size-check-test.h"
#include "string-utils-test.h"
#include "syslog-test.h"
#include "typed-configurations-test.h"
#include "utilities-test.h"
#include "verbose-app-arguments-test.h"
#include "write-all-test.h"

TIMED_SCOPE(testTimer, "Easylogging++ Unit Tests");

int main(int argc, char** argv) {


    testing::InitGoogleTest(&argc, argv);

    ELPP_INITIALIZE_SYSLOG(kSysLogIdent, 0, 0);

    reconfigureLoggersForTest();
    std::cout << "Logs for test are written in [" << logfile << "]" << std::endl;

    return ::testing::UnitTest::GetInstance()->Run();
}
