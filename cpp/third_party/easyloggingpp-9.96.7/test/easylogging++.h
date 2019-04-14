// Header for test that sub-includes original header from src/ folder
#ifndef EASYLOGGING_FOR_TEST_H
#define EASYLOGGING_FOR_TEST_H

// We define these macros here for travis to pick up
#define ELPP_STOP_ON_FIRST_ASSERTION
#define ELPP_STL_LOGGING
#define ELPP_FORCE_ENV_VAR_FROM_BASH
#define ELPP_ENABLE_THREADING
#define ELPP_FEATURE_CRASH_LOG
#define ELPP_SYSLOG
#include "../src/easylogging++.h"
#endif // EASYLOGGING_FOR_SAMPLES_H
