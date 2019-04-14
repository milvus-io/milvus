
#include "easylogging++.h"
#include "mymath.h"

INITIALIZE_EASYLOGGINGPP

TIMED_SCOPE(benchmark, "benchmark-program");

int main(int argc, char *argv[])
{
    // ELPP_INITIALIZE_SYSLOG("my_proc", LOG_PID | LOG_CONS | LOG_PERROR, LOG_USER);
    el::Loggers::reconfigureAllLoggers(el::ConfigurationType::ToStandardOutput, "false");
    TIMED_BLOCK(loggingPerformance, "benchmark-block") {
        el::base::SubsecondPrecision ssPrec(3);
        std::cout << "Starting program " << el::base::utils::DateTime::getDateTime("%h:%m:%s", &ssPrec) << std::endl;
        int MAX_LOOP = 1000000;
        for (int i = 0; i <= MAX_LOOP; ++i) {
            LOG(INFO) << "Log message " << i;
        }
        int result = MyMath::sum(1, 2);
        result = MyMath::sum(1, 3);

        std::cout << "Finished program - cleaning! " << el::base::utils::DateTime::getDateTime("%h:%m:%s", &ssPrec) << std::endl;
    }
    // SYSLOG(INFO) << "This is syslog - read it from /var/log/syslog";

    return 0;
}
