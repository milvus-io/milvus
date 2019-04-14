 //
 // This file is part of Easylogging++ samples
 // LoggerRegistrationCallback sample
 //
 // Revision 1.0
 // @author mkhan3189
 //

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP
class Handler : public el::LoggerRegistrationCallback {
protected:
void handle(const el::Logger* logger) {
    // Never log anything here
    ELPP_COUT << "(Handler) Registered new logger " << logger->id() << std::endl;
}
};

class Handler2 : public el::LoggerRegistrationCallback {
protected:
void handle(const el::Logger* logger) {
    ELPP_COUT << "(Handler2) Registered new logger " << logger->id() << std::endl;
}
};

int main(void) {

    el::Loggers::installLoggerRegistrationCallback<Handler>("handler");
    el::Loggers::installLoggerRegistrationCallback<Handler2>("handler2");

    LOG(INFO) << "Now we will register three loggers";

    el::Loggers::getLogger("logger1");
    el::Loggers::getLogger("logger2");
    el::Loggers::getLogger("logger3");
    return 0;
}
