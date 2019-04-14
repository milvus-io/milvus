 //
 // This file is part of Easylogging++ samples
 // Demonstrates how to use log dispatch callback
 //
 // Revision 1.0
 // @author mkhan3189
 //

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

class MyHandler : public el::LogDispatchCallback {
public:
    void handle(const el::LogDispatchData*) {
        // NEVER DO LOG FROM HANDLER!
        // LOG(INFO) << "Test MyHandler " << msg;
    }
};

class MyHtmlHandler : public el::LogDispatchCallback {
public:
    MyHtmlHandler() {
        el::Loggers::getLogger("html"); // register
    }
    void handle(const el::LogDispatchData*) {
        // NEVER DO LOG FROM HANDLER!
       // CLOG(INFO, "html") << data->logMessage()->message();
       std::cout << "Test handler" << std::endl;
    }
};

int main(void) {
    
    el::Helpers::installLogDispatchCallback<MyHandler>("MyHandler");
    el::Helpers::installLogDispatchCallback<MyHtmlHandler>("MyHtmlHandler");

    LOG(INFO) << "My first log message";
    LOG(INFO) << "My second log message";

    return 0;
}
