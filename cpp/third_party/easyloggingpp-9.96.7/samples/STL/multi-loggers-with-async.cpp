//
// This file is part of Easylogging++ samples
//
// Revision 1.0
//

#include <chrono>
#include <thread>
#include <future>
#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

void f() {
    std::async(std::launch::async, [&]() {
        std::cout << "[internal] inside async()" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(1));
        LOG(INFO) << "This is from async";
    });
}

class MyHandler : public el::LogDispatchCallback {
public:
    void handle(const el::LogDispatchData* d) {
        std::cout << "Message: " << d->logMessage()->message() << " [logger: " << d->logMessage()->logger()->id() << "]" << std::endl;
        if (d->logMessage()->logger()->id() != "default") {
            std::cout << "[internal] calling f()" << std::endl;
            f();
        }
    }
};



int main(int,char**){
    el::Loggers::addFlag(el::LoggingFlag::CreateLoggerAutomatically);
//    el::Helpers::uninstallLogDispatchCallback<el::base::DefaultLogDispatchCallback>("DefaultLogDispatchCallback");
    el::Helpers::installLogDispatchCallback<MyHandler>("MyHandler");

    LOG(INFO)<<"The program has started!";
    CLOG(INFO, "frommain") << "THis is another";
    
    std::thread t1([](){
        LOG(INFO) << "This is from thread";
    });
    
    t1.join();
    
    LOG(INFO) << "Shutting down.";
    return 0;
}
