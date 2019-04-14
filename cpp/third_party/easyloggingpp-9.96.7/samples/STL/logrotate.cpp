//
// This file is part of Easylogging++ samples
//
// Sample to show how to implement log rotate using easylogging++
// Thanks to Darren for efforts (http://darrendev.blogspot.com.au/)
//
// Compile: g++ -std=c++11 -Wall -Werror logrotate.cpp -lpthread -o logrotate -DELPP_THREAD_SAFE
//
// Revision 1.0
// @author Darren
//

#include <thread>
#define ELPP_NO_DEFAULT_LOG_FILE
#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

int main(int,char**){
    el::Loggers::configureFromGlobal("logrotate.conf");
    LOG(INFO)<<"The program has started!";
    
    std::thread logRotatorThread([](){
        const std::chrono::seconds wakeUpDelta = std::chrono::seconds(20);
        auto nextWakeUp = std::chrono::system_clock::now() + wakeUpDelta;
        
        while(true){
            std::this_thread::sleep_until(nextWakeUp);
            nextWakeUp += wakeUpDelta;
            LOG(INFO) << "About to rotate log file!";
            auto L = el::Loggers::getLogger("default");
            if(L == nullptr)LOG(ERROR)<<"Oops, it is not called default!";
            else L->reconfigure();
        }
        
    });
    
    logRotatorThread.detach();
    
    //Main thread
    for(int n = 0; n < 1000; ++n){
        LOG(TRACE) << n;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    LOG(INFO) << "Shutting down.";
    return 0;
}
