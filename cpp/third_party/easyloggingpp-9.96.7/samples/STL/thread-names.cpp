//
// This file is part of Easylogging++ samples
//
// Demo for thread names
//
// Compile: g++ -std=c++11 -Wall -Werror thread-names.cpp -lpthread -o thread-names -DELPP_THREAD_SAFE
//
// Revision 1.0
// @author mkhan3189
//

#include <thread>
#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

int main(int,char**){
    el::Loggers::reconfigureAllLoggers(el::ConfigurationType::Format, "%datetime [%thread_name] %msg");
    LOG(INFO)<<"The program has started! (no thread name)";
    el::Helpers::setThreadName("main-thread");
    
    std::thread thread1([](){
		LOG(INFO) << "Setting thread name for thread1";
        el::Helpers::setThreadName("thread1");
		LOG(INFO) << "Done setting thread name for thread1";
        for (int i = 0; i < 100; ++i) {
            LOG(INFO) << "Current i = " << i << " from thread1";
        }
    });

    std::thread thread2([](){
		LOG(INFO) << "Setting thread name for thread2";
        el::Helpers::setThreadName("thread2");
		LOG(INFO) << "Done setting thread name for thread2";
        for (int i = 0; i < 100; ++i) {
            LOG(INFO) << "Current i = " << i << " from thread2";
        }
    });
    
    thread1.join();
    thread2.join();
    
    LOG(INFO) << "Shutting down.";
    return 0;
}
