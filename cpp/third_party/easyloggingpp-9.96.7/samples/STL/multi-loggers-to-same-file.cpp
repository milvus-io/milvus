//
// This file is part of Easylogging++ samples
//
// Revision 1.0
//

#include <thread>
#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

void def() {
    for (int i = 0; i < 1000; ++i)
        CLOG(INFO, "first") << "This is from first " << i;
}

void second() {
    for (int i = 0; i < 1000; ++i)
        CLOG(INFO, "second") << "This is from second" << i;
}

int main(int,char**){
    el::Loggers::addFlag(el::LoggingFlag::CreateLoggerAutomatically);

    el::Configurations confFromFile("../default-logger.conf");

    el::Loggers::setDefaultConfigurations(confFromFile, true);

    LOG(INFO)<<"The program has started!";
    
    std::thread t1(def);
    std::thread t2(second);
    
    t1.join();
    t2.join();
    
    LOG(INFO) << "Shutting down.";
    return 0;
}
