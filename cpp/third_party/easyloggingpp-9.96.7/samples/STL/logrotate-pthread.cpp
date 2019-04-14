//
// This file is part of Easylogging++ samples
//
// Sample to show how to implement log rotate using easylogging++ (posix thread version)
// Thanks to Darren for efforts (http://darrendev.blogspot.com.au/)
//
// Compile: g++ -std=c++11 -Wall -Werror logrotate.cpp -lpthread -o logrotate -DELPP_THREAD_SAFE
//
// Revision 1.1
// @author mkhan3189
//

#define ELPP_NO_DEFAULT_LOG_FILE
#include "easylogging++.h"
#include <unistd.h> // for sleep()

INITIALIZE_EASYLOGGINGPP

void* logRotate(void*){
	// Rotate every 20 seconds
	while (true){
		sleep(20);
		LOG(INFO) << "About to rotate log file!";
		el::Loggers::getLogger("default")->reconfigure();
	}
	return NULL;
}

int main(int, char**){
    el::Loggers::configureFromGlobal("logrotate.conf");
    LOG(INFO) << "The program has started!";
    
    pthread_t logRotatorThread;
	pthread_create(&logRotatorThread, NULL, logRotate, NULL);
    
    //Main thread
    for(int n = 0; n < 60; ++n){
        LOG(TRACE) << n;
        sleep(1);
    }
    
    LOG(INFO) << "Shutting down.";
    return 0;
}
