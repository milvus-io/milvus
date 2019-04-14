//
// This file is part of Easylogging++ samples
//
// Demonstration of multithreaded application using pthread
// 
// Compile this program using (if using gcc-c++ or intel or clang++):
//     [icpc | g++ | clang++] ./pthread.cpp -o bin/./pthread.cpp.bin -DELPP_THREAD_SAFE -std=c++0x -pthread -Wall -Wextra
// 
// Revision: 1.1
// @author mkhan3189
//

#include <pthread.h>
#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP
class MyHandler : public el::LogDispatchCallback {
public:
    void handle(const el::LogDispatchData*) {
        std::cout << "Test MyHandler " << std::endl;
    }
};


struct Args {
  const char* thrId;
  el::Logger* logger;
}args;

void* write2(void* args){
  struct Args* a = (struct Args*)args;
  
  char* threadId = (char*)a->thrId;
  el::Logger* logger = (el::Logger*)a->logger;
  CLOG(INFO, "default", "network") << "Triggering network log from default and network";

  LOG(INFO) << "Writing from different function using macro [Thread #" << threadId << "]";

  logger->info("Info log from [Thread #%v]", threadId);
  logger->info("Info log");
  logger->verbose(2, "Verbose test [Thread #%v]", threadId);
  logger->verbose(2, "Verbose test");


  CLOG(INFO, "default", "network") << "Triggering network log from default and network";
  return NULL;
}

void *write(void* thrId){
  char* threadId = (char*)thrId;
  // Following line will be logged with every thread
  LOG(INFO) << "This standard log is written by [Thread #" << threadId << "]";
  // Following line will be logged with every thread only when --v=2 argument 
  // is provided, i.e, ./bin/multithread_test.cpp.bin --v=2
  VLOG(2) << "This is verbose level 2 logging from [Thread #" << threadId << "]";

  // Following line will be logged only once from second running thread (which every runs second into 
  // this line because of interval 2)
  LOG_EVERY_N(2, WARNING) << "This will be logged only once from thread who every reaches this line first. Currently running from [Thread #" << threadId << "]";

  for (int i = 1; i <= 10; ++i) {
     VLOG_IF(true, 2) << "Verbose condition [Thread #" << threadId << "]";
     VLOG_EVERY_N(2, 3) << "Verbose level 3 log every 4th time. This is at " << i << " from [Thread #" << threadId << "]";
  }

  // Following line will be logged once with every thread because of interval 1 
  LOG_EVERY_N(1, INFO) << "This interval log will be logged with every thread, this one is from [Thread #" << threadId << "]";

  LOG_IF(strcmp(threadId, "2") == 0, INFO) << "This log is only for thread 2 and is ran by [Thread #" << threadId << "]";
  
  // Register 5 vague loggers
  for (int i = 1; i <= 5; ++i) {
     std::stringstream ss;
     ss << "logger" << i;
     el::Logger* logger = el::Loggers::getLogger(ss.str());
     LOG(INFO) << "Registered logger [" << *logger << "] [Thread #" << threadId << "]";
     CLOG(INFO, "default", "network") << "Triggering network log from default and network";
  }
  CLOG(INFO, "logger1") << "Logging using new logger [Thread #" << threadId << "]";
  CLOG(INFO, "no-logger") << "THIS SHOULD SAY LOGGER NOT REGISTERED YET [Thread #" << threadId << "]"; // << -- NOTE THIS!
  CLOG(INFO, "default", "network") << "Triggering network log from default and network";

  el::Logger* logger = el::Loggers::getLogger("default");
  logger->info("Info log from [Thread #%v]", threadId);

  // Check for log counters positions
  for (int i = 1; i <= 50; ++i) {
     LOG_EVERY_N(2, INFO) << "Counter pos: " << ELPP_COUNTER_POS << " [Thread #" << threadId << "]";
  }
  LOG_EVERY_N(2, INFO) << "Counter pos: " << ELPP_COUNTER_POS << " [Thread #" << threadId << "]";
  return NULL;
}

// If you wish you can define your own way to get thread ID
const char* getThreadId_CustomVersion(const el::LogMessage*) {
    std::stringstream ss;
    ss << pthread_self();
    return ss.str().c_str();
}

int main(int argc, char** argv)
{
     START_EASYLOGGINGPP(argc, argv);

      el::Helpers::installLogDispatchCallback<MyHandler>("MyHandler");

     // Your thread ID specification
     el::CustomFormatSpecifier myThreadIdSpecifier("%mythreadId", getThreadId_CustomVersion);
     el::Helpers::installCustomFormatSpecifier(myThreadIdSpecifier);

     el::Loggers::addFlag(el::LoggingFlag::MultiLoggerSupport);

     // Note your %mythreadId or built-in, both are logged
     el::Loggers::reconfigureAllLoggers(el::ConfigurationType::Format, "%datetime %level (%thread | %mythreadId) [%logger] [%func] [%loc] %msg");
     el::Loggers::reconfigureAllLoggers(el::Level::Verbose, el::ConfigurationType::Format, "%datetime %level-%vlevel (%thread | %mythreadId) [%logger] [%func] [%loc] %msg");
     el::Loggers::getLogger("network");

     pthread_t thread1, thread2, thread3, thread4;

     // Create independent threads each of which will execute function 
     pthread_create( &thread1, NULL, write, (void*)"1");
     pthread_create( &thread2, NULL, write, (void*)"2");
     pthread_create( &thread3, NULL, write, (void*)"3");
     
     el::Logger* logger = el::Loggers::getLogger("default");
     args.thrId = "4";
     args.logger = logger;
     pthread_create( &thread4, NULL, write2, (void*)&args);

     pthread_join(thread1, NULL);
     pthread_join(thread2, NULL); 
     pthread_join(thread3, NULL); 
     pthread_join(thread4, NULL); 

#if 0 // Change this to 1 for some serious multiple threads
    int i = 5; // Last one we created was 4 so we dont want to confuse
    const int max = i + 500;
    for (; i <= max; ++i) {
        pthread_t thread;
        std::string s = std::to_string(static_cast<long long>(i));
        if (i % 2 == 0)
            pthread_create( &thread, NULL, write, (void*)s.c_str());
        else {
            args.thrId = s.c_str();
            args.logger = logger;
            pthread_create( &thread, NULL, write2, (void*)&args);
        }
        pthread_join(thread, NULL); 
    }
#endif

    exit(0);
}
