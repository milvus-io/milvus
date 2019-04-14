#ifndef MYTHREAD_H
#define MYTHREAD_H

#include <QThread>
#include "easylogging++.h"

class MyThread : public QThread {
    Q_OBJECT
public:
    MyThread(int id) : threadId(id) {}
private:
    int threadId;
    
protected:
    void run() {
        LOG(INFO) <<"Writing from a thread " << threadId;
        
        VLOG(2) << "This is verbose level 2 logging from thread #" << threadId;
        
        // Following line will be logged only once from second running thread (which every runs second into
        // this line because of interval 2)
        LOG_EVERY_N(2, WARNING) << "This will be logged only once from thread who every reaches this line first. Currently running from thread #" << threadId;
        
        for (int i = 1; i <= 10; ++i) {
            VLOG_EVERY_N(2, 3) << "Verbose level 3 log every two times. This is at " << i << " from thread #" << threadId;
        }
        
        // Following line will be logged once with every thread because of interval 1 
        LOG_EVERY_N(1, INFO) << "This interval log will be logged with every thread, this one is from thread #" << threadId;
        
        LOG_IF(threadId == 2, INFO) << "This log is only for thread 2 and is ran by thread #" << threadId;

        el::Logger* logger = el::Loggers::getLogger("default");
        logger->info("Info log from [Thread #%v]", threadId);
        logger->info("Info log");
        logger->verbose(1, "test");
        logger->verbose(1, "test %v", 123);
        logger->verbose(1, "test");
    }
};
#endif
