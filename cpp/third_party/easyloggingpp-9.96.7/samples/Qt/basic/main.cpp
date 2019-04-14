/**
 * This file is part of EasyLogging++ samples
 * Demonstration of multithreaded application in C++ (Qt)
 *
 * Compile this program using Qt
 *    qmake qt-sample.pro && make
 *
 * Revision: 1.1
 * @author mkhan3189
 */

#include "mythread.h"
#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

class LogHandler : public el::LogDispatchCallback {
public:
    void handle(const el::LogDispatchData* data) {
        // NEVER LOG ANYTHING HERE! NOT HAPPY WITH MULTI_THREADING
        ELPP_COUT << "Test this " << data << std::endl;
    }
};

class HtmlHandler : public el::LogDispatchCallback {
public:
    HtmlHandler() {
        el::Loggers::getLogger("html");
    }
    void handle(const el::LogDispatchData* data) {
        // NEVER LOG ANYTHING HERE! NOT HAPPY WITH MULTI_THREADING
        ELPP_COUT << "<b>" << data->logMessage()->message() << "</b>" << std::endl;
    }
};

    
int main(int argc, char* argv[]) {
    START_EASYLOGGINGPP(argc, argv);

    el::Loggers::removeFlag(el::LoggingFlag::NewLineForContainer);
    el::Helpers::installLogDispatchCallback<LogHandler>("LogHandler");
    el::Helpers::installLogDispatchCallback<HtmlHandler>("HtmlHandler");
    LOG(INFO) << "First log";
    LogHandler* logHandler = el::Helpers::logDispatchCallback<LogHandler>("LogHandler");
    logHandler->setEnabled(false);

    LOG(INFO) << "Second log";
#if 1
    bool runThreads = true;

    if (runThreads) {
        for (int i = 1; i <= 10; ++i) {
            MyThread t(i);
            t.start();
            t.wait();
        }
    }

     TIMED_BLOCK(t, "whole-block") {
        t.timer->checkpoint();

        LOG(WARNING) << "Starting Qt Logging";

        QVector<QString> stringList;
        stringList.push_back (QString("Test"));
        stringList.push_back (QString("Test 2"));
        int i = 0;
        while (++i != 2)
            LOG(INFO) << stringList;

        QPair<QString, int> qpair_;
        qpair_.first = "test";
        qpair_.second = 2;
        LOG(INFO) << qpair_;

        QMap<QString, int> qmap_;
        qmap_.insert ("john", 100);
        qmap_.insert ("michael", 101);
        LOG(INFO) << qmap_;

        QMultiMap<QString, int> qmmap_;
        qmmap_.insert ("john", 100);
        qmmap_.insert ("michael", 101);
        LOG(INFO) << qmmap_;

        QSet<QString> qset_;
        qset_.insert ("test");
        qset_.insert ("second");
        LOG(INFO) << qset_;

        QVector<QString*> ptrList;
        ptrList.push_back (new QString("Test"));
        LOG(INFO) << ptrList;
        qDeleteAll(ptrList);

        QHash<QString, QString> qhash_;
        qhash_.insert ("john", "101fa");
        qhash_.insert ("michael", "102mf");
        LOG(INFO) << qhash_;

        QLinkedList<QString> qllist_;
        qllist_.push_back ("test");
        qllist_.push_back ("test 2");
        LOG(INFO) << qllist_ ;

        QStack<QString> qstack_;
        qstack_.push ("100");
        qstack_.push ("200");
        qstack_.push ("100");
        LOG(DEBUG) << "Printing qstack " << qstack_;


        DCHECK(2 > 1) << "What????";
    }
    
    LOG(INFO) << "This is not unicode";
    LOG(INFO) << "This is unicode: " << L"世界，你好";
#endif
    return 0;
}
