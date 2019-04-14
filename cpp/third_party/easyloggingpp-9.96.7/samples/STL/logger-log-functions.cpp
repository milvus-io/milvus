 //
 // This file is part of Easylogging++ samples
 // Very basic sample for Logger::info etc
 //
 // Revision 1.0
 // @author mkhan3189
 //

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

int main(int argc, char** argv) {

    START_EASYLOGGINGPP(argc, argv);

    el::Logger* defaultLogger = el::Loggers::getLogger("default");

    LOG(INFO) << "Blah";

    std::vector<int> i;
    i.push_back(1);
    i.push_back(2);
    defaultLogger->info("simple %v", "msg");
    defaultLogger->warn("My first ultimate log message %v %v %v", 123, 222, i);

    // Escaping
    defaultLogger->info("My first ultimate log message %% %%v %v %v", 123, 222); // My first ultimate log message % %v 123 222

    defaultLogger->verbose(1, "test verbose");
    defaultLogger->verbose(1, "test verbose with args %v", 2);


    return 0;
}
