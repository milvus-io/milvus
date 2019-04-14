 //
 // This file is part of Easylogging++ samples
 // TIMED_BLOCK sample
 //
 // Revision 1.1
 // @author mkhan3189
 //

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

int main(int argc, char** argv) {

    START_EASYLOGGINGPP(argc, argv);
   
    TIMED_BLOCK(t, "my-block") {
        for (long i = 0; i <= 300; ++i) {
            LOG(INFO) << "This is for-block 1";
        }
        t.timer->checkpoint("checkpoint-1"); // Note t.timer to access el::base::Trackable
        for (int i = 0; i <= 200; ++i) {
            LOG(INFO) << "This is for-block 2";
        }
    }
    LOG(INFO) << "You should get performance result of above scope";

    return 0;
}
