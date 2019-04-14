 //
 // This file is part of Easylogging++ samples
 // TIMED_SCOPE sample
 //
 // Revision 1.0
 // @author mkhan3189
 //

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

int main(void) {

    {
        TIMED_SCOPE(timer, "my-block");
        for (int i = 0; i <= 500; ++i) {
            LOG(INFO) << "This is iter " << i;
        }
    } 
    LOG(INFO) << "By now, you should get performance result of above scope";

    return 0;
}
