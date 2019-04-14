 //
 // This file is part of Easylogging++ samples
 // Smart pointer null check
 //
 // Revision 1.0
 // @author mkhan3189
 //

#include "easylogging++.h"
#include <memory>

INITIALIZE_EASYLOGGINGPP

int main(void) {
    std::unique_ptr<std::string> test2(new std::string);
    CHECK_NOTNULL(test2) << "And I didn't expect this to be null anyway";

    std::unique_ptr<int> test3;
    CHECK_NOTNULL(test3) << "It should crash here";
    
    return 0;
}
