 //
 // This file is part of Easylogging++ samples
 //
 // Check macros (incl. debug versions i.e, DCHECK etc) + PCHECK and DCHECK
 // We add logging flag `DisableApplicationAbortOnFatalLog` that prevents application abort because CHECK macros always log FATAL
 //
 // Revision 1.1
 // @author mkhan3189
 //

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

int main(void) {
    el::Loggers::addFlag(el::LoggingFlag::DisableApplicationAbortOnFatalLog);
    
    // These checks should fail
    LOG(INFO) << "----- DONT WORRY ABOUT FOLLOWING CHECKS FAILING - THEY ARE EXPECTED";
    CHECK(1 > 2) << "1 is not greater than 2";
    CHECK_EQ(1, 2) << "1 is not equal to 2";
    CHECK_NE(1, 1) << "Wow, I did not know 1 == 1";
    CHECK_STREQ("abc", "def") << " :)";
    CHECK_STRNE("abc", "abc") << " :(";
    CHECK_STRCASEEQ("abc", "ABCD") << " :p";
    CHECK_STRCASENE("abc", "ABC") << " B)";
    int* f = new int;
    CHECK_NOTNULL(f);

    delete f;
    f = nullptr;
    // These checks should pass 
    LOG(WARNING) << "----- START WORRYING ABOUT CHECKS NOW";
    CHECK(1 < 2) << " snap -- lib has bug!";
    CHECK_EQ(1, 1) << " snap -- lib has bug!";
    CHECK_NE(1, 2) << " snap -- lib has bug!";
    CHECK_STREQ("abc", "abc") << " snap -- lib has bug!";
    CHECK_STRNE("abc", "abe") << " snap -- lib has bug!";
    CHECK_STRCASEEQ("abc", "ABC") << " snap -- lib has bug!";
    CHECK_STRCASENE("abc", "ABE") << " snap -- lib has bug!";
    LOG(INFO) << "----- HOPEFULLY NO CHECK FAILED SINCE YOU STARTED WORRYING!";

    // DCHECKs
    DCHECK(1 > 2) << "1 is not greater than 2";
    DCHECK_EQ(1, 2) << "1 is not equal to 2";
    DCHECK_NE(1, 1) << "Wow, I did not know 1 == 1";
    DCHECK_STREQ("abc", "def") << " :)";
    DCHECK_STRNE("abc", "abc") << " :(";
    DCHECK_STRCASEEQ("abc", "ABCD") << " :p";
    DCHECK_STRCASENE("abc", "ABC") << " B)";
    
    // PCHECKs
    std::fstream fstr("a/file/that/does/not/exist", std::fstream::in);
    PCHECK(fstr.is_open());
    DPCHECK(fstr.is_open());

    int min = 1;
    int max = 5;
    CHECK_BOUNDS(1, min, max) << "Index out of bounds";
    CHECK_BOUNDS(2, min, max) << "Index out of bounds";
    CHECK_BOUNDS(3, min, max) << "Index out of bounds";
    CHECK_BOUNDS(4, min, max) << "Index out of bounds";
    CHECK_BOUNDS(5, min, max) << "Index out of bounds";
    CHECK_BOUNDS(6, min, max) << "Index out of bounds";
    DCHECK_BOUNDS(1, min, max) << "Index out of bounds";
    DCHECK_BOUNDS(2, min, max) << "Index out of bounds";
    DCHECK_BOUNDS(3, min, max) << "Index out of bounds";
    DCHECK_BOUNDS(4, min, max) << "Index out of bounds";
    DCHECK_BOUNDS(5, min, max) << "Index out of bounds";
    DCHECK_BOUNDS(6, min, max) << "Index out of bounds";

    return 0;
}
