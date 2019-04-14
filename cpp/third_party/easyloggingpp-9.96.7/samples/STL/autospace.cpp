 //
 // This file is part of Easylogging++ samples
 // Demonstration of auto spacing functionality
 //
 // Revision 1.2
 // @author mkhan3189
 //

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

int main(void) {

    LOG(INFO) << "this" << "is" << "a" << "message";
    std::string str = "-sample-";
    std::wstring wstr = L"-wsample-";
    const char* chr = "-csample-";
    const wchar_t* wchr = L"-wcsample-";
    LOG(INFO) << str << str << str << str;
    LOG(INFO) << wstr << wstr << wstr << wstr;
    LOG(INFO) << chr << chr << chr << chr;
    LOG(INFO) << wchr << wchr << wchr << wchr;

    // ---- THIS IS MAGIC 
    el::Loggers::addFlag(el::LoggingFlag::AutoSpacing);

    LOG(INFO) << "this" << "is" << "a" << "message";    
    LOG(INFO) << str << str << str << str;
    LOG(INFO) << wstr << wstr << wstr << wstr;
    LOG(INFO) << chr << chr << chr << chr;
    LOG(INFO) << wchr << wchr << wchr << wchr;

    return 0;
}
