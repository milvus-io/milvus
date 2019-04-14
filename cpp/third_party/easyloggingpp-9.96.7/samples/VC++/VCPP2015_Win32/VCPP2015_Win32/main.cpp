/**
* This file is part of Easylogging++ samples
* Demonstration of simple VC++ project.
*
* PLEASE NOTE: We have ELPP_FEATURE_PERFORMANCE_TRACKING preprocessor defined in project settings
* Otherwise we will get linker error
*
* Revision: 1.1
* @author mkhan3189
*/
#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

TIMED_SCOPE(appTimer, "myapplication");

int main() {
	
	LOG(INFO) << "Starting...";
	el::Loggers::removeFlag(el::LoggingFlag::AllowVerboseIfModuleNotSpecified);

	{
		TIMED_SCOPE(tmr, "write-simple");
		LOG(INFO) << "Test " << __FILE__;
	}

	VLOG(3) << "Test verbose";
	system("pause");
}
