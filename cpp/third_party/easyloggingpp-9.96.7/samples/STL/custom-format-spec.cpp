 //
 // This file is part of Easylogging++ samples
 //
 // Custom format specifier to demonstrate usage of el::Helpers::installCustomFormatSpecifier
 //
 // Revision 1.2
 // @author mkhan3189
 //

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

enum ELogLevel : el::base::type::VerboseLevel {
    kLogLevel_Off = 0,
    kLogLevel_Error,
    kLogLevel_Warning,
    kLogLevel_Info,
    kLogLevel_Debug,
    kLogLevel_Verbose
    };

static std::map<ELogLevel, std::string> sSeverityMap {
    { kLogLevel_Error,   "ouch!" },
    { kLogLevel_Warning, "oops" },
    { kLogLevel_Info,    "hey" },
    { kLogLevel_Debug,   "debugging" },
    { kLogLevel_Verbose, "loquacious" }
};

std::string
getSeverity(const el::LogMessage* message) {
    return sSeverityMap[static_cast<ELogLevel>(message->verboseLevel())].c_str();
}

class HttpRequest {
public:
    std::string getIp(const el::LogMessage*) {
        return "192.168.1.1";
    }
};

int main(void) {
    using namespace std::placeholders;

    HttpRequest request;

    // Install format specifier
    el::Helpers::installCustomFormatSpecifier(el::CustomFormatSpecifier("%ip_addr",  std::bind(&HttpRequest::getIp, request, _1)));
    // Either you can do what's done above (for member function) or if you have static function you can simply say
    // el::CustomFormatSpecifier("%ip_addr", getIp)

    // Configure loggers
    el::Loggers::reconfigureAllLoggers(el::ConfigurationType::Format, "%datetime %level %ip_addr : %msg");
    LOG(INFO) << "This is after installed 'ip_addr' spec";

    // Uninstall custom format specifier
    el::Helpers::uninstallCustomFormatSpecifier("%ip_addr");
    LOG(INFO) << "This is after uninstalled";

    // Install format specifier
    el::Helpers::installCustomFormatSpecifier(el::CustomFormatSpecifier("%severity", getSeverity));

    // Configure loggers
    el::Loggers::reconfigureAllLoggers(el::ConfigurationType::Format, "%datetime{%b %d %H:%m:%s}: [%severity] %msg");
    el::Loggers::setVerboseLevel(kLogLevel_Verbose);

    VLOG(kLogLevel_Info) << "Installed 'severity' custom formatter";
    VLOG(kLogLevel_Error) << "This is an error";
    VLOG(kLogLevel_Warning) << "This is a warning";
    VLOG(kLogLevel_Info) << "This is info";
    VLOG(kLogLevel_Debug) << "This is debug";
    VLOG(kLogLevel_Verbose) << "This is verbose";

    // Uninstall custom format specifier
    el::Helpers::uninstallCustomFormatSpecifier("%severity");

    VLOG(kLogLevel_Info) << "Uninstalled 'severity' custom formatter";
    VLOG(kLogLevel_Error) << "This is an error";
    VLOG(kLogLevel_Warning) << "This is a warning";
    VLOG(kLogLevel_Info) << "This is info";
    VLOG(kLogLevel_Debug) << "This is debug";
    VLOG(kLogLevel_Verbose) << "This is verbose";

    return 0;
}
