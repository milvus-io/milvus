 //
 // This file is part of Easylogging++ samples
 // Simple custom log builder
 //
 // Revision 1.0
 // @author mkhan3189
 //

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

/// @brief Not a very fast log builder but good enough for sample
class MyLogBuilder : public el::LogBuilder {
public:
    std::string build(const el::LogMessage* logMessage, bool appendNewLine) const {
        std::stringstream str;
        str << logMessage->message();
        if (appendNewLine) str << "\n";
        return str.str().c_str();
    }
};

int main(void) {
    el::LogBuilderPtr myLogBuilder = el::LogBuilderPtr(new MyLogBuilder());
    el::Loggers::getLogger("default")->setLogBuilder(myLogBuilder);
    LOG(INFO) << "message from default logger";

    el::Loggers::getLogger("new_logger");
    CLOG(INFO, "new_logger") << "message from new_logger logger";

    return 0;
}
