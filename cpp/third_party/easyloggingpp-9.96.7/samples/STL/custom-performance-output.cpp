 //
 // This file is part of Easylogging++ samples
 // PerformanceTrackingCallback sample to customize performance output
 //
 // Revision 1.0
 // @author mkhan3189
 //

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

class MyPerformanceTrackingOutput : public el::PerformanceTrackingCallback {
public:
    MyPerformanceTrackingOutput() {
        el::PerformanceTrackingCallback* defaultCallback = 
            el::Helpers::performanceTrackingCallback<el::base::DefaultPerformanceTrackingCallback>("DefaultPerformanceTrackingCallback");
        defaultCallback->setEnabled(false);
    }
    virtual ~MyPerformanceTrackingOutput() {
        el::PerformanceTrackingCallback* defaultCallback = 
            el::Helpers::performanceTrackingCallback<el::base::DefaultPerformanceTrackingCallback>("DefaultPerformanceTrackingCallback");
        defaultCallback->setEnabled(true);
    }
protected:
    void handle(const el::PerformanceTrackingData* data) {
        if (data->firstCheckpoint()) { return; } // ignore first check point
        el::base::type::stringstream_t ss;
        ss << data->blockName()->c_str() << " took " <<  *data->formattedTimeTaken() << " to run";
        if (data->dataType() == el::PerformanceTrackingData::DataType::Checkpoint) {
            ss << " [CHECKPOINT ONLY] ";
        }
        CLOG(INFO, data->loggerId().c_str()) << ss.str();
    }
};

int main(void) {

    TIMED_SCOPE(mainBlock, "main");

    el::Helpers::installPerformanceTrackingCallback<MyPerformanceTrackingOutput>("MyPerformanceTrackingOutput");

    {
        TIMED_SCOPE(timer, "myblock");
        for (int i = 0; i <= 500; ++i) {
            // Spend time
            for (int j = 0; j < 10000; ++j) { std::string tmp; }
            if (i % 100 == 0) {
                timer->checkpoint(std::string(std::string("checkpoint at ") + std::to_string(static_cast<unsigned long long>(i))).c_str());
            }
        }
    } 
    LOG(INFO) << "By now, you should get performance result of above scope";
    el::Helpers::uninstallPerformanceTrackingCallback<MyPerformanceTrackingOutput>("MyPerformanceTrackingOutput");
    // You will get "main" block in normal format (default) 
    // Now that we have uninstalled our custom callback we should get our default call back enabled (see destructor of MyPerformanceTrackingOutput above)
    return 0;
}
