// Easylogging++ sample for libsigc++
// @author mkhan3189
// @rev 1.0

#include <unistd.h>
#include <sigc++/sigc++.h>
#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

class AsyncConnector {
public:
    AsyncConnector() {}
    void sendNow(void) { LOG(INFO) << "Sending data..."; sleep(2); sent.emit(); }
    sigc::signal<void> received;
    sigc::signal<void> sent;
};

void dataReceived(void) {
    LOG(INFO) << "Async data has been received";
}

void dataSent(void) {
    LOG(INFO) << "Async data has been sent";
}

int main(void) {
    AsyncConnector asyncConnection;
    asyncConnection.received.connect(sigc::ptr_fun(dataReceived));
    asyncConnection.sent.connect(sigc::ptr_fun(dataSent));

    asyncConnection.sendNow();

    return 0;
}
