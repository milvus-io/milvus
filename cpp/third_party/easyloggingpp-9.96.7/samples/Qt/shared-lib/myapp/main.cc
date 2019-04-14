#include <QApplication>
#include "mylib.hh"
#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

int main(int argc, char *argv[])
{
    QApplication a(argc, argv);

    Mylib l;

    LOG(INFO) << "1 + 2 = " << l.add(1, 2);
    LOG(INFO) << "1 / 2 = " << l.div(1, 2);
    LOG(DEBUG) << "1 * 2 = " << l.mul(1, 2);

    // This will cause FATAL error because of division by zero
    // LOG(INFO) << l.div(1, 0);
    
    return 0;
}
