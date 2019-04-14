#include "easylogging++.h"

class MyMath {
public:
    static int sum(int a, int b) {
        LOG(INFO) << "Adding " << a << " and " << b;
        return a + b;
    }
};
