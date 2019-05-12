#include "LicenseCheck.h"

namespace zilliz {
namespace vecwise {
namespace server {

class LicenseCheck {
 public:
    static LicenseCheck&
    GetInstance() {
        static LicenseCheck instance;
        return instance;
    }

    ServerError
    Check();

    ServerError
    PeriodicUpdate();

 private:
    LicenseCheck() {};


};

}
}
}