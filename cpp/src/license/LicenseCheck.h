#pragma once

#include "utils/Error.h"
#include "LicenseLibrary.h"

#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>


namespace zilliz {
namespace vecwise {
namespace server {

class LicenseCheck {
 public:
    static LicenseCheck &
    GetInstance() {
        static LicenseCheck instance;
        return instance;
    };


    // Part 1:  Legality check
    static ServerError
    LegalityCheck(const std::string &license_file_path);


    // Part 2: Timing check license
    static ServerError
    AlterFile(const std::string &license_file_path,
              const boost::system::error_code &ec,
              boost::asio::deadline_timer *pt);


    static ServerError
    StartCountingDown(const std::string &license_file_path);

 private:


};

}
}
}


