#pragma once

#include "utils/Error.h"
#include "LicenseLibrary.h"

#include <boost/asio.hpp>

#include <thread>
#include <memory>

namespace zilliz {
namespace milvus {
namespace server {

class LicenseCheck {
private:
    LicenseCheck();
    ~LicenseCheck();

public:
    static LicenseCheck &
    GetInstance() {
        static LicenseCheck instance;
        return instance;
    };

    static ServerError
    LegalityCheck(const std::string &license_file_path);

    ServerError
    StartCountingDown(const std::string &license_file_path);

    ServerError
    StopCountingDown();

private:
    static ServerError
    AlterFile(const std::string &license_file_path,
              const boost::system::error_code &ec,
              boost::asio::deadline_timer *pt);

private:
    boost::asio::io_service io_service_;
    std::shared_ptr<std::thread> counting_thread_;

};

}
}
}


