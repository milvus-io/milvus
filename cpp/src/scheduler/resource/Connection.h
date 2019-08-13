/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <string>
#include <sstream>


namespace zilliz {
namespace milvus {
namespace engine {

class Connection {
public:
    Connection(std::string name, double speed)
        : name_(std::move(name)), speed_(speed) {}

    const std::string &
    get_name() const {
        return name_;
    }

    const double
    get_speed() const {
        return speed_;
    }

public:
    std::string
    Dump() const {
        std::stringstream ss;
        ss << "<name: " << name_ << ", speed: " << speed_ << ">";
        return ss.str();
    }

private:
    std::string name_;
    double speed_;
};


}
}
}
