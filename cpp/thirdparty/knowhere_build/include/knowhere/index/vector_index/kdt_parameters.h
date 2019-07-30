#pragma once

#include <string>
#include <vector>


namespace zilliz {
namespace knowhere {

using KDTParameter = std::pair<std::string, std::string>;

class KDTParameterManagement {
 public:
    const std::vector<KDTParameter> &
    GetKDTParameters();

 public:
    static KDTParameterManagement &
    GetInstance() {
        static KDTParameterManagement instance;
        return instance;
    }

    KDTParameterManagement(const KDTParameterManagement &) = delete;
    KDTParameterManagement &operator=(const KDTParameterManagement &) = delete;
 private:
    KDTParameterManagement();

 private:
    std::vector<KDTParameter> kdt_parameters_;
};

} // namespace knowhere
} // namespace zilliz
