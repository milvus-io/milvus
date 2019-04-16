////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include "Operand.h"

namespace zilliz {
namespace vecwise {
namespace engine {

std::ostream &operator<<(std::ostream &os, const Operand &obj) {
    os << obj.d << " "
       << obj.index_type << " "
       << obj.preproc << " "
       << obj.postproc << " "
       << obj.metric_type << " "
       << obj.ncent;
    return os;
}

std::istream &operator>>(std::istream &is, Operand &obj) {
    is >> obj.d
       >> obj.index_type
       >> obj.preproc
       >> obj.postproc
       >> obj.metric_type
       >> obj.ncent;
    return is;
}

std::string operand_to_str(const Operand_ptr &opd) {
    std::ostringstream ss;
    ss << opd;
    return ss.str();
}

Operand_ptr str_to_operand(const std::string &input) {
    std::istringstream is(input);
    auto opd = std::make_shared<Operand>();
    is >> *(opd.get());

    return opd;
}

}
}
}
