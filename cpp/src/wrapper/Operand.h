////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <string>
#include <memory>
#include <iostream>
#include <sstream>


namespace zilliz {
namespace milvus {
namespace engine {

struct Operand {
    friend std::ostream &operator<<(std::ostream &os, const Operand &obj);

    friend std::istream &operator>>(std::istream &is, Operand &obj);

    int d;
    std::string index_type = "IVF";
    std::string metric_type = "L2"; //> L2 / IP(Inner Product)
    std::string preproc;
    std::string postproc = "Flat";
    std::string index_str;
    int ncent = 0;

    std::string get_index_type(const int &nb);
};

using Operand_ptr = std::shared_ptr<Operand>;

extern std::string operand_to_str(const Operand_ptr &opd);

extern Operand_ptr str_to_operand(const std::string &input);


}
}
}
