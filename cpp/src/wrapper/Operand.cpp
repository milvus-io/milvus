////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include "src/server/ServerConfig.h"
#include "Operand.h"


namespace zilliz {
namespace milvus {
namespace engine {

using std::string;

enum IndexType {
    Invalid_Option = 0,
    IVF = 1,
    IDMAP = 2,
    IVFSQ8 = 3,
};

IndexType resolveIndexType(const string &index_type) {
    if (index_type == "IVF") { return IndexType::IVF; }
    if (index_type == "IDMap") { return IndexType::IDMAP; }
    if (index_type == "IVFSQ8") { return IndexType::IVFSQ8; }
    return IndexType::Invalid_Option;
}

// nb at least 100
string Operand::get_index_type(const int &nb) {
    if (!index_str.empty()) { return index_str; }

    switch (resolveIndexType(index_type)) {
        case Invalid_Option: {
            // TODO: add exception
            break;
        }
        case IVF: {

            using namespace zilliz::milvus::server;
            ServerConfig &config = ServerConfig::GetInstance();
            ConfigNode engine_config = config.GetConfig(CONFIG_ENGINE);
            size_t nlist = engine_config.GetInt32Value(CONFIG_NLIST, 16384);

            index_str += (ncent != 0 ? index_type + std::to_string(ncent) :
                          index_type + std::to_string(int(nb / 1000000.0 * nlist)));
//            std::cout<<"nlist = "<<nlist<<std::endl;
            if (!postproc.empty()) { index_str += ("," + postproc); }
            break;
        }
        case IVFSQ8: {

            using namespace zilliz::milvus::server;
            ServerConfig &config = ServerConfig::GetInstance();
            ConfigNode engine_config = config.GetConfig(CONFIG_ENGINE);
            size_t nlist = engine_config.GetInt32Value(CONFIG_NLIST, 16384);

            index_str += (ncent != 0 ? "IVF" + std::to_string(ncent) :
                          "IVF" + std::to_string(int(nb / 1000000.0 * nlist)));
            index_str += ",SQ8";
//            std::cout<<"nlist = "<<nlist<<std::endl;
            break;
        }
        case IDMAP: {
            index_str += index_type;
            if (!postproc.empty()) { index_str += ("," + postproc); }
            break;
        }
    }

    return index_str;
}

std::ostream &operator<<(std::ostream &os, const Operand &obj) {
    os << obj.d << " "
       << obj.index_type << " "
       << obj.metric_type << " "
       << obj.preproc << " "
       << obj.postproc << " "
       << obj.ncent;
    return os;
}

std::istream &operator>>(std::istream &is, Operand &obj) {
    is >> obj.d
       >> obj.index_type
       >> obj.metric_type
       >> obj.preproc
       >> obj.postproc
       >> obj.ncent;
    return is;
}

std::string operand_to_str(const Operand_ptr &opd) {
    std::ostringstream ss;
    ss << *opd;
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
