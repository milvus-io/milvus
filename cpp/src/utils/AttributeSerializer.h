/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <map>

#include "Error.h"

namespace zilliz {
namespace vecwise {
namespace server {

using AttribMap = std::map<std::string, std::string>;

class AttributeSerializer {
public:
    static ServerError Encode(const AttribMap& attrib_map, std::string& attrib_str);
    static ServerError Decode(const std::string& attrib_str, AttribMap& attrib_map);
};


}
}
}
