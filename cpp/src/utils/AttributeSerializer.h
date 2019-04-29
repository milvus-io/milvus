/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <map>

namespace zilliz {
namespace vecwise {
namespace server {

class AttributeSerializer {
public:
    static void Encode(const std::map<std::string, std::string>& attrib, std::string& result);
    static void Decode(const std::string& str, std::map<std::string, std::string>& result);
};


}
}
}
