/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "AttributeSerializer.h"
#include "StringHelpFunctions.h"

namespace zilliz {
namespace vecwise {
namespace server {


ServerError AttributeSerializer::Encode(const AttribMap& attrib_map, std::string& attrib_str) {
    attrib_str = "";
    for(auto iter : attrib_map) {
        attrib_str += iter.first;
        attrib_str += ":\"";
        attrib_str += iter.second;
        attrib_str += "\";";
    }

    return SERVER_SUCCESS;
}

ServerError AttributeSerializer::Decode(const std::string& attrib_str, AttribMap& attrib_map) {
    attrib_map.clear();

    std::vector<std::string> kv_pairs;
    StringHelpFunctions::SplitStringByQuote(attrib_str, ";", "\"", kv_pairs);
    for(std::string& str : kv_pairs) {
        std::string key, val;
        size_t index = str.find_first_of(":", 0);
        if (index != std::string::npos) {
            key = str.substr(0, index);
            val = str.substr(index + 1);
        } else {
            key = str;
        }

        attrib_map.insert(std::make_pair(key, val));
    }

    return SERVER_SUCCESS;
}

}
}
}
