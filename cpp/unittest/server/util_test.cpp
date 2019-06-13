////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <gtest/gtest.h>

#include "utils/AttributeSerializer.h"
#include "utils/StringHelpFunctions.h"

using namespace zilliz::milvus;

TEST(AttribSerializeTest, ATTRIBSERIAL_TEST) {
    std::map<std::string, std::string> attrib;
    attrib["uid"] = "ABCDEF";
    attrib["color"] = "red";
    attrib["number"] = "9900";
    attrib["comment"] = "please note: it is a car, not a ship";
    attrib["address"] = " china;shanghai ";

    std::string attri_str;
    server::AttributeSerializer::Encode(attrib, attri_str);

    std::map<std::string, std::string> attrib_out;
    server::ServerError err = server::AttributeSerializer::Decode(attri_str, attrib_out);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    ASSERT_EQ(attrib_out.size(), attrib.size());
    for(auto iter : attrib) {
        ASSERT_EQ(attrib_out[iter.first], attrib_out[iter.first]);
    }

}

