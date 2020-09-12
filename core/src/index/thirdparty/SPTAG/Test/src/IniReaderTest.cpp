// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Test.h"
#include "inc/Helper/SimpleIniReader.h"

#include <fstream>

BOOST_AUTO_TEST_SUITE(IniReaderTest)

BOOST_AUTO_TEST_CASE(IniReaderLoadTest)
{
    std::ofstream tmpIni("temp.ini");
    tmpIni << "[Common]" << std::endl;
    tmpIni << "; Comment " << std::endl;
    tmpIni << "Param1=1" << std::endl;
    tmpIni << "Param2=Exp=2" << std::endl;

    tmpIni.close();

    SPTAG::Helper::IniReader reader;
    BOOST_CHECK(SPTAG::ErrorCode::Success == reader.LoadIniFile("temp.ini"));

    BOOST_CHECK(reader.DoesSectionExist("Common"));
    BOOST_CHECK(reader.DoesParameterExist("Common", "Param1"));
    BOOST_CHECK(reader.DoesParameterExist("Common", "Param2"));

    BOOST_CHECK(!reader.DoesSectionExist("NotExist"));
    BOOST_CHECK(!reader.DoesParameterExist("NotExist", "Param1"));
    BOOST_CHECK(!reader.DoesParameterExist("Common", "ParamNotExist"));

    BOOST_CHECK(1 == reader.GetParameter<int>("Common", "Param1", 0));
    BOOST_CHECK(0 == reader.GetParameter<int>("Common", "ParamNotExist", 0));

    BOOST_CHECK(std::string("Exp=2") == reader.GetParameter<std::string>("Common", "Param2", std::string()));
    BOOST_CHECK(std::string("1") == reader.GetParameter<std::string>("Common", "Param1", std::string()));
    BOOST_CHECK(std::string() == reader.GetParameter<std::string>("Common", "ParamNotExist", std::string()));
}

BOOST_AUTO_TEST_SUITE_END()