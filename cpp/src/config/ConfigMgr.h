/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "utils/Error.h"
#include "ConfigNode.h"

namespace zilliz {
namespace milvus {
namespace server {

// this class can parse nested config file and return config item
// config file example(yaml style)
//       AAA: 1
//       BBB:
//         CCC: hello
//         DDD: 23.5
//
// usage
//   const ConfigMgr* mgr = ConfigMgr::GetInstance();
//   const ConfigNode& node = mgr->GetRootNode();
//   std::string val = node.GetValue("AAA"); // return '1'
//   const ConfigNode& child = node.GetChild("BBB");
//   val = child.GetValue("CCC"); //return 'hello'

class ConfigMgr {
 public:
    static ConfigMgr* GetInstance();

    virtual ServerError LoadConfigFile(const std::string &filename) = 0;
    virtual void Print() const = 0;//will be deleted
    virtual std::string DumpString() const = 0;

    virtual const ConfigNode& GetRootNode() const = 0;
    virtual ConfigNode& GetRootNode() = 0;
};

}
}
}
