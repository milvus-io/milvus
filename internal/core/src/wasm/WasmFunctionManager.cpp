// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License


//
// Created by wzy on 22-8-6.
//


#include <fstream>
#include <iostream>
#include <sstream>
#include "WasmFunctionManager.h"

namespace milvus {

bool WasmFunctionManager::RegisterFunction(std::string functionName,
                                           std::string functionHandler,
                                           const std::string &base64OrOtherString) {
    auto funcBody = funcMap.find(functionName);
    if (funcBody != funcMap.end()) {
        return false;
    }
    auto watString = myBase64Decode(base64OrOtherString);
    auto wasmRuntime = createInstanceAndFunction(watString, functionHandler);
    modules.emplace(functionName, wasmRuntime);
    funcMap.emplace(functionName, base64OrOtherString);
    return true;
}

WasmtimeRunInstance WasmFunctionManager::createInstanceAndFunction(const std::string &watString,
                                                                   const std::string &functionHandler) {
    auto module = wasmtime::Module::compile(*engine, watString).unwrap();
    auto instance = wasmtime::Instance::create(store, module, {}).unwrap();
    auto function_obj = instance.get(store, functionHandler);
    wasmtime::Func *func = std::get_if<wasmtime::Func>(&*function_obj);
    return WasmtimeRunInstance(*func, instance);
}

bool WasmFunctionManager::runElemFunc(const std::string functionName, std::vector<wasmtime::Val> args) {
    auto module = modules.at(functionName);
    auto results = module.func.call(store, args).unwrap();
    return results[0].i32();
}

bool WasmFunctionManager::DeleteFunction(std::string functionName) {
    auto funcBody = funcMap.find(functionName);
    if (funcBody == funcMap.end()) {
        return false;
    }
    modules.erase(functionName);
    return true;
}

}  // namespace milvus
