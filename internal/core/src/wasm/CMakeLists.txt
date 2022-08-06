# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License

option( EMBEDDED_MILVUS "Enable embedded Milvus" OFF )

if ( EMBEDDED_MILVUS )
    add_compile_definitions( EMBEDDED_MILVUS )
endif()

#WasmFunctionManager
set(MILVUS_WASM_SRCS
        WasmFunctionManager.cpp
        )

add_library(milvus_wasm ${MILVUS_WASM_SRCS})

target_link_libraries(milvus_wasm PUBLIC wasmtime)
