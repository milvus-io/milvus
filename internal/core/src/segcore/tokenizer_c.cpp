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

#include "segcore/tokenizer_c.h"
#include "common/EasyAssert.h"

#include "tokenizer.h"

using Map = std::map<std::string, std::string>;

CStatus
create_tokenizer(CMap m, CTokenizer* tokenizer) {
    try {
        auto mm = reinterpret_cast<Map*>(m);
        auto impl = std::make_unique<milvus::tantivy::Tokenizer>(*mm);
        *tokenizer = impl.release();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

void
free_tokenizer(CTokenizer tokenizer) {
    auto impl = reinterpret_cast<milvus::tantivy::Tokenizer*>(tokenizer);
    delete impl;
}

CTokenStream
create_token_stream(CTokenizer tokenizer, const char* text, uint32_t text_len) {
    auto impl = reinterpret_cast<milvus::tantivy::Tokenizer*>(tokenizer);
    return impl->CreateTokenStream(std::string(text, text_len)).release();
}
