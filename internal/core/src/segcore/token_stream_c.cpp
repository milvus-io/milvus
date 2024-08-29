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

#include <stdlib.h>
#include <string.h>

#include "segcore/token_stream_c.h"
#include "token-stream.h"

void
free_token_stream(CTokenStream token_stream) {
    delete reinterpret_cast<milvus::tantivy::TokenStream*>(token_stream);
}

bool
token_stream_advance(CTokenStream token_stream) {
    return reinterpret_cast<milvus::tantivy::TokenStream*>(token_stream)
        ->advance();
}

// Note: returned string must be freed by the caller.
char*
token_stream_get_token(CTokenStream token_stream) {
    auto token = reinterpret_cast<milvus::tantivy::TokenStream*>(token_stream)
                     ->get_token();
    char* ret = reinterpret_cast<char*>(malloc(token.length() + 1));
    memcpy(ret, token.c_str(), token.length());
    ret[token.length()] = 0;
    return ret;
}
