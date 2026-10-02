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
#include "segcore/token_stream_c.h"

#include "common/CGoCatch.h"
#include "tantivy-binding.h"
#include "token-stream.h"

// Ring-3 note: these are extern "C" entry points driven directly from Go, so
// no exception may escape (it would cross the C ABI and terminate). The
// C++-side throw surface is small (the wrappers delegate to tantivy's C
// binding), but the catch tails close the boundary uniformly. A Rust-side
// panic aborts in the binding itself and cannot be caught here — that is an
// abort-class source to be eliminated upstream.

void
free_token_stream(CTokenStream token_stream) {
    try {
        delete static_cast<milvus::tantivy::TokenStream*>(token_stream);
    }
    CGO_CATCH_AND_LOG("free_token_stream")
}

bool
token_stream_advance(CTokenStream token_stream) {
    try {
        return static_cast<milvus::tantivy::TokenStream*>(token_stream)
            ->advance();
    }
    CGO_CATCH_AND_LOG("token_stream_advance")
    // false ends the token iteration; the failure is visible in the log.
    return false;
}

// Note: returned token must be freed by the caller using `free_token`.
const char*
token_stream_get_token(CTokenStream token_stream) {
    try {
        return static_cast<milvus::tantivy::TokenStream*>(token_stream)
            ->get_token_no_copy();
    }
    CGO_CATCH_AND_LOG("token_stream_get_token")
    return nullptr;  // C.GoString(NULL) yields "" on the Go side
}

CToken
token_stream_get_detailed_token(CTokenStream token_stream) {
    try {
        auto token = static_cast<milvus::tantivy::TokenStream*>(token_stream)
                         ->get_detailed_token();
        return CToken{token.token,
                      token.start_offset,
                      token.end_offset,
                      token.position,
                      token.position_length};
    }
    CGO_CATCH_AND_LOG("token_stream_get_detailed_token")
    return CToken{};
}

void
free_token(void* token) {
    try {
        free_rust_string(static_cast<const char*>(token));
    }
    CGO_CATCH_AND_LOG("free_token")
}
