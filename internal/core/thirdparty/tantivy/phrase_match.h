#pragma once

#include <string>
#include "tantivy-binding.h"
#include "rust-binding.h"
#include "rust-array.h"
#include "common/EasyAssert.h"

namespace milvus::tantivy {

inline uint32_t
compute_phrase_match_slop(const std::string& tokenizer_params,
                          const std::string& query,
                          const std::string& data) {
    uint32_t slop = 0;
    auto res = RustResultWrapper(tantivy_compute_phrase_match_slop(
        tokenizer_params.c_str(), query.c_str(), data.c_str(), &slop));
    AssertInfo(res.result_->success,
               "compute_phrase_match_slop failed: {}",
               res.result_->error);
    return slop;
}

}  // namespace milvus::tantivy
