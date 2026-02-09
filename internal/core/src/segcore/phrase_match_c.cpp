#include "segcore/phrase_match_c.h"

#include <exception>
#include <memory>

#include "common/EasyAssert.h"
#include "tantivy/phrase_match.h"

CStatus
compute_phrase_match_slop_c(const char* params,
                            const char* query,
                            const char* data,
                            uint32_t* slop) {
    try {
        *slop = milvus::tantivy::compute_phrase_match_slop(params, query, data);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}
