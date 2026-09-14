#include "segcore/phrase_match_c.h"

#include <exception>
#include <memory>

#include "common/CGoCatch.h"
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
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}
