#include "segcore/phrase_match_c.h"
#include "tantivy/phrase_match.h"
#include "common/type_c.h"
#include "common/Exception.h"

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
