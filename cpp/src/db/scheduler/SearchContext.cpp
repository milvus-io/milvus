/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "SearchContext.h"
#include "utils/Log.h"
#include <time.h>

namespace zilliz {
namespace vecwise {
namespace engine {

SearchContext::SearchContext(uint64_t topk, uint64_t nq, const float* vectors)
    : topk_(topk),
      nq_(nq),
      vectors_(vectors) {
    //use current time to identify this context
    time_t t;
    time(&t);
    identity_ = std::to_string(t);
}

bool
SearchContext::AddIndexFile(TableFileSchemaPtr& index_file) {
    std::unique_lock <std::mutex> lock(mtx_);
    if(index_file == nullptr || map_index_files_.find(index_file->id) != map_index_files_.end()) {
        return false;
    }

    SERVER_LOG_INFO << "SearchContext " << identity_ << " add index file: " << index_file->id;

    map_index_files_[index_file->id] = index_file;
    return true;
}

void
SearchContext::IndexSearchDone(size_t index_id) {
    std::unique_lock <std::mutex> lock(mtx_);
    map_index_files_.erase(index_id);
    done_cond_.notify_all();
    SERVER_LOG_INFO << "SearchContext " << identity_ << " finish index file: " << index_id;
}

void
SearchContext::WaitResult() {
    std::unique_lock <std::mutex> lock(mtx_);
    done_cond_.wait(lock, [this] { return map_index_files_.empty(); });
}

}
}
}