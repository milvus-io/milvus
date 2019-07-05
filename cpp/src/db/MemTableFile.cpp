#include "MemTableFile.h"
#include "Constants.h"
#include "Log.h"

#include <cmath>

namespace zilliz {
namespace milvus {
namespace engine {

MemTableFile::MemTableFile(const std::string& table_id,
                           const std::shared_ptr<meta::Meta>& meta) :
                           table_id_(table_id),
                           meta_(meta) {

    current_mem_ = 0;
    CreateTableFile();
}

Status MemTableFile::CreateTableFile() {

    meta::TableFileSchema table_file_schema;
    table_file_schema.table_id_ = table_id_;
    auto status = meta_->CreateTableFile(table_file_schema);
    if (status.ok()) {
        table_file_schema_ = table_file_schema;
    }
    else {
        std::string errMsg = "MemTableFile::CreateTableFile failed: " + status.ToString();
        ENGINE_LOG_ERROR << errMsg;
    }
    return status;
}

Status MemTableFile::Add(const VectorSource::Ptr& source) {

    size_t singleVectorMemSize = table_file_schema_.dimension_ * VECTOR_TYPE_SIZE;
    size_t memLeft = GetMemLeft();
    if (memLeft >= singleVectorMemSize) {
        size_t numVectorsToAdd = std::ceil(memLeft / singleVectorMemSize);
        size_t numVectorsAdded;
        auto status = source->Add(table_file_schema_, numVectorsToAdd, numVectorsAdded);
        if (status.ok()) {
            current_mem_ += (numVectorsAdded * singleVectorMemSize);
        }
        return status;
    }
    return Status::OK();
}

size_t MemTableFile::GetCurrentMem() {
    return current_mem_;
}

size_t MemTableFile::GetMemLeft() {
    return (MAX_TABLE_FILE_MEM - current_mem_);
}

bool MemTableFile::isFull() {
    size_t singleVectorMemSize = table_file_schema_.dimension_ * VECTOR_TYPE_SIZE;
    return (GetMemLeft() < singleVectorMemSize);
}

} // namespace engine
} // namespace milvus
} // namespace zilliz