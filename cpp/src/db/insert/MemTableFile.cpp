#include "MemTableFile.h"
#include "db/Constants.h"
#include "db/Log.h"
#include "db/engine/EngineFactory.h"
#include "metrics/Metrics.h"

#include <cmath>


namespace zilliz {
namespace milvus {
namespace engine {

MemTableFile::MemTableFile(const std::string &table_id,
                           const std::shared_ptr<meta::Meta> &meta,
                           const Options &options) :
    table_id_(table_id),
    meta_(meta),
    options_(options) {

    current_mem_ = 0;
    auto status = CreateTableFile();
    if (status.ok()) {
        execution_engine_ = EngineFactory::Build(table_file_schema_.dimension_,
                                                 table_file_schema_.location_,
                                                 (EngineType) table_file_schema_.engine_type_,
                                                 (MetricType)table_file_schema_.metric_type_,
                                                 table_file_schema_.nlist_);
    }
}

Status MemTableFile::CreateTableFile() {

    meta::TableFileSchema table_file_schema;
    table_file_schema.table_id_ = table_id_;
    auto status = meta_->CreateTableFile(table_file_schema);
    if (status.ok()) {
        table_file_schema_ = table_file_schema;
    } else {
        std::string err_msg = "MemTableFile::CreateTableFile failed: " + status.ToString();
        ENGINE_LOG_ERROR << err_msg;
    }
    return status;
}

Status MemTableFile::Add(const VectorSource::Ptr &source, IDNumbers& vector_ids) {

    if (table_file_schema_.dimension_ <= 0) {
        std::string err_msg = "MemTableFile::Add: table_file_schema dimension = " +
            std::to_string(table_file_schema_.dimension_) + ", table_id = " + table_file_schema_.table_id_;
        ENGINE_LOG_ERROR << err_msg;
        return Status::Error(err_msg);
    }

    size_t single_vector_mem_size = table_file_schema_.dimension_ * VECTOR_TYPE_SIZE;
    size_t mem_left = GetMemLeft();
    if (mem_left >= single_vector_mem_size) {
        size_t num_vectors_to_add = std::ceil(mem_left / single_vector_mem_size);
        size_t num_vectors_added;
        auto status = source->Add(execution_engine_, table_file_schema_, num_vectors_to_add, num_vectors_added, vector_ids);
        if (status.ok()) {
            current_mem_ += (num_vectors_added * single_vector_mem_size);
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

bool MemTableFile::IsFull() {
    size_t single_vector_mem_size = table_file_schema_.dimension_ * VECTOR_TYPE_SIZE;
    return (GetMemLeft() < single_vector_mem_size);
}

Status MemTableFile::Serialize() {
    size_t size = GetCurrentMem();
    server::CollectSerializeMetrics metrics(size);

    execution_engine_->Serialize();
    table_file_schema_.file_size_ = execution_engine_->PhysicalSize();
    table_file_schema_.row_count_ = execution_engine_->Count();

    table_file_schema_.file_type_ = (size >= table_file_schema_.index_file_size_) ?
                                    meta::TableFileSchema::TO_INDEX : meta::TableFileSchema::RAW;

    auto status = meta_->UpdateTableFile(table_file_schema_);

    ENGINE_LOG_DEBUG << "New " << ((table_file_schema_.file_type_ == meta::TableFileSchema::RAW) ? "raw" : "to_index")
               << " file " << table_file_schema_.file_id_ << " of size " << size << " bytes";

    if(options_.insert_cache_immediately_) {
        execution_engine_->Cache();
    }

    return status;
}

} // namespace engine
} // namespace milvus
} // namespace zilliz