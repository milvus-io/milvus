#include "MemTable.h"
#include "Log.h"

namespace zilliz {
namespace milvus {
namespace engine {

MemTable::MemTable(const std::string& table_id,
                   const std::shared_ptr<meta::Meta>& meta,
                   const Options& options) :
                   table_id_(table_id),
                   meta_(meta),
                   options_(options) {

}

Status MemTable::Add(VectorSource::Ptr& source) {
    while (!source->AllAdded()) {
        MemTableFile::Ptr currentMemTableFile;
        if (!mem_table_file_list_.empty()) {
            currentMemTableFile = mem_table_file_list_.back();
        }
        Status status;
        if (mem_table_file_list_.empty() || currentMemTableFile->IsFull()) {
            MemTableFile::Ptr newMemTableFile = std::make_shared<MemTableFile>(table_id_, meta_, options_);
            status = newMemTableFile->Add(source);
            if (status.ok()) {
                mem_table_file_list_.emplace_back(newMemTableFile);
            }
        }
        else {
            status = currentMemTableFile->Add(source);
        }
        if (!status.ok()) {
            std::string errMsg = "MemTable::Add failed: " + status.ToString();
            ENGINE_LOG_ERROR << errMsg;
            return Status::Error(errMsg);
        }
    }
    return Status::OK();
}

void MemTable::GetCurrentMemTableFile(MemTableFile::Ptr& mem_table_file) {
    mem_table_file = mem_table_file_list_.back();
}

size_t MemTable::GetTableFileCount() {
    return mem_table_file_list_.size();
}

Status MemTable::Serialize() {
    for (auto& memTableFile : mem_table_file_list_) {
        auto status = memTableFile->Serialize();
        if (!status.ok()) {
            std::string errMsg = "MemTable::Serialize failed: " + status.ToString();
            ENGINE_LOG_ERROR << errMsg;
            return Status::Error(errMsg);
        }
    }
    return Status::OK();
}

bool MemTable::Empty() {
    return mem_table_file_list_.empty();
}

std::string MemTable::GetTableId() {
    return table_id_;
}

} // namespace engine
} // namespace milvus
} // namespace zilliz