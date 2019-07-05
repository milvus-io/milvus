#include "MemTable.h"
#include "Log.h"

namespace zilliz {
namespace milvus {
namespace engine {

MemTable::MemTable(const std::string& table_id,
                   const std::shared_ptr<meta::Meta>& meta) :
                   table_id_(table_id),
                   meta_(meta) {

}

Status MemTable::Add(VectorSource::Ptr& source) {
    while (!source->AllAdded()) {
        MemTableFile::Ptr currentMemTableFile;
        if (!mem_table_file_stack_.empty()) {
            currentMemTableFile = mem_table_file_stack_.top();
        }
        Status status;
        if (mem_table_file_stack_.empty() || currentMemTableFile->isFull()) {
            MemTableFile::Ptr newMemTableFile = std::make_shared<MemTableFile>(table_id_, meta_);
            status = newMemTableFile->Add(source);
            if (status.ok()) {
                mem_table_file_stack_.push(newMemTableFile);
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
    mem_table_file = mem_table_file_stack_.top();
}

size_t MemTable::GetStackSize() {
    return mem_table_file_stack_.size();
}

} // namespace engine
} // namespace milvus
} // namespace zilliz