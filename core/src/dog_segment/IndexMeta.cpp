#include "IndexMeta.h"
#include <mutex>
#include <cassert>
namespace milvus::dog_segment {

Status
IndexMeta::AddEntry(const std::string& index_name, const std::string& field_name, IndexType type, IndexMode mode,
                    IndexConfig config) {
    Entry entry{
        index_name,
        field_name,
        type,
        mode,
        std::move(config)
    };
    VerifyEntry(entry);

    if (entries_.count(index_name)) {
        throw std::invalid_argument("duplicate index_name");
    }
    // TODO: support multiple indexes for single field
    assert(!lookups_.count(field_name));
    lookups_[field_name] = index_name;
    entries_[index_name] = std::move(entry);

    return Status::OK();
}

Status
IndexMeta::DropEntry(const std::string& index_name) {
    assert(entries_.count(index_name));
    auto entry = std::move(entries_[index_name]);
    if(lookups_[entry.field_name] == index_name) {
        lookups_.erase(entry.field_name);
    }
    return Status::OK();
}

void IndexMeta::VerifyEntry(const Entry &entry) {
    auto is_mode_valid = std::set{IndexMode::MODE_CPU, IndexMode::MODE_GPU}.count(entry.mode);
    if(!is_mode_valid) {
        throw std::invalid_argument("invalid mode");
    }

    auto& schema = *schema_;
    auto& field_meta = schema[entry.index_name];
    // TODO checking
    if(field_meta.is_vector()) {
        assert(entry.type == knowhere::IndexEnum::INDEX_FAISS_IVFFLAT);
    } else {
        assert(false);
    }
}

}  // namespace milvus::dog_segment
