#pragma once
//
//#include <shared_mutex>
//
//#include "SegmentDefs.h"
// #include "segcore/SegmentBase.h"
#include "segcore/SegmentDefs.h"
#include "knowhere/index/IndexType.h"
#include "knowhere/common/Config.h"
#include <map>
#include <memory>
namespace milvus::segcore {
// TODO: this is
class IndexMeta {
 public:
    IndexMeta(SchemaPtr schema) : schema_(schema) {
    }
    using IndexType = knowhere::IndexType;
    using IndexMode = knowhere::IndexMode;
    using IndexConfig = knowhere::Config;

    struct Entry {
        std::string index_name;
        std::string field_name;
        IndexType type;
        IndexMode mode;
        IndexConfig config;
    };

    Status
    AddEntry(const std::string& index_name,
             const std::string& field_name,
             IndexType type,
             IndexMode mode,
             IndexConfig config);

    Status
    DropEntry(const std::string& index_name);

    const std::map<std::string, Entry>&
    get_entries() {
        return entries_;
    }

    const Entry&
    lookup_by_field(const std::string& field_name) {
        AssertInfo(lookups_.count(field_name), field_name);
        auto index_name = lookups_.at(field_name);
        AssertInfo(entries_.count(index_name), index_name);
        return entries_.at(index_name);
    }

 private:
    void
    VerifyEntry(const Entry& entry);

 private:
    SchemaPtr schema_;
    std::map<std::string, Entry> entries_;        // index_name => Entry
    std::map<std::string, std::string> lookups_;  // field_name => index_name
};

using IndexMetaPtr = std::shared_ptr<IndexMeta>;
}  // namespace milvus::segcore
