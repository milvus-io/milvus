#pragma once
#include "common/FieldDataInterface.h"
#include "index/InvertedIndexTantivy.h"
#include "storage/FileManager.h"
#include "boost/filesystem.hpp"
#include "tantivy-binding.h"

namespace milvus::indexbuilder {

template <typename T>
class JsonInvertedIndexCreator : public index::InvertedIndexTantivy<T> {
 public:
    JsonInvertedIndexCreator(const proto::schema::DataType cast_type,
                             const std::string& nested_path,
                             const storage::FileManagerContext& ctx)
        : nested_path_(nested_path) {
        this->schema_ = ctx.fieldDataMeta.field_schema;
        this->mem_file_manager_ =
            std::make_shared<storage::MemFileManagerImpl>(ctx);
        this->disk_file_manager_ =
            std::make_shared<storage::DiskFileManagerImpl>(ctx);

        auto prefix = this->disk_file_manager_->GetTextIndexIdentifier();
        constexpr const char* TMP_INVERTED_INDEX_PREFIX =
            "/tmp/milvus/inverted-index/";
        this->path_ = std::string(TMP_INVERTED_INDEX_PREFIX) + prefix;

        this->d_type_ = index::get_tantivy_data_type(cast_type);
        boost::filesystem::create_directories(this->path_);
        std::string field_name = std::to_string(
            this->disk_file_manager_->GetFieldDataMeta().field_id);
        this->wrapper_ = std::make_shared<index::TantivyIndexWrapper>(
            field_name.c_str(), this->d_type_, this->path_.c_str());
    }

    void
    build_index_for_json(const std::vector<std::shared_ptr<FieldDataBase>>&
                             field_datas) override;

    void
    finish() {
        this->wrapper_->finish();
    }

    void
    create_reader() {
        this->wrapper_->create_reader();
    }

 private:
    std::string nested_path_;
};
}  // namespace milvus::indexbuilder