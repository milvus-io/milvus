#ifndef VECENGINE_DB_META_IMPL_H_
#define VECENGINE_DB_META_IMPL_H_

#include "Meta.h"
#include "Options.h"

namespace zilliz {
namespace vecwise {
namespace engine {
namespace meta {

auto StoragePrototype(const std::string& path);

class DBMetaImpl : public Meta {
public:
    DBMetaImpl(const DBMetaOptions& options_);

    virtual Status add_group(GroupSchema& group_info) override;
    virtual Status get_group(GroupSchema& group_info_) override;
    virtual Status has_group(const std::string& group_id_, bool& has_or_not_) override;

    virtual Status add_group_file(GroupFileSchema& group_file_info) override;

    virtual Status has_group_file(const std::string& group_id_,
                                  const std::string& file_id_,
                                  bool& has_or_not_) override;
    virtual Status get_group_file(const std::string& group_id_,
                                  const std::string& file_id_,
                                  GroupFileSchema& group_file_info_) override;
    virtual Status update_group_file(const GroupFileSchema& group_file_) override;

    virtual Status get_group_files(const std::string& group_id_,
                                   const int date_delta_,
                                   GroupFilesSchema& group_files_info_) override;

    virtual Status update_files(const GroupFilesSchema& files) override;

    virtual Status files_to_merge(const std::string& group_id,
            DatePartionedGroupFilesSchema& files) override;

    virtual Status files_to_index(GroupFilesSchema&) override;

    virtual Status cleanup() override;

    virtual ~DBMetaImpl();

private:

    Status get_group_no_lock(GroupSchema& group_info);
    std::string GetGroupPath(const std::string& group_id);
    std::string GetGroupDatePartitionPath(const std::string& group_id, DateT& date);
    void GetGroupFilePath(GroupFileSchema& group_file);
    Status initialize();

    const DBMetaOptions _options;
}; // DBMetaImpl

} // namespace meta
} // namespace engine
} // namespace vecwise
} // namespace zilliz

#endif // VECENGINE_DB_META_IMPL_H_
