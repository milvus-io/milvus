#ifndef VECENGINE_DB_META_IMPL_H_
#define VECENGINE_DB_META_IMPL_H_

#include "db_meta.h"
#include "options.h"

namespace zilliz {
namespace vecwise {
namespace engine {

class DBMetaImpl : public Meta {
public:
    DBMetaImpl(DBMetaOptions& options_);

    virtual Status add_group(const GroupOptions& options_,
            const std::string& group_id_,
            GroupSchema& group_info_) override;
    virtual Status get_group(const std::string& group_id_, GroupSchema& group_info_) override;
    virtual Status has_group(const std::string& group_id_, bool& has_or_not_) override;

    virtual Status add_group_file(const std::string& group_id_,
                                  GroupFileSchema& group_file_info_) override;
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

private:

    Status initialize();

    const DBMetaOptions _options;

}; // DBMetaImpl

} // namespace engine
} // namespace vecwise
} // namespace zilliz

#endif // VECENGINE_DB_META_IMPL_H_
