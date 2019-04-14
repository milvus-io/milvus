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

    virtual Status add_group(const std::string& group_id_, GroupSchema& group_info) override;
    virtual Status get_group(const std::string& group_id_, GroupSchema& group_info) override;
    virtual Status has_group(const std::string& group_id_, bool& has_or_not) override;

    virtual Status add_group_file(const std::string& group_id,
                                  GroupFileSchema& group_file_info) override;
    virtual Status has_group_file(const std::string& group_id,
                                  const std::string& file_id,
                                  bool& has_or_not) override;
    virtual Status get_group_file(const std::string& group_id,
                                  const std::string& file_id,
                                  GroupFileSchema& group_file_info) override;
    virtual Status mark_group_file_as_index(const std::string& group_id,
                                            const std::string& file_id) override;

    virtual Status get_group_files(const std::string& group_id,
                                   GroupFilesSchema& group_files_info) override;

private:

    Status initialize();

    const DBMetaOptions _options;

}; // DBMetaImpl

} // namespace engine
} // namespace vecwise
} // namespace zilliz

#endif // VECENGINE_DB_META_IMPL_H_
