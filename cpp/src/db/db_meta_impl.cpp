#include "db_meta_impl.h"

namespace zilliz {
namespace vecwise {
namespace engine {

Status DBMetaImpl::DBMetaImpl(DBMetaOptions options_)
    : _options(options_) {
    initialize();
}

Status DBMetaImpl::initialize() {
    // PXU TODO: Create DB Connection
    return Status.OK();
}

Status DBMetaImpl::add_group(const std::string& group_id_, GroupSchema& group_info) {
    //PXU TODO
    return Status.OK();
}

Status DBMetaImpl::get_group(const std::string& group_id_, GroupSchema& group_info) {
    //PXU TODO
    return Status.OK();
}

Status DBMetaImpl::has_group(const std::string& group_id_, bool& has_or_not) {
    //PXU TODO
    return Status.OK();
}

Status DBMetaImpl::add_group_file(const std::string& group_id,
                              GroupFileSchema& group_file_info) {
    //PXU TODO
    return Status.OK();
}

Status DBMetaImpl::has_group_file(const std::string& group_id,
                              const std::string& file_id,
                              bool& has_or_not) {
    //PXU TODO
    return Status.OK();
}

Status DBMetaImpl::get_group_file(const std::string& group_id,
                              const std::string& file_id,
                              GroupFileSchema& group_file_info) {
    //PXU TODO
    return Status.OK();
}

Status DBMetaImpl::get_group_files(const std::string& group_id,
                               GroupFilesSchema& group_files_info) {
    // PXU TODO
    return Status.OK();
}

Status DBMetaImpl::mark_group_file_as_index(const std::string& group_id,
                                        const std::string& file_id) {
    //PXU TODO
    return Status.OK();
}

} // namespace engine
} // namespace vecwise
} // namespace zilliz
