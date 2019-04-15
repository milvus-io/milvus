#include "db_meta_impl.h"

namespace zilliz {
namespace vecwise {
namespace engine {

DBMetaImpl::DBMetaImpl(const MetaOptions& options_)
    : _options(static_cast<const DBMetaOptions&>(options_)) {
    initialize();
}

Status DBMetaImpl::initialize() {
    // PXU TODO: Create DB Connection
    return Status::OK();
}

Status DBMetaImpl::add_group(const GroupOptions& options_,
            const std::string& group_id_,
            GroupSchema& group_info_) {
    //PXU TODO
    return Status::OK();
}

Status DBMetaImpl::get_group(const std::string& group_id_, GroupSchema& group_info_) {
    //PXU TODO
    return Status::OK();
}

Status DBMetaImpl::has_group(const std::string& group_id_, bool& has_or_not_) {
    //PXU TODO
    return Status::OK();
}

Status DBMetaImpl::add_group_file(const std::string& group_id_,
                              GroupFileSchema& group_file_info_) {
    //PXU TODO
    return Status::OK();
}

Status DBMetaImpl::has_group_file(const std::string& group_id_,
                              const std::string& file_id_,
                              bool& has_or_not_) {
    //PXU TODO
    return Status::OK();
}

Status DBMetaImpl::get_group_file(const std::string& group_id_,
                              const std::string& file_id_,
                              GroupFileSchema& group_file_info_) {
    //PXU TODO
    return Status::OK();
}

Status DBMetaImpl::get_group_files(const std::string& group_id_,
                               const int date_delta_,
                               GroupFilesSchema& group_files_info_) {
    // PXU TODO
    return Status::OK();
}

Status DBMetaImpl::update_group_file(const GroupFileSchema& group_file_) {
    //PXU TODO
    return Status::OK();
}

} // namespace engine
} // namespace vecwise
} // namespace zilliz
