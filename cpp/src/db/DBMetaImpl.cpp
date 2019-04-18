#include <sys/stat.h>
#include <unistd.h>
#include <sstream>
#include <iostream>
#include <boost/filesystem.hpp>
#include <fstream>
#include <sqlite_orm/sqlite_orm.h>
#include "DBMetaImpl.h"
#include "IDGenerator.h"

namespace zilliz {
namespace vecwise {
namespace engine {
namespace meta {

using namespace sqlite_orm;

inline auto StoragePrototype(const std::string& path) {
    return make_storage(path,
            make_table("Group",
                      make_column("id", &GroupSchema::id, primary_key()),
                      make_column("group_id", &GroupSchema::group_id, unique()),
                      make_column("dimension", &GroupSchema::dimension),
                      make_column("files_cnt", &GroupSchema::files_cnt, default_value(0))),
            make_table("GroupFile",
                      make_column("id", &GroupFileSchema::id, primary_key()),
                      make_column("group_id", &GroupFileSchema::group_id),
                      make_column("file_id", &GroupFileSchema::file_id),
                      make_column("file_type", &GroupFileSchema::file_type),
                      make_column("rows", &GroupFileSchema::rows, default_value(0)),
                      make_column("date", &GroupFileSchema::date))
            );

}

using ConnectorT = decltype(StoragePrototype(""));
static std::unique_ptr<ConnectorT> ConnectorPtr;

long GetFileSize(const std::string& filename)
{
    struct stat stat_buf;
    int rc = stat(filename.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

std::string DBMetaImpl::GetGroupPath(const std::string& group_id) {
    return _options.path + "/" + group_id;
}

std::string DBMetaImpl::GetGroupDatePartitionPath(const std::string& group_id, DateT& date) {
    std::stringstream ss;
    ss << GetGroupPath(group_id) << "/" << date;
    return ss.str();
}

void DBMetaImpl::GetGroupFilePath(GroupFileSchema& group_file) {
    if (group_file.date == EmptyDate) {
        group_file.date = Meta::GetDate();
    }
    std::stringstream ss;
    ss << GetGroupDatePartitionPath(group_file.group_id, group_file.date)
       << "/" << group_file.file_id;
    group_file.location = ss.str();
}

DBMetaImpl::DBMetaImpl(const DBMetaOptions& options_)
    : _options(options_) {
    initialize();
}

Status DBMetaImpl::initialize() {
    if (!boost::filesystem::is_directory(_options.path)) {
        assert(boost::filesystem::create_directory(_options.path));
    }

    ConnectorPtr = std::make_unique<ConnectorT>(StoragePrototype(_options.path+"/meta.sqlite"));

    ConnectorPtr->sync_schema();

    return Status::OK();
}

Status DBMetaImpl::add_group(GroupSchema& group_info) {
    if (group_info.group_id == "") {
        std::stringstream ss;
        SimpleIDGenerator g;
        ss << g.getNextIDNumber();
        group_info.group_id = ss.str();
    }
    group_info.files_cnt = 0;
    group_info.id = -1;
    try {
        auto id = ConnectorPtr->insert(group_info);
        std::cout << "id=" << id << std::endl;
        group_info.id = id;
    } catch(std::system_error& e) {
        return Status::GroupError("Add Group " + group_info.group_id + " Error");
    }
    return Status::OK();
}

Status DBMetaImpl::get_group(GroupSchema& group_info) {
    auto groups = ConnectorPtr->select(columns(&GroupSchema::id,
                                              &GroupSchema::group_id,
                                              &GroupSchema::files_cnt,
                                              &GroupSchema::dimension),
                                      where(c(&GroupSchema::group_id) == group_info.group_id));
    assert(groups.size() <= 1);
    if (groups.size() == 1) {
        group_info.id = std::get<0>(groups[0]);
        group_info.files_cnt = std::get<2>(groups[0]);
        group_info.dimension = std::get<3>(groups[0]);
    } else {
        return Status::NotFound("Group " + group_info.group_id + " not found");
    }

    std::cout << __func__ << ": gid=" << group_info.group_id
        << " dimension=" << group_info.dimension << std::endl;
    return Status::OK();
}

Status DBMetaImpl::has_group(const std::string& group_id, bool& has_or_not) {
    auto groups = ConnectorPtr->select(columns(&GroupSchema::id),
                                      where(c(&GroupSchema::group_id) == group_id));
    assert(groups.size() <= 1);
    if (groups.size() == 1) {
        has_or_not = true;
    } else {
        has_or_not = false;
    }
    return Status::OK();
}

Status DBMetaImpl::add_group_file(GroupFileSchema& group_file) {
    if (group_file.date == EmptyDate) {
        group_file.date = Meta::GetDate();
    }
    GroupSchema group_info;
    group_info.group_id = group_file.group_id;
    auto status = get_group(group_info);
    if (!status.ok()) {
        return status;
    }

    SimpleIDGenerator g;
    group_file.file_type = GroupFileSchema::NEW;
    group_file.file_id = g.getNextIDNumber();
    group_file.dimension = group_info.dimension;
    GetGroupFilePath(group_file);
    try {
        auto id = ConnectorPtr->insert(group_file);
        std::cout << __func__ << " id=" << id << std::endl;
        group_info.id = id;
    } catch(std::system_error& e) {
        return Status::GroupError("Add GroupFile Group "
                + group_info.group_id + " File " + group_file.file_id + " Error");
    }
    return Status::OK();
}

Status DBMetaImpl::files_to_index(GroupFilesSchema& files) {
    files.clear();

    auto selected = ConnectorPtr->select(columns(&GroupFileSchema::id,
                                               &GroupFileSchema::group_id,
                                               &GroupFileSchema::file_id,
                                               &GroupFileSchema::file_type,
                                               &GroupFileSchema::rows,
                                               &GroupFileSchema::date),
                                      where(c(&GroupFileSchema::file_type) == (int)GroupFileSchema::TO_INDEX));

    std::map<std::string, GroupSchema> groups;

    for (auto& file : selected) {
        GroupFileSchema group_file;
        group_file.id = std::get<0>(file);
        group_file.group_id = std::get<1>(file);
        group_file.file_id = std::get<2>(file);
        group_file.file_type = std::get<3>(file);
        group_file.rows = std::get<4>(file);
        group_file.date = std::get<5>(file);
        auto groupItr = groups.find(group_file.group_id);
        if (groupItr == groups.end()) {
            GroupSchema group_info;
            group_info.group_id = group_file.group_id;
            auto status = get_group(group_info);
            if (!status.ok()) {
                return status;
            }
            groups[group_file.group_id] = group_info;
        }
        group_file.dimension = groups[group_file.group_id].dimension;
        files.push_back(group_file);
    }

    return Status::OK();
}

Status DBMetaImpl::files_to_merge(const std::string& group_id,
        DatePartionedGroupFilesSchema& files) {
    files.clear();

    auto selected = ConnectorPtr->select(columns(&GroupFileSchema::id,
                                               &GroupFileSchema::group_id,
                                               &GroupFileSchema::file_id,
                                               &GroupFileSchema::file_type,
                                               &GroupFileSchema::rows,
                                               &GroupFileSchema::date));
                                      /* where(is_equal(&GroupFileSchema::file_type, GroupFileSchema::RAW) && */
                                      /*       is_equal(&GroupFileSchema::group_id, group_id))); */

    GroupSchema group_info;
    group_info.group_id = group_id;
    auto status = get_group(group_info);
    if (!status.ok()) {
        return status;
    }

    for (auto& file : selected) {
        GroupFileSchema group_file;
        group_file.id = std::get<0>(file);
        group_file.group_id = std::get<1>(file);
        group_file.file_id = std::get<2>(file);
        group_file.file_type = std::get<3>(file);
        group_file.rows = std::get<4>(file);
        group_file.date = std::get<5>(file);
        group_file.dimension = group_info.dimension;
        auto dateItr = files.find(group_file.date);
        if (dateItr == files.end()) {
            files[group_file.date] = GroupFilesSchema();
        }
        files[group_file.date].push_back(group_file);
    }

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

Status DBMetaImpl::update_files(const GroupFilesSchema& files) {
    auto commited = ConnectorPtr->transaction([&] () mutable {
        for (auto& file : files) {
            ConnectorPtr->update(file);
        }
        return true;
    });
    if (!commited) {
        return Status::DBTransactionError("Update files Error");
    }
    return Status::OK();
}

} // namespace meta
} // namespace engine
} // namespace vecwise
} // namespace zilliz
