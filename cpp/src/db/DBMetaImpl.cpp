/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include <unistd.h>
#include <sstream>
#include <iostream>
#include <boost/filesystem.hpp>
#include <chrono>
#include <fstream>
#include <sqlite_orm.h>
#include <easylogging++.h>
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
                      make_column("updated_time", &GroupFileSchema::updated_time),
                      make_column("date", &GroupFileSchema::date))
            );

}

using ConnectorT = decltype(StoragePrototype("/tmp/dummy.sqlite3"));
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

long DBMetaImpl::GetMicroSecTimeStamp() {
    auto now = std::chrono::system_clock::now();
    auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
            now.time_since_epoch()).count();

    return micros;
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
        auto ret = boost::filesystem::create_directory(_options.path);
        if (!ret) {
            LOG(ERROR) << "Create directory " << _options.path << " Error";
        }
        assert(ret);
    }

    ConnectorPtr = std::make_unique<ConnectorT>(StoragePrototype(_options.path+"/meta.sqlite"));

    ConnectorPtr->sync_schema();
    ConnectorPtr->open_forever(); // thread safe option
    ConnectorPtr->pragma.journal_mode(journal_mode::WAL); // WAL => write ahead log

    cleanup();

    return Status::OK();
}

// PXU TODO: Temp solution. Will fix later
Status DBMetaImpl::delete_group_partitions(const std::string& group_id,
            const meta::DatesT& dates) {
    if (dates.size() == 0) {
        return Status::OK();
    }

    GroupSchema group_info;
    group_info.group_id = group_id;
    auto status = get_group(group_info);
    if (!status.ok()) {
        return status;
    }

    auto yesterday = GetDateWithDelta(-2);

    for (auto& date : dates) {
        if (date >= yesterday) {
            return Status::Error("Could not delete partitions with 2 days");
        }
    }

    try {
        ConnectorPtr->update_all(
                    set(
                        c(&GroupFileSchema::file_type) = (int)GroupFileSchema::TO_DELETE
                    ),
                    where(
                        c(&GroupFileSchema::group_id) == group_id and
                        in(&GroupFileSchema::date, dates)
                    ));
    } catch (std::exception & e) {
        LOG(DEBUG) << e.what();
        throw e;
    }
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

    {
        try {
            auto id = ConnectorPtr->insert(group_info);
            group_info.id = id;
            /* LOG(DEBUG) << "Add group " << id; */
        } catch (...) {
            return Status::DBTransactionError("Add Group Error");
        }
    }

    auto group_path = GetGroupPath(group_info.group_id);

    if (!boost::filesystem::is_directory(group_path)) {
        auto ret = boost::filesystem::create_directory(group_path);
        if (!ret) {
            LOG(ERROR) << "Create directory " << group_path << " Error";
        }
        assert(ret);
    }

    return Status::OK();
}

Status DBMetaImpl::get_group(GroupSchema& group_info) {
    return get_group_no_lock(group_info);
}

Status DBMetaImpl::get_group_no_lock(GroupSchema& group_info) {
    try {
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
    } catch (std::exception &e) {
        LOG(DEBUG) << e.what();
        throw e;
    }

    return Status::OK();
}

Status DBMetaImpl::has_group(const std::string& group_id, bool& has_or_not) {
    try {
        auto groups = ConnectorPtr->select(columns(&GroupSchema::id),
                                          where(c(&GroupSchema::group_id) == group_id));
        assert(groups.size() <= 1);
        if (groups.size() == 1) {
            has_or_not = true;
        } else {
            has_or_not = false;
        }
    } catch (std::exception &e) {
        LOG(DEBUG) << e.what();
        throw e;
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
    std::stringstream ss;
    ss << g.getNextIDNumber();
    group_file.file_type = GroupFileSchema::NEW;
    group_file.file_id = ss.str();
    group_file.dimension = group_info.dimension;
    group_file.rows = 0;
    group_file.updated_time = GetMicroSecTimeStamp(); //ConnectorPtr->select(datetime("now", "localtime +1 hour")).front();
    GetGroupFilePath(group_file);

    {
        try {
            auto id = ConnectorPtr->insert(group_file);
            group_file.id = id;
            /* LOG(DEBUG) << "Add group_file of file_id=" << group_file.file_id; */
        } catch (...) {
            return Status::DBTransactionError("Add file Error");
        }
    }

    auto partition_path = GetGroupDatePartitionPath(group_file.group_id, group_file.date);

    if (!boost::filesystem::is_directory(partition_path)) {
        auto ret = boost::filesystem::create_directory(partition_path);
        if (!ret) {
            LOG(ERROR) << "Create directory " << partition_path << " Error";
        }
        assert(ret);
    }

    return Status::OK();
}

Status DBMetaImpl::files_to_index(GroupFilesSchema& files) {
    files.clear();

    try {
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
            GetGroupFilePath(group_file);
            auto groupItr = groups.find(group_file.group_id);
            if (groupItr == groups.end()) {
                GroupSchema group_info;
                group_info.group_id = group_file.group_id;
                auto status = get_group_no_lock(group_info);
                if (!status.ok()) {
                    return status;
                }
                groups[group_file.group_id] = group_info;
            }
            group_file.dimension = groups[group_file.group_id].dimension;
            files.push_back(group_file);
        }
    } catch (std::exception & e) {
        LOG(DEBUG) << e.what();
        throw e;
    }

    return Status::OK();
}

Status DBMetaImpl::files_to_search(const std::string &group_id,
                                   const DatesT& partition,
                                   DatePartionedGroupFilesSchema &files) {
    files.clear();
    DatesT today = {Meta::GetDate()};
    const DatesT& dates = (partition.empty() == true) ? today : partition;

    try {
        auto selected = ConnectorPtr->select(columns(&GroupFileSchema::id,
                                                     &GroupFileSchema::group_id,
                                                     &GroupFileSchema::file_id,
                                                     &GroupFileSchema::file_type,
                                                     &GroupFileSchema::rows,
                                                     &GroupFileSchema::date),
                                             where(c(&GroupFileSchema::group_id) == group_id and
                                                 in(&GroupFileSchema::date, dates) and
                                                 (c(&GroupFileSchema::file_type) == (int) GroupFileSchema::RAW or
                                                     c(&GroupFileSchema::file_type) == (int) GroupFileSchema::TO_INDEX or
                                                     c(&GroupFileSchema::file_type) == (int) GroupFileSchema::INDEX)));

        GroupSchema group_info;
        group_info.group_id = group_id;
        auto status = get_group_no_lock(group_info);
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
            GetGroupFilePath(group_file);
            auto dateItr = files.find(group_file.date);
            if (dateItr == files.end()) {
                files[group_file.date] = GroupFilesSchema();
            }
            files[group_file.date].push_back(group_file);
        }
    } catch (std::exception & e) {
        LOG(DEBUG) << e.what();
        throw e;
    }

    return Status::OK();
}

Status DBMetaImpl::files_to_merge(const std::string& group_id,
        DatePartionedGroupFilesSchema& files) {
    files.clear();

    try {
        auto selected = ConnectorPtr->select(columns(&GroupFileSchema::id,
                                                   &GroupFileSchema::group_id,
                                                   &GroupFileSchema::file_id,
                                                   &GroupFileSchema::file_type,
                                                   &GroupFileSchema::rows,
                                                   &GroupFileSchema::date),
                                          where(c(&GroupFileSchema::file_type) == (int)GroupFileSchema::RAW and
                                                c(&GroupFileSchema::group_id) == group_id));

        GroupSchema group_info;
        group_info.group_id = group_id;
        auto status = get_group_no_lock(group_info);
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
            GetGroupFilePath(group_file);
            auto dateItr = files.find(group_file.date);
            if (dateItr == files.end()) {
                files[group_file.date] = GroupFilesSchema();
            }
            files[group_file.date].push_back(group_file);
        }
    } catch (std::exception & e) {
        LOG(DEBUG) << e.what();
        throw e;
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

Status DBMetaImpl::update_group_file(GroupFileSchema& group_file) {
    group_file.updated_time = GetMicroSecTimeStamp();
    try {
        ConnectorPtr->update(group_file);
        /* auto commited = ConnectorPtr->transaction([&] () mutable { */
        /*     ConnectorPtr->update(group_file); */
        /*     return true; */
        /* }); */
        /* if (!commited) { */
        /*     return Status::DBTransactionError("Update file Error"); */
        /* } */
    } catch (std::exception & e) {
        LOG(DEBUG) << e.what();
        LOG(DEBUG) << "id= " << group_file.id << " file_id=" << group_file.file_id;
        throw e;
    }
    return Status::OK();
}

Status DBMetaImpl::update_files(GroupFilesSchema& files) {
    try {
        auto commited = ConnectorPtr->transaction([&] () mutable {
            for (auto& file : files) {
                file.updated_time = GetMicroSecTimeStamp();
                ConnectorPtr->update(file);
            }
            return true;
        });
        if (!commited) {
            return Status::DBTransactionError("Update files Error");
        }
    } catch (std::exception & e) {
        LOG(DEBUG) << e.what();
        throw e;
    }
    return Status::OK();
}

Status DBMetaImpl::cleanup_ttl_files(uint16_t seconds) {
    auto now = GetMicroSecTimeStamp();
    try {
        auto selected = ConnectorPtr->select(columns(&GroupFileSchema::id,
                                                   &GroupFileSchema::group_id,
                                                   &GroupFileSchema::file_id,
                                                   &GroupFileSchema::file_type,
                                                   &GroupFileSchema::rows,
                                                   &GroupFileSchema::date),
                                          where(c(&GroupFileSchema::file_type) == (int)GroupFileSchema::TO_DELETE and
                                                c(&GroupFileSchema::updated_time) > now - 1000000*seconds));

        GroupFilesSchema updated;

        for (auto& file : selected) {
            GroupFileSchema group_file;
            group_file.id = std::get<0>(file);
            group_file.group_id = std::get<1>(file);
            group_file.file_id = std::get<2>(file);
            group_file.file_type = std::get<3>(file);
            group_file.rows = std::get<4>(file);
            group_file.date = std::get<5>(file);
            GetGroupFilePath(group_file);
            if (group_file.file_type == GroupFileSchema::TO_DELETE) {
                boost::filesystem::remove(group_file.location);
            }
            ConnectorPtr->remove<GroupFileSchema>(group_file.id);
            /* LOG(DEBUG) << "Removing deleted id=" << group_file.id << " location=" << group_file.location << std::endl; */
        }
    } catch (std::exception & e) {
        LOG(DEBUG) << e.what();
        throw e;
    }

    return Status::OK();
}

Status DBMetaImpl::cleanup() {
    try {
        auto selected = ConnectorPtr->select(columns(&GroupFileSchema::id,
                                                   &GroupFileSchema::group_id,
                                                   &GroupFileSchema::file_id,
                                                   &GroupFileSchema::file_type,
                                                   &GroupFileSchema::rows,
                                                   &GroupFileSchema::date),
                                          where(c(&GroupFileSchema::file_type) == (int)GroupFileSchema::TO_DELETE or
                                                c(&GroupFileSchema::file_type) == (int)GroupFileSchema::NEW));

        GroupFilesSchema updated;

        for (auto& file : selected) {
            GroupFileSchema group_file;
            group_file.id = std::get<0>(file);
            group_file.group_id = std::get<1>(file);
            group_file.file_id = std::get<2>(file);
            group_file.file_type = std::get<3>(file);
            group_file.rows = std::get<4>(file);
            group_file.date = std::get<5>(file);
            GetGroupFilePath(group_file);
            if (group_file.file_type == GroupFileSchema::TO_DELETE) {
                boost::filesystem::remove(group_file.location);
            }
            ConnectorPtr->remove<GroupFileSchema>(group_file.id);
            /* LOG(DEBUG) << "Removing id=" << group_file.id << " location=" << group_file.location << std::endl; */
        }
    } catch (std::exception & e) {
        LOG(DEBUG) << e.what();
        throw e;
    }

    return Status::OK();
}

Status DBMetaImpl::count(const std::string& group_id, long& result) {

    try {
        auto selected = ConnectorPtr->select(columns(&GroupFileSchema::rows,
                                                   &GroupFileSchema::date),
                                          where((c(&GroupFileSchema::file_type) == (int)GroupFileSchema::RAW or
                                                 c(&GroupFileSchema::file_type) == (int)GroupFileSchema::TO_INDEX or
                                                 c(&GroupFileSchema::file_type) == (int)GroupFileSchema::INDEX) and
                                                c(&GroupFileSchema::group_id) == group_id));

        GroupSchema group_info;
        group_info.group_id = group_id;
        auto status = get_group_no_lock(group_info);
        if (!status.ok()) {
            return status;
        }

        result = 0;
        for (auto& file : selected) {
            result += std::get<0>(file);
        }

        result /= group_info.dimension;

    } catch (std::exception & e) {
        LOG(DEBUG) << e.what();
        throw e;
    }
    return Status::OK();
}

Status DBMetaImpl::drop_all() {
    if (boost::filesystem::is_directory(_options.path)) {
        boost::filesystem::remove_all(_options.path);
    }
    return Status::OK();
}

DBMetaImpl::~DBMetaImpl() {
    cleanup();
}

} // namespace meta
} // namespace engine
} // namespace vecwise
} // namespace zilliz
