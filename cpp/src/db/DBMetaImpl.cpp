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
#include "Utils.h"
#include "MetaConsts.h"

namespace zilliz {
namespace vecwise {
namespace engine {
namespace meta {

using namespace sqlite_orm;

inline auto StoragePrototype(const std::string& path) {
    return make_storage(path,
            make_table("Group",
                      make_column("id", &TableSchema::id, primary_key()),
                      make_column("table_id", &TableSchema::table_id, unique()),
                      make_column("dimension", &TableSchema::dimension),
                      make_column("created_on", &TableSchema::created_on),
                      make_column("files_cnt", &TableSchema::files_cnt, default_value(0))),
            make_table("GroupFile",
                      make_column("id", &TableFileSchema::id, primary_key()),
                      make_column("table_id", &TableFileSchema::table_id),
                      make_column("file_id", &TableFileSchema::file_id),
                      make_column("file_type", &TableFileSchema::file_type),
                      make_column("size", &TableFileSchema::size, default_value(0)),
                      make_column("updated_time", &TableFileSchema::updated_time),
                      make_column("created_on", &TableFileSchema::created_on),
                      make_column("date", &TableFileSchema::date))
            );

}

using ConnectorT = decltype(StoragePrototype(""));
static std::unique_ptr<ConnectorT> ConnectorPtr;

std::string DBMetaImpl::GetGroupPath(const std::string& table_id) {
    return _options.path + "/tables/" + table_id;
}

std::string DBMetaImpl::GetGroupDatePartitionPath(const std::string& table_id, DateT& date) {
    std::stringstream ss;
    ss << GetGroupPath(table_id) << "/" << date;
    return ss.str();
}

void DBMetaImpl::GetGroupFilePath(TableFileSchema& group_file) {
    if (group_file.date == EmptyDate) {
        group_file.date = Meta::GetDate();
    }
    std::stringstream ss;
    ss << GetGroupDatePartitionPath(group_file.table_id, group_file.date)
       << "/" << group_file.file_id;
    group_file.location = ss.str();
}

Status DBMetaImpl::NextGroupId(std::string& table_id) {
    std::stringstream ss;
    SimpleIDGenerator g;
    ss << g.getNextIDNumber();
    table_id = ss.str();
    return Status::OK();
}

Status DBMetaImpl::NextFileId(std::string& file_id) {
    std::stringstream ss;
    SimpleIDGenerator g;
    ss << g.getNextIDNumber();
    file_id = ss.str();
    return Status::OK();
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
Status DBMetaImpl::delete_group_partitions(const std::string& table_id,
            const meta::DatesT& dates) {
    if (dates.size() == 0) {
        return Status::OK();
    }

    TableSchema group_info;
    group_info.table_id = table_id;
    auto status = get_group(group_info);
    if (!status.ok()) {
        return status;
    }

    auto yesterday = GetDateWithDelta(-1);

    for (auto& date : dates) {
        if (date >= yesterday) {
            return Status::Error("Could not delete partitions with 2 days");
        }
    }

    try {
        ConnectorPtr->update_all(
                    set(
                        c(&TableFileSchema::file_type) = (int)TableFileSchema::TO_DELETE
                    ),
                    where(
                        c(&TableFileSchema::table_id) == table_id and
                        in(&TableFileSchema::date, dates)
                    ));
    } catch (std::exception & e) {
        LOG(DEBUG) << e.what();
        throw e;
    }
    return Status::OK();
}

Status DBMetaImpl::add_group(TableSchema& group_info) {
    if (group_info.table_id == "") {
        NextGroupId(group_info.table_id);
    }
    group_info.files_cnt = 0;
    group_info.id = -1;
    group_info.created_on = utils::GetMicroSecTimeStamp();

    {
        try {
            auto id = ConnectorPtr->insert(group_info);
            group_info.id = id;
        } catch (...) {
            return Status::DBTransactionError("Add Group Error");
        }
    }

    auto group_path = GetGroupPath(group_info.table_id);

    if (!boost::filesystem::is_directory(group_path)) {
        auto ret = boost::filesystem::create_directories(group_path);
        if (!ret) {
            LOG(ERROR) << "Create directory " << group_path << " Error";
        }
        assert(ret);
    }

    return Status::OK();
}

Status DBMetaImpl::get_group(TableSchema& group_info) {
    return get_group_no_lock(group_info);
}

Status DBMetaImpl::get_group_no_lock(TableSchema& group_info) {
    try {
        auto groups = ConnectorPtr->select(columns(&TableSchema::id,
                                                  &TableSchema::table_id,
                                                  &TableSchema::files_cnt,
                                                  &TableSchema::dimension),
                                          where(c(&TableSchema::table_id) == group_info.table_id));
        assert(groups.size() <= 1);
        if (groups.size() == 1) {
            group_info.id = std::get<0>(groups[0]);
            group_info.files_cnt = std::get<2>(groups[0]);
            group_info.dimension = std::get<3>(groups[0]);
        } else {
            return Status::NotFound("Group " + group_info.table_id + " not found");
        }
    } catch (std::exception &e) {
        LOG(DEBUG) << e.what();
        throw e;
    }

    return Status::OK();
}

Status DBMetaImpl::has_group(const std::string& table_id, bool& has_or_not) {
    try {
        auto groups = ConnectorPtr->select(columns(&TableSchema::id),
                                          where(c(&TableSchema::table_id) == table_id));
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

Status DBMetaImpl::add_group_file(TableFileSchema& group_file) {
    if (group_file.date == EmptyDate) {
        group_file.date = Meta::GetDate();
    }
    TableSchema group_info;
    group_info.table_id = group_file.table_id;
    auto status = get_group(group_info);
    if (!status.ok()) {
        return status;
    }

    NextFileId(group_file.file_id);
    group_file.file_type = TableFileSchema::NEW;
    group_file.dimension = group_info.dimension;
    group_file.size = 0;
    group_file.created_on = utils::GetMicroSecTimeStamp();
    group_file.updated_time = group_file.created_on;
    GetGroupFilePath(group_file);

    {
        try {
            auto id = ConnectorPtr->insert(group_file);
            group_file.id = id;
        } catch (...) {
            return Status::DBTransactionError("Add file Error");
        }
    }

    auto partition_path = GetGroupDatePartitionPath(group_file.table_id, group_file.date);

    if (!boost::filesystem::is_directory(partition_path)) {
        auto ret = boost::filesystem::create_directory(partition_path);
        if (!ret) {
            LOG(ERROR) << "Create directory " << partition_path << " Error";
        }
        assert(ret);
    }

    return Status::OK();
}

Status DBMetaImpl::files_to_index(TableFilesSchema& files) {
    files.clear();

    try {
        auto selected = ConnectorPtr->select(columns(&TableFileSchema::id,
                                                   &TableFileSchema::table_id,
                                                   &TableFileSchema::file_id,
                                                   &TableFileSchema::file_type,
                                                   &TableFileSchema::size,
                                                   &TableFileSchema::date),
                                          where(c(&TableFileSchema::file_type) == (int)TableFileSchema::TO_INDEX));

        std::map<std::string, TableSchema> groups;

        for (auto& file : selected) {
            TableFileSchema group_file;
            group_file.id = std::get<0>(file);
            group_file.table_id = std::get<1>(file);
            group_file.file_id = std::get<2>(file);
            group_file.file_type = std::get<3>(file);
            group_file.size = std::get<4>(file);
            group_file.date = std::get<5>(file);
            GetGroupFilePath(group_file);
            auto groupItr = groups.find(group_file.table_id);
            if (groupItr == groups.end()) {
                TableSchema group_info;
                group_info.table_id = group_file.table_id;
                auto status = get_group_no_lock(group_info);
                if (!status.ok()) {
                    return status;
                }
                groups[group_file.table_id] = group_info;
            }
            group_file.dimension = groups[group_file.table_id].dimension;
            files.push_back(group_file);
        }
    } catch (std::exception & e) {
        LOG(DEBUG) << e.what();
        throw e;
    }

    return Status::OK();
}

Status DBMetaImpl::files_to_search(const std::string &table_id,
                                   const DatesT& partition,
                                   DatePartionedTableFilesSchema &files) {
    files.clear();
    DatesT today = {Meta::GetDate()};
    const DatesT& dates = (partition.empty() == true) ? today : partition;

    try {
        auto selected = ConnectorPtr->select(columns(&TableFileSchema::id,
                                                     &TableFileSchema::table_id,
                                                     &TableFileSchema::file_id,
                                                     &TableFileSchema::file_type,
                                                     &TableFileSchema::size,
                                                     &TableFileSchema::date),
                                             where(c(&TableFileSchema::table_id) == table_id and
                                                 in(&TableFileSchema::date, dates) and
                                                 (c(&TableFileSchema::file_type) == (int) TableFileSchema::RAW or
                                                     c(&TableFileSchema::file_type) == (int) TableFileSchema::TO_INDEX or
                                                     c(&TableFileSchema::file_type) == (int) TableFileSchema::INDEX)));

        TableSchema group_info;
        group_info.table_id = table_id;
        auto status = get_group_no_lock(group_info);
        if (!status.ok()) {
            return status;
        }

        for (auto& file : selected) {
            TableFileSchema group_file;
            group_file.id = std::get<0>(file);
            group_file.table_id = std::get<1>(file);
            group_file.file_id = std::get<2>(file);
            group_file.file_type = std::get<3>(file);
            group_file.size = std::get<4>(file);
            group_file.date = std::get<5>(file);
            group_file.dimension = group_info.dimension;
            GetGroupFilePath(group_file);
            auto dateItr = files.find(group_file.date);
            if (dateItr == files.end()) {
                files[group_file.date] = TableFilesSchema();
            }
            files[group_file.date].push_back(group_file);
        }
    } catch (std::exception & e) {
        LOG(DEBUG) << e.what();
        throw e;
    }

    return Status::OK();
}

Status DBMetaImpl::files_to_merge(const std::string& table_id,
        DatePartionedTableFilesSchema& files) {
    files.clear();

    try {
        auto selected = ConnectorPtr->select(columns(&TableFileSchema::id,
                                                   &TableFileSchema::table_id,
                                                   &TableFileSchema::file_id,
                                                   &TableFileSchema::file_type,
                                                   &TableFileSchema::size,
                                                   &TableFileSchema::date),
                                          where(c(&TableFileSchema::file_type) == (int)TableFileSchema::RAW and
                                                c(&TableFileSchema::table_id) == table_id));

        TableSchema group_info;
        group_info.table_id = table_id;
        auto status = get_group_no_lock(group_info);
        if (!status.ok()) {
            return status;
        }

        for (auto& file : selected) {
            TableFileSchema group_file;
            group_file.id = std::get<0>(file);
            group_file.table_id = std::get<1>(file);
            group_file.file_id = std::get<2>(file);
            group_file.file_type = std::get<3>(file);
            group_file.size = std::get<4>(file);
            group_file.date = std::get<5>(file);
            group_file.dimension = group_info.dimension;
            GetGroupFilePath(group_file);
            auto dateItr = files.find(group_file.date);
            if (dateItr == files.end()) {
                files[group_file.date] = TableFilesSchema();
            }
            files[group_file.date].push_back(group_file);
        }
    } catch (std::exception & e) {
        LOG(DEBUG) << e.what();
        throw e;
    }

    return Status::OK();
}

Status DBMetaImpl::has_group_file(const std::string& table_id_,
                              const std::string& file_id_,
                              bool& has_or_not_) {
    //PXU TODO
    return Status::OK();
}

Status DBMetaImpl::get_group_file(const std::string& table_id_,
                              const std::string& file_id_,
                              TableFileSchema& group_file_info_) {
    try {
        auto files = ConnectorPtr->select(columns(&TableFileSchema::id,
                                                   &TableFileSchema::table_id,
                                                   &TableFileSchema::file_id,
                                                   &TableFileSchema::file_type,
                                                   &TableFileSchema::size,
                                                   &TableFileSchema::date),
                                          where(c(&TableFileSchema::file_id) == file_id_ and
                                                c(&TableFileSchema::table_id) == table_id_
                                          ));
        assert(files.size() <= 1);
        if (files.size() == 1) {
            group_file_info_.id = std::get<0>(files[0]);
            group_file_info_.table_id = std::get<1>(files[0]);
            group_file_info_.file_id = std::get<2>(files[0]);
            group_file_info_.file_type = std::get<3>(files[0]);
            group_file_info_.size = std::get<4>(files[0]);
            group_file_info_.date = std::get<5>(files[0]);
        } else {
            return Status::NotFound("GroupFile " + file_id_ + " not found");
        }
    } catch (std::exception &e) {
        LOG(DEBUG) << e.what();
        throw e;
    }

    return Status::OK();
}

Status DBMetaImpl::get_group_files(const std::string& table_id_,
                               const int date_delta_,
                               TableFilesSchema& group_files_info_) {
    // PXU TODO
    return Status::OK();
}

// PXU TODO: Support Swap
Status DBMetaImpl::archive_files() {
    auto& criterias = _options.archive_conf.GetCriterias();
    if (criterias.size() == 0) {
        return Status::OK();
    }

    for (auto kv : criterias) {
        auto& criteria = kv.first;
        auto& limit = kv.second;
        if (criteria == "days") {
            long usecs = limit * D_SEC * US_PS;
            long now = utils::GetMicroSecTimeStamp();
            try
            {
                ConnectorPtr->update_all(
                        set(
                            c(&TableFileSchema::file_type) = (int)TableFileSchema::TO_DELETE
                           ),
                        where(
                            c(&TableFileSchema::created_on) < (long)(now - usecs) and
                            c(&TableFileSchema::file_type) != (int)TableFileSchema::TO_DELETE
                            ));
            } catch (std::exception & e) {
                LOG(DEBUG) << e.what();
                throw e;
            }
        }
        if (criteria == "disk") {
            long sum = 0;
            size(sum);

            // PXU TODO: refactor size
            auto to_delete = (sum - limit*G);
            discard_files_of_size(to_delete);
        }
    }

    return Status::OK();
}

Status DBMetaImpl::size(long& result) {
    result = 0;
    try {
        auto selected = ConnectorPtr->select(columns(sum(&TableFileSchema::size)),
                where(
                    c(&TableFileSchema::file_type) != (int)TableFileSchema::TO_DELETE
                    ));

        for (auto& sub_query : selected) {
            if(!std::get<0>(sub_query)) {
                continue;
            }
            result += (long)(*std::get<0>(sub_query));
        }
    } catch (std::exception & e) {
        LOG(DEBUG) << e.what();
        throw e;
    }

    return Status::OK();
}

Status DBMetaImpl::discard_files_of_size(long to_discard_size) {
    LOG(DEBUG) << "Abort to discard size=" << to_discard_size;
    if (to_discard_size <= 0) {
        return Status::OK();
    }
    try {
        auto selected = ConnectorPtr->select(columns(&TableFileSchema::id,
                                                   &TableFileSchema::size),
                                          where(c(&TableFileSchema::file_type) != (int)TableFileSchema::TO_DELETE),
                                          order_by(&TableFileSchema::id),
                                          limit(10));
        std::vector<int> ids;

        for (auto& file : selected) {
            if (to_discard_size <= 0) break;
            TableFileSchema group_file;
            group_file.id = std::get<0>(file);
            group_file.size = std::get<1>(file);
            ids.push_back(group_file.id);
            LOG(DEBUG) << "Discard group_file.id=" << group_file.id << " group_file.size=" << group_file.size;
            to_discard_size -= group_file.size;
        }

        if (ids.size() == 0) {
            return Status::OK();
        }

        ConnectorPtr->update_all(
                    set(
                        c(&TableFileSchema::file_type) = (int)TableFileSchema::TO_DELETE
                    ),
                    where(
                        in(&TableFileSchema::id, ids)
                    ));

    } catch (std::exception & e) {
        LOG(DEBUG) << e.what();
        throw e;
    }


    return discard_files_of_size(to_discard_size);
}

Status DBMetaImpl::update_group_file(TableFileSchema& group_file) {
    group_file.updated_time = utils::GetMicroSecTimeStamp();
    try {
        ConnectorPtr->update(group_file);
    } catch (std::exception & e) {
        LOG(DEBUG) << e.what();
        LOG(DEBUG) << "id= " << group_file.id << " file_id=" << group_file.file_id;
        throw e;
    }
    return Status::OK();
}

Status DBMetaImpl::update_files(TableFilesSchema& files) {
    try {
        auto commited = ConnectorPtr->transaction([&] () mutable {
            for (auto& file : files) {
                file.updated_time = utils::GetMicroSecTimeStamp();
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
    auto now = utils::GetMicroSecTimeStamp();
    try {
        auto selected = ConnectorPtr->select(columns(&TableFileSchema::id,
                                                   &TableFileSchema::table_id,
                                                   &TableFileSchema::file_id,
                                                   &TableFileSchema::file_type,
                                                   &TableFileSchema::size,
                                                   &TableFileSchema::date),
                                          where(c(&TableFileSchema::file_type) == (int)TableFileSchema::TO_DELETE and
                                                c(&TableFileSchema::updated_time) > now - seconds*US_PS));

        TableFilesSchema updated;

        for (auto& file : selected) {
            TableFileSchema group_file;
            group_file.id = std::get<0>(file);
            group_file.table_id = std::get<1>(file);
            group_file.file_id = std::get<2>(file);
            group_file.file_type = std::get<3>(file);
            group_file.size = std::get<4>(file);
            group_file.date = std::get<5>(file);
            GetGroupFilePath(group_file);
            if (group_file.file_type == TableFileSchema::TO_DELETE) {
                boost::filesystem::remove(group_file.location);
            }
            ConnectorPtr->remove<TableFileSchema>(group_file.id);
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
        auto selected = ConnectorPtr->select(columns(&TableFileSchema::id,
                                                   &TableFileSchema::table_id,
                                                   &TableFileSchema::file_id,
                                                   &TableFileSchema::file_type,
                                                   &TableFileSchema::size,
                                                   &TableFileSchema::date),
                                          where(c(&TableFileSchema::file_type) == (int)TableFileSchema::TO_DELETE or
                                                c(&TableFileSchema::file_type) == (int)TableFileSchema::NEW));

        TableFilesSchema updated;

        for (auto& file : selected) {
            TableFileSchema group_file;
            group_file.id = std::get<0>(file);
            group_file.table_id = std::get<1>(file);
            group_file.file_id = std::get<2>(file);
            group_file.file_type = std::get<3>(file);
            group_file.size = std::get<4>(file);
            group_file.date = std::get<5>(file);
            GetGroupFilePath(group_file);
            if (group_file.file_type == TableFileSchema::TO_DELETE) {
                boost::filesystem::remove(group_file.location);
            }
            ConnectorPtr->remove<TableFileSchema>(group_file.id);
            /* LOG(DEBUG) << "Removing id=" << group_file.id << " location=" << group_file.location << std::endl; */
        }
    } catch (std::exception & e) {
        LOG(DEBUG) << e.what();
        throw e;
    }

    return Status::OK();
}

Status DBMetaImpl::count(const std::string& table_id, long& result) {

    try {
        auto selected = ConnectorPtr->select(columns(&TableFileSchema::size,
                                                   &TableFileSchema::date),
                                          where((c(&TableFileSchema::file_type) == (int)TableFileSchema::RAW or
                                                 c(&TableFileSchema::file_type) == (int)TableFileSchema::TO_INDEX or
                                                 c(&TableFileSchema::file_type) == (int)TableFileSchema::INDEX) and
                                                c(&TableFileSchema::table_id) == table_id));

        TableSchema group_info;
        group_info.table_id = table_id;
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
