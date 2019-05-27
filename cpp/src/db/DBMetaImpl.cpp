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

    CleanUp();

    return Status::OK();
}

// PXU TODO: Temp solution. Will fix later
Status DBMetaImpl::DropPartitionsByDates(const std::string& table_id,
            const DatesT& dates) {
    if (dates.size() == 0) {
        return Status::OK();
    }

    TableSchema table_schema;
    table_schema.table_id = table_id;
    auto status = DescribeTable(table_schema);
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

Status DBMetaImpl::CreateTable(TableSchema& table_schema) {
    if (table_schema.table_id == "") {
        NextGroupId(table_schema.table_id);
    }
    table_schema.files_cnt = 0;
    table_schema.id = -1;
    table_schema.created_on = utils::GetMicroSecTimeStamp();

    {
        try {
            auto id = ConnectorPtr->insert(table_schema);
            table_schema.id = id;
        } catch (...) {
            return Status::DBTransactionError("Add Group Error");
        }
    }

    auto group_path = GetGroupPath(table_schema.table_id);

    if (!boost::filesystem::is_directory(group_path)) {
        auto ret = boost::filesystem::create_directories(group_path);
        if (!ret) {
            LOG(ERROR) << "Create directory " << group_path << " Error";
        }
        assert(ret);
    }

    return Status::OK();
}

Status DBMetaImpl::DescribeTable(TableSchema& table_schema) {
    try {
        auto groups = ConnectorPtr->select(columns(&TableSchema::id,
                                                  &TableSchema::table_id,
                                                  &TableSchema::files_cnt,
                                                  &TableSchema::dimension),
                                          where(c(&TableSchema::table_id) == table_schema.table_id));
        assert(groups.size() <= 1);
        if (groups.size() == 1) {
            table_schema.id = std::get<0>(groups[0]);
            table_schema.files_cnt = std::get<2>(groups[0]);
            table_schema.dimension = std::get<3>(groups[0]);
        } else {
            return Status::NotFound("Group " + table_schema.table_id + " not found");
        }
    } catch (std::exception &e) {
        LOG(DEBUG) << e.what();
        throw e;
    }

    return Status::OK();
}

Status DBMetaImpl::HasTable(const std::string& table_id, bool& has_or_not) {
    try {
        auto tables = ConnectorPtr->select(columns(&TableSchema::id),
                                          where(c(&TableSchema::table_id) == table_id));
        assert(tables.size() <= 1);
        if (tables.size() == 1) {
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

Status DBMetaImpl::CreateTableFile(TableFileSchema& file_schema) {
    if (file_schema.date == EmptyDate) {
        file_schema.date = Meta::GetDate();
    }
    TableSchema table_schema;
    table_schema.table_id = file_schema.table_id;
    auto status = DescribeTable(table_schema);
    if (!status.ok()) {
        return status;
    }

    NextFileId(file_schema.file_id);
    file_schema.file_type = TableFileSchema::NEW;
    file_schema.dimension = table_schema.dimension;
    file_schema.size = 0;
    file_schema.created_on = utils::GetMicroSecTimeStamp();
    file_schema.updated_time = file_schema.created_on;
    GetGroupFilePath(file_schema);

    {
        try {
            auto id = ConnectorPtr->insert(file_schema);
            file_schema.id = id;
        } catch (...) {
            return Status::DBTransactionError("Add file Error");
        }
    }

    auto partition_path = GetGroupDatePartitionPath(file_schema.table_id, file_schema.date);

    if (!boost::filesystem::is_directory(partition_path)) {
        auto ret = boost::filesystem::create_directory(partition_path);
        if (!ret) {
            LOG(ERROR) << "Create directory " << partition_path << " Error";
        }
        assert(ret);
    }

    return Status::OK();
}

Status DBMetaImpl::FilesToIndex(TableFilesSchema& files) {
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
        TableFileSchema table_file;

        for (auto& file : selected) {
            table_file.id = std::get<0>(file);
            table_file.table_id = std::get<1>(file);
            table_file.file_id = std::get<2>(file);
            table_file.file_type = std::get<3>(file);
            table_file.size = std::get<4>(file);
            table_file.date = std::get<5>(file);
            GetGroupFilePath(table_file);
            auto groupItr = groups.find(table_file.table_id);
            if (groupItr == groups.end()) {
                TableSchema table_schema;
                table_schema.table_id = table_file.table_id;
                auto status = DescribeTable(table_schema);
                if (!status.ok()) {
                    return status;
                }
                groups[table_file.table_id] = table_schema;
            }
            table_file.dimension = groups[table_file.table_id].dimension;
            files.push_back(table_file);
        }
    } catch (std::exception & e) {
        LOG(DEBUG) << e.what();
        throw e;
    }

    return Status::OK();
}

Status DBMetaImpl::FilesToSearch(const std::string &table_id,
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

        TableSchema table_schema;
        table_schema.table_id = table_id;
        auto status = DescribeTable(table_schema);
        if (!status.ok()) {
            return status;
        }

        TableFileSchema table_file;

        for (auto& file : selected) {
            table_file.id = std::get<0>(file);
            table_file.table_id = std::get<1>(file);
            table_file.file_id = std::get<2>(file);
            table_file.file_type = std::get<3>(file);
            table_file.size = std::get<4>(file);
            table_file.date = std::get<5>(file);
            table_file.dimension = table_schema.dimension;
            GetGroupFilePath(table_file);
            auto dateItr = files.find(table_file.date);
            if (dateItr == files.end()) {
                files[table_file.date] = TableFilesSchema();
            }
            files[table_file.date].push_back(table_file);
        }
    } catch (std::exception & e) {
        LOG(DEBUG) << e.what();
        throw e;
    }

    return Status::OK();
}

Status DBMetaImpl::FilesToMerge(const std::string& table_id,
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

        TableSchema table_schema;
        table_schema.table_id = table_id;
        auto status = DescribeTable(table_schema);
        if (!status.ok()) {
            return status;
        }

        TableFileSchema table_file;
        for (auto& file : selected) {
            table_file.id = std::get<0>(file);
            table_file.table_id = std::get<1>(file);
            table_file.file_id = std::get<2>(file);
            table_file.file_type = std::get<3>(file);
            table_file.size = std::get<4>(file);
            table_file.date = std::get<5>(file);
            table_file.dimension = table_schema.dimension;
            GetGroupFilePath(table_file);
            auto dateItr = files.find(table_file.date);
            if (dateItr == files.end()) {
                files[table_file.date] = TableFilesSchema();
            }
            files[table_file.date].push_back(table_file);
        }
    } catch (std::exception & e) {
        LOG(DEBUG) << e.what();
        throw e;
    }

    return Status::OK();
}

Status DBMetaImpl::GetTableFile(TableFileSchema& file_schema) {

    try {
        auto files = ConnectorPtr->select(columns(&TableFileSchema::id,
                                                   &TableFileSchema::table_id,
                                                   &TableFileSchema::file_id,
                                                   &TableFileSchema::file_type,
                                                   &TableFileSchema::size,
                                                   &TableFileSchema::date),
                                          where(c(&TableFileSchema::file_id) == file_schema.file_id and
                                                c(&TableFileSchema::table_id) == file_schema.table_id
                                          ));
        assert(files.size() <= 1);
        if (files.size() == 1) {
            file_schema.id = std::get<0>(files[0]);
            file_schema.table_id = std::get<1>(files[0]);
            file_schema.file_id = std::get<2>(files[0]);
            file_schema.file_type = std::get<3>(files[0]);
            file_schema.size = std::get<4>(files[0]);
            file_schema.date = std::get<5>(files[0]);
        } else {
            return Status::NotFound("Table:" + file_schema.table_id +
                    " File:" + file_schema.file_id + " not found");
        }
    } catch (std::exception &e) {
        LOG(DEBUG) << e.what();
        throw e;
    }

    return Status::OK();
}

// PXU TODO: Support Swap
Status DBMetaImpl::Archive() {
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
            Size(sum);

            // PXU TODO: refactor size
            auto to_delete = (sum - limit*G);
            discard_files_of_size(to_delete);
        }
    }

    return Status::OK();
}

Status DBMetaImpl::Size(long& result) {
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
    LOG(DEBUG) << "About to discard size=" << to_discard_size;
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
        TableFileSchema table_file;

        for (auto& file : selected) {
            if (to_discard_size <= 0) break;
            table_file.id = std::get<0>(file);
            table_file.size = std::get<1>(file);
            ids.push_back(table_file.id);
            LOG(DEBUG) << "Discard table_file.id=" << table_file.file_id << " table_file.size=" << table_file.size;
            to_discard_size -= table_file.size;
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

Status DBMetaImpl::UpdateTableFile(TableFileSchema& file_schema) {
    file_schema.updated_time = utils::GetMicroSecTimeStamp();
    try {
        ConnectorPtr->update(file_schema);
    } catch (std::exception & e) {
        LOG(DEBUG) << e.what();
        LOG(DEBUG) << "table_id= " << file_schema.table_id << " file_id=" << file_schema.file_id;
        throw e;
    }
    return Status::OK();
}

Status DBMetaImpl::UpdateTableFiles(TableFilesSchema& files) {
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

Status DBMetaImpl::CleanUpFilesWithTTL(uint16_t seconds) {
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
        TableFileSchema table_file;

        for (auto& file : selected) {
            table_file.id = std::get<0>(file);
            table_file.table_id = std::get<1>(file);
            table_file.file_id = std::get<2>(file);
            table_file.file_type = std::get<3>(file);
            table_file.size = std::get<4>(file);
            table_file.date = std::get<5>(file);
            GetGroupFilePath(table_file);
            if (table_file.file_type == TableFileSchema::TO_DELETE) {
                boost::filesystem::remove(table_file.location);
            }
            ConnectorPtr->remove<TableFileSchema>(table_file.id);
            /* LOG(DEBUG) << "Removing deleted id=" << table_file.id << " location=" << table_file.location << std::endl; */
        }
    } catch (std::exception & e) {
        LOG(DEBUG) << e.what();
        throw e;
    }

    return Status::OK();
}

Status DBMetaImpl::CleanUp() {
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
        TableFileSchema table_file;

        for (auto& file : selected) {
            table_file.id = std::get<0>(file);
            table_file.table_id = std::get<1>(file);
            table_file.file_id = std::get<2>(file);
            table_file.file_type = std::get<3>(file);
            table_file.size = std::get<4>(file);
            table_file.date = std::get<5>(file);
            GetGroupFilePath(table_file);
            if (table_file.file_type == TableFileSchema::TO_DELETE) {
                boost::filesystem::remove(table_file.location);
            }
            ConnectorPtr->remove<TableFileSchema>(table_file.id);
            /* LOG(DEBUG) << "Removing id=" << table_file.id << " location=" << table_file.location << std::endl; */
        }
    } catch (std::exception & e) {
        LOG(DEBUG) << e.what();
        throw e;
    }

    return Status::OK();
}

Status DBMetaImpl::Count(const std::string& table_id, long& result) {

    try {
        auto selected = ConnectorPtr->select(columns(&TableFileSchema::size,
                                                   &TableFileSchema::date),
                                          where((c(&TableFileSchema::file_type) == (int)TableFileSchema::RAW or
                                                 c(&TableFileSchema::file_type) == (int)TableFileSchema::TO_INDEX or
                                                 c(&TableFileSchema::file_type) == (int)TableFileSchema::INDEX) and
                                                c(&TableFileSchema::table_id) == table_id));

        TableSchema table_schema;
        table_schema.table_id = table_id;
        auto status = DescribeTable(table_schema);
        if (!status.ok()) {
            return status;
        }

        result = 0;
        for (auto& file : selected) {
            result += std::get<0>(file);
        }

        result /= table_schema.dimension;

    } catch (std::exception & e) {
        LOG(DEBUG) << e.what();
        throw e;
    }
    return Status::OK();
}

Status DBMetaImpl::DropAll() {
    if (boost::filesystem::is_directory(_options.path)) {
        boost::filesystem::remove_all(_options.path);
    }
    return Status::OK();
}

DBMetaImpl::~DBMetaImpl() {
    CleanUp();
}

} // namespace meta
} // namespace engine
} // namespace vecwise
} // namespace zilliz
