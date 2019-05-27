////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <sys/stat.h>
#include <unistd.h>
#include <sstream>
#include <iostream>
#include <boost/filesystem.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

#include <fstream>
#include "LocalMetaImpl.h"
#include "IDGenerator.h"

namespace zilliz {
namespace vecwise {
namespace engine {
namespace meta {

long LocalMetaImpl::GetFileSize(const std::string& filename)
{
    struct stat stat_buf;
    int rc = stat(filename.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

std::string LocalMetaImpl::GetGroupPath(const std::string& table_id) {
    return _options.path + "/" + table_id;
}

std::string LocalMetaImpl::GetGroupDatePartitionPath(const std::string& table_id, DateT& date) {
    std::stringstream ss;
    ss << GetGroupPath(table_id) << "/" << date;
    return ss.str();
}

std::string LocalMetaImpl::GetNextGroupFileLocationByPartition(const std::string& table_id, DateT& date,
        TableFileSchema::FILE_TYPE file_type) {
    std::string suffix = (file_type == TableFileSchema::RAW) ? ".raw" : ".index";
    SimpleIDGenerator g;
    std::stringstream ss;
    ss << GetGroupPath(table_id) << "/" << date << "/" << g.getNextIDNumber() << suffix;
    return ss.str();
}

std::string LocalMetaImpl::GetGroupMetaPathByGroupPath(const std::string& group_path) {
    return group_path + "/" + "meta";
}

std::string LocalMetaImpl::GetGroupMetaPath(const std::string& table_id) {
    return GetGroupMetaPathByGroupPath(GetGroupPath(table_id));
}

Status LocalMetaImpl::GetGroupMetaInfoByPath(const std::string& path, TableSchema& table_schema) {
    boost::property_tree::ptree ptree;
    boost::property_tree::read_json(path, ptree);
    auto files_cnt = ptree.get_child("files_cnt").data();
    auto dimension = ptree.get_child("dimension").data();
    /* std::cout << dimension << std::endl; */
    /* std::cout << files_cnt << std::endl; */

    table_schema.id = std::stoi(table_schema.table_id);
    table_schema.files_cnt = std::stoi(files_cnt);
    table_schema.dimension = std::stoi(dimension);
    table_schema.location = GetGroupPath(table_schema.table_id);

    return Status::OK();

}

Status LocalMetaImpl::GetGroupMetaInfo(const std::string& table_id, TableSchema& table_schema) {
    table_schema.table_id = table_id;
    return GetGroupMetaInfoByPath(GetGroupMetaPath(table_id), table_schema);
}

LocalMetaImpl::LocalMetaImpl(const DBMetaOptions& options_)
    : _options(options_) {
    initialize();
}

Status LocalMetaImpl::initialize() {
    if (boost::filesystem::is_directory(_options.path)) {
    }
    else if (!boost::filesystem::create_directory(_options.path)) {
        return Status::InvalidDBPath("Cannot Create " + _options.path);
    }
    return Status::OK();
}

Status LocalMetaImpl::CreateTable(TableSchema& table_schema) {
    std::string real_gid;
    size_t id = SimpleIDGenerator().getNextIDNumber();
    if (table_schema.table_id == "") {
        std::stringstream ss;
        ss << id;
        real_gid = ss.str();
    } else {
        real_gid = table_schema.table_id;
    }

    bool group_exist;
    has_group(real_gid, group_exist);
    if (group_exist) {
        return Status::GroupError("Group Already Existed " + real_gid);
    }
    if (!boost::filesystem::create_directory(GetGroupPath(real_gid))) {
        return Status::GroupError("Cannot Create Group " + real_gid);
    }

    table_schema.table_id = real_gid;
    table_schema.files_cnt = 0;
    table_schema.id = 0;
    table_schema.location = GetGroupPath(real_gid);

    boost::property_tree::ptree out;
    out.put("files_cnt", table_schema.files_cnt);
    out.put("dimension", table_schema.dimension);
    boost::property_tree::write_json(GetGroupMetaPath(real_gid), out);

    return Status::OK();
}

Status LocalMetaImpl::get_group(TableSchema& table_schema) {
    bool group_exist;
    has_group(table_schema.table_id, group_exist);
    if (!group_exist) {
        return Status::NotFound("Group " + table_schema.table_id + " Not Found");
    }

    return GetGroupMetaInfo(table_schema.table_id, table_schema);
}

Status LocalMetaImpl::has_group(const std::string& table_id, bool& has_or_not) {
    has_or_not = boost::filesystem::is_directory(GetGroupPath(table_id));
    return Status::OK();
}

Status LocalMetaImpl::add_group_file(TableFileSchema& group_file_info) {
    TableSchema table_schema;
    /* auto status = get_group(table_schema); */
    /* if (!status.ok()) { */
    /*     return status; */
    /* } */
    /* auto location = GetNextGroupFileLocationByPartition(table_id, date, file_type); */
    /* group_file_info.table_id = table_id; */
    /* group_file_info.dimension = table_schema.dimension; */
    /* group_file_info.location = location; */
    /* group_file_info.date = date; */
    return Status::OK();
}

Status LocalMetaImpl::files_to_index(TableFilesSchema& files) {
    files.clear();

    std::string suffix;
    boost::filesystem::directory_iterator end_itr;
    for (boost::filesystem::directory_iterator itr(_options.path); itr != end_itr; ++itr) {
        auto group_path = itr->path().string();
        TableSchema table_schema;
        GetGroupMetaInfoByPath(GetGroupMetaPathByGroupPath(group_path), table_schema);
        for (boost::filesystem::directory_iterator innerItr(group_path); innerItr != end_itr; ++innerItr) {
            auto partition_path = innerItr->path().string();
            for (boost::filesystem::directory_iterator fItr(partition_path); fItr != end_itr; ++fItr) {
                auto location = fItr->path().string();
                suffix = location.substr(location.find_last_of('.') + 1);
                if (suffix == "index") continue;
                if (INDEX_TRIGGER_SIZE >= GetFileSize(location)) continue;
                std::cout << "[About to index] " << location << std::endl;
                TableFileSchema f;
                f.location = location;
                /* f.table_id = table_id; */
                f.dimension = table_schema.dimension;
                files.push_back(f);
            }
        }
    }

    return Status::OK();
}

Status LocalMetaImpl::files_to_merge(const std::string& table_id,
        DatePartionedTableFilesSchema& files) {
    files.clear();
    /* std::string suffix; */
    /* boost::filesystem::directory_iterator end_itr; */
    /* for (boost::filesystem::directory_iterator itr(_options.path); itr != end_itr; ++itr) { */
    /*     auto group_path = itr->path().string(); */
    /*     TableSchema table_schema; */
    /*     GetGroupMetaInfoByPath(GetGroupMetaPathByGroupPath(group_path), table_schema); */
    /*     for (boost::filesystem::directory_iterator innerItr(group_path); innerItr != end_itr; ++innerItr) { */
    /*         auto partition_path = innerItr->path().string(); */
    /*         for (boost::filesystem::directory_iterator fItr(partition_path); fItr != end_itr; ++fItr) { */
    /*             auto location = fItr->path().string(); */
    /*             suffix = location.substr(location.find_last_of('.') + 1); */
    /*             if (suffix == "index") continue; */
    /*             if (INDEX_TRIGGER_SIZE < GetFileSize(location)) continue; */
    /*             std::cout << "[About to index] " << location << std::endl; */
    /*             TableFileSchema f; */
    /*             f.location = location; */
    /*             f.table_id = table_id; */
    /*             f.dimension = table_schema.dimension; */
    /*             files.push_back(f); */
    /*         } */
    /*     } */
    /* } */

    return Status::OK();
}

Status LocalMetaImpl::has_group_file(const std::string& table_id_,
                              const std::string& file_id_,
                              bool& has_or_not_) {
    //PXU TODO
    return Status::OK();
}

Status LocalMetaImpl::get_group_file(const std::string& table_id_,
                              const std::string& file_id_,
                              TableFileSchema& group_file_info_) {
    //PXU TODO
    return Status::OK();
}

Status LocalMetaImpl::get_group_files(const std::string& table_id_,
                               const int date_delta_,
                               TableFilesSchema& group_files_info_) {
    // PXU TODO
    return Status::OK();
}

Status LocalMetaImpl::update_group_file(TableFileSchema& group_file_) {
    //PXU TODO
    return Status::OK();
}

Status LocalMetaImpl::update_files(TableFilesSchema& files) {
    //PXU TODO
    return Status::OK();
}

Status LocalMetaImpl::archive_files() {
    //PXU TODO
    return Status::OK();
}

Status LocalMetaImpl::cleanup() {
    //PXU TODO
    return Status::OK();
}

Status LocalMetaImpl::cleanup_ttl_files(uint16_t seconds) {
    // PXU TODO
    return Status::OK();
}

Status LocalMetaImpl::drop_all() {
    // PXU TODO
    return Status::OK();
}

Status LocalMetaImpl::size(long& result) {
    // PXU TODO
    return Status::OK();
}

Status LocalMetaImpl::count(const std::string& table_id, long& result) {
    // PXU TODO
    return Status::OK();
}

} // namespace meta
} // namespace engine
} // namespace vecwise
} // namespace zilliz
