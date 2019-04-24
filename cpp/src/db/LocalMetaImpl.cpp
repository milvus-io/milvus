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

std::string LocalMetaImpl::GetGroupPath(const std::string& group_id) {
    return _options.path + "/" + group_id;
}

std::string LocalMetaImpl::GetGroupDatePartitionPath(const std::string& group_id, DateT& date) {
    std::stringstream ss;
    ss << GetGroupPath(group_id) << "/" << date;
    return ss.str();
}

std::string LocalMetaImpl::GetNextGroupFileLocationByPartition(const std::string& group_id, DateT& date,
        GroupFileSchema::FILE_TYPE file_type) {
    std::string suffix = (file_type == GroupFileSchema::RAW) ? ".raw" : ".index";
    SimpleIDGenerator g;
    std::stringstream ss;
    ss << GetGroupPath(group_id) << "/" << date << "/" << g.getNextIDNumber() << suffix;
    return ss.str();
}

std::string LocalMetaImpl::GetGroupMetaPathByGroupPath(const std::string& group_path) {
    return group_path + "/" + "meta";
}

std::string LocalMetaImpl::GetGroupMetaPath(const std::string& group_id) {
    return GetGroupMetaPathByGroupPath(GetGroupPath(group_id));
}

Status LocalMetaImpl::GetGroupMetaInfoByPath(const std::string& path, GroupSchema& group_info) {
    boost::property_tree::ptree ptree;
    boost::property_tree::read_json(path, ptree);
    auto files_cnt = ptree.get_child("files_cnt").data();
    auto dimension = ptree.get_child("dimension").data();
    /* std::cout << dimension << std::endl; */
    /* std::cout << files_cnt << std::endl; */

    group_info.id = std::stoi(group_info.group_id);
    group_info.files_cnt = std::stoi(files_cnt);
    group_info.dimension = std::stoi(dimension);
    group_info.location = GetGroupPath(group_info.group_id);

    return Status::OK();

}

Status LocalMetaImpl::GetGroupMetaInfo(const std::string& group_id, GroupSchema& group_info) {
    group_info.group_id = group_id;
    return GetGroupMetaInfoByPath(GetGroupMetaPath(group_id), group_info);
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

Status LocalMetaImpl::add_group(GroupSchema& group_info) {
    std::string real_gid;
    size_t id = SimpleIDGenerator().getNextIDNumber();
    if (group_info.group_id == "") {
        std::stringstream ss;
        ss << id;
        real_gid = ss.str();
    } else {
        real_gid = group_info.group_id;
    }

    bool group_exist;
    has_group(real_gid, group_exist);
    if (group_exist) {
        return Status::GroupError("Group Already Existed " + real_gid);
    }
    if (!boost::filesystem::create_directory(GetGroupPath(real_gid))) {
        return Status::GroupError("Cannot Create Group " + real_gid);
    }

    group_info.group_id = real_gid;
    group_info.files_cnt = 0;
    group_info.id = 0;
    group_info.location = GetGroupPath(real_gid);

    boost::property_tree::ptree out;
    out.put("files_cnt", group_info.files_cnt);
    out.put("dimension", group_info.dimension);
    boost::property_tree::write_json(GetGroupMetaPath(real_gid), out);

    return Status::OK();
}

Status LocalMetaImpl::get_group(GroupSchema& group_info) {
    bool group_exist;
    has_group(group_info.group_id, group_exist);
    if (!group_exist) {
        return Status::NotFound("Group " + group_info.group_id + " Not Found");
    }

    return GetGroupMetaInfo(group_info.group_id, group_info);
}

Status LocalMetaImpl::has_group(const std::string& group_id, bool& has_or_not) {
    has_or_not = boost::filesystem::is_directory(GetGroupPath(group_id));
    return Status::OK();
}

Status LocalMetaImpl::add_group_file(GroupFileSchema& group_file_info) {
    GroupSchema group_info;
    /* auto status = get_group(group_info); */
    /* if (!status.ok()) { */
    /*     return status; */
    /* } */
    /* auto location = GetNextGroupFileLocationByPartition(group_id, date, file_type); */
    /* group_file_info.group_id = group_id; */
    /* group_file_info.dimension = group_info.dimension; */
    /* group_file_info.location = location; */
    /* group_file_info.date = date; */
    return Status::OK();
}

Status LocalMetaImpl::files_to_index(GroupFilesSchema& files) {
    files.clear();

    std::string suffix;
    boost::filesystem::directory_iterator end_itr;
    for (boost::filesystem::directory_iterator itr(_options.path); itr != end_itr; ++itr) {
        auto group_path = itr->path().string();
        GroupSchema group_info;
        GetGroupMetaInfoByPath(GetGroupMetaPathByGroupPath(group_path), group_info);
        for (boost::filesystem::directory_iterator innerItr(group_path); innerItr != end_itr; ++innerItr) {
            auto partition_path = innerItr->path().string();
            for (boost::filesystem::directory_iterator fItr(partition_path); fItr != end_itr; ++fItr) {
                auto location = fItr->path().string();
                suffix = location.substr(location.find_last_of('.') + 1);
                if (suffix == "index") continue;
                if (INDEX_TRIGGER_SIZE >= GetFileSize(location)) continue;
                std::cout << "[About to index] " << location << std::endl;
                GroupFileSchema f;
                f.location = location;
                /* f.group_id = group_id; */
                f.dimension = group_info.dimension;
                files.push_back(f);
            }
        }
    }

    return Status::OK();
}

Status LocalMetaImpl::files_to_merge(const std::string& group_id,
        DatePartionedGroupFilesSchema& files) {
    files.clear();
    /* std::string suffix; */
    /* boost::filesystem::directory_iterator end_itr; */
    /* for (boost::filesystem::directory_iterator itr(_options.path); itr != end_itr; ++itr) { */
    /*     auto group_path = itr->path().string(); */
    /*     GroupSchema group_info; */
    /*     GetGroupMetaInfoByPath(GetGroupMetaPathByGroupPath(group_path), group_info); */
    /*     for (boost::filesystem::directory_iterator innerItr(group_path); innerItr != end_itr; ++innerItr) { */
    /*         auto partition_path = innerItr->path().string(); */
    /*         for (boost::filesystem::directory_iterator fItr(partition_path); fItr != end_itr; ++fItr) { */
    /*             auto location = fItr->path().string(); */
    /*             suffix = location.substr(location.find_last_of('.') + 1); */
    /*             if (suffix == "index") continue; */
    /*             if (INDEX_TRIGGER_SIZE < GetFileSize(location)) continue; */
    /*             std::cout << "[About to index] " << location << std::endl; */
    /*             GroupFileSchema f; */
    /*             f.location = location; */
    /*             f.group_id = group_id; */
    /*             f.dimension = group_info.dimension; */
    /*             files.push_back(f); */
    /*         } */
    /*     } */
    /* } */

    return Status::OK();
}

Status LocalMetaImpl::has_group_file(const std::string& group_id_,
                              const std::string& file_id_,
                              bool& has_or_not_) {
    //PXU TODO
    return Status::OK();
}

Status LocalMetaImpl::get_group_file(const std::string& group_id_,
                              const std::string& file_id_,
                              GroupFileSchema& group_file_info_) {
    //PXU TODO
    return Status::OK();
}

Status LocalMetaImpl::get_group_files(const std::string& group_id_,
                               const int date_delta_,
                               GroupFilesSchema& group_files_info_) {
    // PXU TODO
    return Status::OK();
}

Status LocalMetaImpl::update_group_file(GroupFileSchema& group_file_) {
    //PXU TODO
    return Status::OK();
}

Status LocalMetaImpl::update_files(GroupFilesSchema& files) {
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

Status LocalMetaImpl::count(const std::string& group_id, long& result) {
    // PXU TODO
    return Status::OK();
}

} // namespace meta
} // namespace engine
} // namespace vecwise
} // namespace zilliz
