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

long GetFileSize(const std::string& filename)
{
    struct stat stat_buf;
    int rc = stat(filename.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

std::string LocalMetaImpl::GetGroupPath(const std::string& group_id) {
    return _options.path + "/" + group_id;
}

std::string LocalMetaImpl::GetGroupMetaPath(const std::string& group_id) {
    return GetGroupPath(group_id) + "/" + "meta";
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

Status LocalMetaImpl::add_group(const GroupOptions& options,
            const std::string& group_id,
            GroupSchema& group_info) {
    std::string real_gid;
    size_t id = SimpleIDGenerator().getNextIDNumber();
    if (group_id == "") {
        std::stringstream ss;
        ss << id;
        real_gid = ss.str();
    } else {
        real_gid = group_id;
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
    group_info.dimension = options.dimension;

    boost::property_tree::ptree out;
    out.put("files_cnt", group_info.files_cnt);
    out.put("dimension", group_info.dimension);
    boost::property_tree::write_json(GetGroupMetaPath(real_gid), out);

    return Status::OK();
}

Status LocalMetaImpl::get_group(const std::string& group_id, GroupSchema& group_info) {
    bool group_exist;
    has_group(group_id, group_exist);
    if (!group_exist) {
        return Status::NotFound("Group " + group_id + " Not Found");
    }

    boost::property_tree::ptree ptree;
    boost::property_tree::read_json(GetGroupMetaPath(group_id), ptree);
    auto files_cnt = ptree.get_child("files_cnt").data();
    auto dimension = ptree.get_child("dimension").data();
    std::cout << dimension << std::endl;
    std::cout << files_cnt << std::endl;

    group_info.id = std::stoi(group_id);
    group_info.group_id = group_id;
    group_info.files_cnt = std::stoi(files_cnt);
    group_info.dimension = std::stoi(dimension);
    group_info.location = GetGroupPath(group_id);

    return Status::OK();
}

Status LocalMetaImpl::has_group(const std::string& group_id, bool& has_or_not) {
    has_or_not = boost::filesystem::is_directory(GetGroupPath(group_id));
    /* if (!has_or_not) return Status::OK(); */
    /* boost::filesystem::is_regular_file() */
    return Status::OK();
}

Status LocalMetaImpl::add_group_file(const std::string& group_id,
                              GroupFileSchema& group_file_info,
                              GroupFileSchema::FILE_TYPE file_type) {
    return add_group_file(group_id, Meta::GetDate(), group_file_info);
}

Status LocalMetaImpl::add_group_file(const std::string& group_id,
                              DateT date,
                              GroupFileSchema& group_file_info,
                              GroupFileSchema::FILE_TYPE file_type) {
    //PXU TODO
    std::stringstream ss;
    SimpleIDGenerator g;
    std::string suffix = (file_type == GroupFileSchema::RAW) ? ".raw" : ".index";
    ss << "/tmp/test/" << date
                       << "/" << g.getNextIDNumber()
                       << suffix;
    group_file_info.group_id = "1";
    group_file_info.dimension = 64;
    group_file_info.location = ss.str();
    group_file_info.date = date;
    return Status::OK();
}

Status LocalMetaImpl::files_to_index(GroupFilesSchema& files) {
    // PXU TODO
    files.clear();
    std::stringstream ss;
    ss << "/tmp/test/" << Meta::GetDate();
    boost::filesystem::path path(ss.str().c_str());
    boost::filesystem::directory_iterator end_itr;
    for (boost::filesystem::directory_iterator itr(path); itr != end_itr; ++itr) {
        /* std::cout << itr->path().string() << std::endl; */
        GroupFileSchema f;
        f.location = itr->path().string();
        std::string suffixStr = f.location.substr(f.location.find_last_of('.') + 1);
        if (suffixStr == "index") continue;
        if (1024*1024*1000 >= GetFileSize(f.location)) continue;
        std::cout << "[About to index] " << f.location << std::endl;
        f.date = Meta::GetDate();
        files.push_back(f);
    }
    return Status::OK();
}

Status LocalMetaImpl::files_to_merge(const std::string& group_id,
        DatePartionedGroupFilesSchema& files) {
    //PXU TODO
    files.clear();
    std::stringstream ss;
    ss << "/tmp/test/" << Meta::GetDate();
    boost::filesystem::path path(ss.str().c_str());
    boost::filesystem::directory_iterator end_itr;
    GroupFilesSchema gfiles;
    DateT date = Meta::GetDate();
    files[date] = gfiles;
    for (boost::filesystem::directory_iterator itr(path); itr != end_itr; ++itr) {
        /* std::cout << itr->path().string() << std::endl; */
        GroupFileSchema f;
        f.location = itr->path().string();
        std::string suffixStr = f.location.substr(f.location.find_last_of('.') + 1);
        if (suffixStr == "index") continue;
        if (1024*1024*1000 < GetFileSize(f.location)) continue;
        std::cout << "About to merge " << f.location << std::endl;
        files[date].push_back(f);
    }

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

Status LocalMetaImpl::update_group_file(const GroupFileSchema& group_file_) {
    //PXU TODO
    return Status::OK();
}

Status LocalMetaImpl::update_files(const GroupFilesSchema& files) {
    //PXU TODO
    return Status::OK();
}

} // namespace meta
} // namespace engine
} // namespace vecwise
} // namespace zilliz
