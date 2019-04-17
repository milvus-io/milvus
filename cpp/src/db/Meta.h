#pragma once

#include <string>
#include <cstddef>
#include <vector>
#include <map>
#include <ctime>
#include "Options.h"
#include "Status.h"

namespace zilliz {
namespace vecwise {
namespace engine {
namespace meta {

typedef int DateT;

struct GroupSchema {
    size_t id;
    std::string group_id;
    size_t files_cnt = 0;
    uint16_t dimension;
    std::string location = "";
}; // GroupSchema


struct GroupFileSchema {
    typedef enum {
        NEW,
        RAW,
        INDEX,
        TO_DELETE,
    } FILE_TYPE;

    size_t id;
    std::string group_id;
    std::string file_id;
    int file_type = NEW;
    size_t rows;
    DateT date;
    uint16_t dimension;
    std::string location = "";
}; // GroupFileSchema

typedef std::vector<GroupFileSchema> GroupFilesSchema;
typedef std::map<DateT, GroupFilesSchema> DatePartionedGroupFilesSchema;


class Meta {
public:
    virtual Status add_group(const GroupOptions& options_,
            const std::string& group_id_,
            GroupSchema& group_info_) = 0;
    virtual Status get_group(const std::string& group_id_, GroupSchema& group_info_) = 0;
    virtual Status has_group(const std::string& group_id_, bool& has_or_not_) = 0;

    virtual Status add_group_file(const std::string& group_id_,
                                  GroupFileSchema& group_file_info_,
                                  GroupFileSchema::FILE_TYPE file_type=GroupFileSchema::RAW) = 0;
    virtual Status add_group_file(const std::string& group_id,
                                  DateT date,
                                  GroupFileSchema& group_file_info,
                                  GroupFileSchema::FILE_TYPE file_type=GroupFileSchema::RAW) = 0;

    virtual Status has_group_file(const std::string& group_id_,
                                  const std::string& file_id_,
                                  bool& has_or_not_) = 0;
    virtual Status get_group_file(const std::string& group_id_,
                                  const std::string& file_id_,
                                  GroupFileSchema& group_file_info_) = 0;
    virtual Status update_group_file(const GroupFileSchema& group_file_) = 0;

    virtual Status get_group_files(const std::string& group_id_,
                                   const int date_delta_,
                                   GroupFilesSchema& group_files_info_) = 0;

    virtual Status update_files(const GroupFilesSchema& files) = 0;

    virtual Status files_to_merge(const std::string& group_id,
            DatePartionedGroupFilesSchema& files) = 0;

    virtual Status files_to_index(GroupFilesSchema&) = 0;

    static DateT GetDate(const std::time_t& t);
    static DateT GetDate();

}; // MetaData

} // namespace meta
} // namespace engine
} // namespace vecwise
} // namespace zilliz
