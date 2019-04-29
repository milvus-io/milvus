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
const DateT EmptyDate = -1;
typedef std::vector<DateT> DatesT;

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
        TO_INDEX,
        INDEX,
        TO_DELETE,
    } FILE_TYPE;

    size_t id;
    std::string group_id;
    std::string file_id;
    int file_type = NEW;
    size_t rows;
    DateT date = EmptyDate;
    uint16_t dimension;
    std::string location = "";
    long updated_time;
}; // GroupFileSchema

typedef std::vector<GroupFileSchema> GroupFilesSchema;
typedef std::map<DateT, GroupFilesSchema> DatePartionedGroupFilesSchema;


class Meta {
public:
    virtual Status add_group(GroupSchema& group_info) = 0;
    virtual Status get_group(GroupSchema& group_info) = 0;
    virtual Status has_group(const std::string& group_id_, bool& has_or_not_) = 0;

    virtual Status add_group_file(GroupFileSchema& group_file_info) = 0;

    virtual Status has_group_file(const std::string& group_id_,
                                  const std::string& file_id_,
                                  bool& has_or_not_) = 0;
    virtual Status get_group_file(const std::string& group_id_,
                                  const std::string& file_id_,
                                  GroupFileSchema& group_file_info_) = 0;
    virtual Status update_group_file(GroupFileSchema& group_file_) = 0;

    virtual Status get_group_files(const std::string& group_id_,
                                   const int date_delta_,
                                   GroupFilesSchema& group_files_info_) = 0;

    virtual Status update_files(GroupFilesSchema& files) = 0;

    virtual Status files_to_search(const std::string& group_id,
                                   const DatesT& partition,
                                   DatePartionedGroupFilesSchema& files) = 0;

    virtual Status files_to_merge(const std::string& group_id,
            DatePartionedGroupFilesSchema& files) = 0;

    virtual Status files_to_index(GroupFilesSchema&) = 0;

    virtual Status cleanup() = 0;
    virtual Status cleanup_ttl_files(uint16_t) = 0;

    virtual Status drop_all() = 0;

    virtual Status count(const std::string& group_id, long& result) = 0;

    static DateT GetDate(const std::time_t& t);
    static DateT GetDate();

}; // MetaData

} // namespace meta
} // namespace engine
} // namespace vecwise
} // namespace zilliz
