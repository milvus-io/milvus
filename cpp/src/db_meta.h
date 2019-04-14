#ifndef VECENGINE_DB_META_H_
#define VECENGINE_DB_META_H_

namespace zilliz {
namespace vecwise {
namespace engine {

struct GroupSchema {
    size_t id;
    std::string group_id;
    size_t files_cnt = 0;
    uint16_t dimension;
    std::string location = "";
}; // GroupSchema


struct GroupFileSchema {
    typedef enum {
        RAW,
        INDEX
    } FILE_TYPE;

    size_t id;
    std::string group_id;
    std::string file_id;
    int files_type = RAW;
    size_t rows;
    std::string location = "";
}; // GroupFileSchema

typedef std::vector<GroupFileSchema> GroupFilesSchema;


class Meta {
public:
    virtual Status add_group(const std::string& group_id_, GroupSchema& group_info) = 0;
    virtual Status get_group(const std::string& group_id_, GroupSchema& group_info) = 0;
    virtual Status has_group(const std::string& group_id_, bool& has_or_not) = 0;

    virtual Status add_group_file(const std::string& group_id,
                                  GroupFileSchema& group_file_info) = 0;
    virtual Status has_group_file(const std::string& group_id,
                                  const std::string& file_id,
                                  bool& has_or_not) = 0;
    virtual Status get_group_file(const std::string& group_id,
                                  const std::string& file_id,
                                  GroupFileSchema& group_file_info) = 0;
    virtual Status mark_group_file_as_index(const std::string& group_id,
                                            const std::string& file_id) = 0;

    virtual Status get_group_files(const std::string& group_id,
                                   GroupFilesSchema& group_files_info) = 0;

}; // MetaData

} // namespace engine
} // namespace vecwise
} // namespace zilliz

#endif // VECENGINE_DB_META_H_
