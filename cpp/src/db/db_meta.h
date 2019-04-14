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
    virtual Status add_group(const GroupOptions& options_,
            const std::string& group_id_,
            GroupSchema& group_info_) = 0;
    virtual Status get_group(const std::string& group_id_, GroupSchema& group_info_) = 0;
    virtual Status has_group(const std::string& group_id_, bool& has_or_not_) = 0;

    virtual Status add_group_file(const std::string& group_id_,
                                  GroupFileSchema& group_file_info_) = 0;
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

}; // MetaData

} // namespace engine
} // namespace vecwise
} // namespace zilliz

#endif // VECENGINE_DB_META_H_
