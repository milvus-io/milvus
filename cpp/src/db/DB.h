#ifndef VECENGINE_DB_H_
#define VECENGINE_DB_H_

#include <string>
#include "Options.h"
#include "Meta.h"
#include "Status.h"
#include "Types.h"

namespace zilliz {
namespace vecwise {
namespace engine {

class Env;

class DB {
public:
    static DB* Open(const Options& options);

    virtual Status add_group(meta::GroupSchema& group_info_) = 0;
    virtual Status get_group(meta::GroupSchema& group_info_) = 0;
    virtual Status has_group(const std::string& group_id_, bool& has_or_not_) = 0;
    virtual Status get_group_files(const std::string& group_id_,
                                   const int date_delta_,
                                   meta::GroupFilesSchema& group_files_info_) = 0;

    virtual Status add_vectors(const std::string& group_id_,
            size_t n, const float* vectors, IDNumbers& vector_ids_) = 0;

    virtual Status search(const std::string& group_id, size_t k, size_t nq,
            const float* vectors, QueryResults& results) = 0;

    DB() = default;
    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;

    virtual ~DB();
}; // DB

} // namespace engine
} // namespace vecwise
} // namespace zilliz

#endif // VECENGINE_DB_H_
