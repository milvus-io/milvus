#ifndef STORAGE_VECENGINE_ENV_H_
#define STORAGE_VECENGINE_ENV_H_

namespace vecengine {

/* struct Options { */
/*     std::string _db_location; */
/*     size_t _mem_sync_interval; */
/*     size_t _file_merge_trigger_number; */
/*     size_t _index_file_build_trigger_size; */
/* }; // Config */

class Env {
public:
    Env() = default;

private:
    Options _option;
}; // Env

} //namespace vecengine

#endif // STORAGE_VECENGINE_ENV_H_
