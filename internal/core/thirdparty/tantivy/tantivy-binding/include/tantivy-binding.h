#pragma once

#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

enum class TantivyDataType : uint8_t {
  Text,
  Keyword,
  I64,
  F64,
  Bool,
};

struct RustArray {
  uint32_t *array;
  size_t len;
  size_t cap;
};

struct RustArrayI64 {
  int64_t *array;
  size_t len;
  size_t cap;
};

struct Value {
  enum class Tag {
    None,
    RustArray,
    RustArrayI64,
    U32,
    Ptr,
  };

  struct None_Body {

  };

  struct RustArray_Body {
    RustArray _0;
  };

  struct RustArrayI64_Body {
    RustArrayI64 _0;
  };

  struct U32_Body {
    uint32_t _0;
  };

  struct Ptr_Body {
    void *_0;
  };

  Tag tag;
  union {
    None_Body none;
    RustArray_Body rust_array;
    RustArrayI64_Body rust_array_i64;
    U32_Body u32;
    Ptr_Body ptr;
  };
};

struct RustResult {
  bool success;
  Value value;
  const char *error;
};

using SetBitsetFn = void(*)(void*, const uint32_t*, uintptr_t);

struct TantivyToken {
  const char *token;
  int64_t start_offset;
  int64_t end_offset;
  int64_t position;
  int64_t position_length;
};

extern "C" {

void free_rust_array(RustArray array);

void free_rust_array_i64(RustArrayI64 array);

void free_rust_result(RustResult result);

void free_rust_error(const char *error);

RustResult test_enum_with_array();

RustResult test_enum_with_ptr();

void free_test_ptr(void *ptr);

void print_vector_of_strings(const char *const *ptr, uintptr_t len);

void *create_hashmap();

void hashmap_set_value(void *map, const char *key, const char *value);

void free_hashmap(void *map);

RustResult tantivy_create_json_key_stats_writer(const char *field_name,
                                                const char *path,
                                                uint32_t tantivy_index_version,
                                                uintptr_t num_threads,
                                                uintptr_t overall_memory_budget_in_bytes,
                                                bool in_ram);

RustResult tantivy_load_index(const char *path, SetBitsetFn set_bitset);

void tantivy_free_index_reader(void *ptr);

RustResult tantivy_reload_index(void *ptr);

RustResult tantivy_index_count(void *ptr);

RustResult tantivy_term_query_i64(void *ptr, int64_t term, void *bitset);

RustResult tantivy_lower_bound_range_query_i64(void *ptr,
                                               int64_t lower_bound,
                                               bool inclusive,
                                               void *bitset);

RustResult tantivy_lower_bound_range_query_bool(void *ptr,
                                                bool lower_bound,
                                                bool inclusive,
                                                void *bitset);

RustResult tantivy_upper_bound_range_query_i64(void *ptr,
                                               int64_t upper_bound,
                                               bool inclusive,
                                               void *bitset);

RustResult tantivy_upper_bound_range_query_bool(void *ptr,
                                                bool upper_bound,
                                                bool inclusive,
                                                void *bitset);

RustResult tantivy_range_query_i64(void *ptr,
                                   int64_t lower_bound,
                                   int64_t upper_bound,
                                   bool lb_inclusive,
                                   bool ub_inclusive,
                                   void *bitset);

RustResult tantivy_range_query_bool(void *ptr,
                                    bool lower_bound,
                                    bool upper_bound,
                                    bool lb_inclusive,
                                    bool ub_inclusive,
                                    void *bitset);

RustResult tantivy_term_query_f64(void *ptr, double term, void *bitset);

RustResult tantivy_lower_bound_range_query_f64(void *ptr,
                                               double lower_bound,
                                               bool inclusive,
                                               void *bitset);

RustResult tantivy_upper_bound_range_query_f64(void *ptr,
                                               double upper_bound,
                                               bool inclusive,
                                               void *bitset);

RustResult tantivy_range_query_f64(void *ptr,
                                   double lower_bound,
                                   double upper_bound,
                                   bool lb_inclusive,
                                   bool ub_inclusive,
                                   void *bitset);

RustResult tantivy_term_query_bool(void *ptr, bool term, void *bitset);

RustResult tantivy_term_query_keyword(void *ptr, const char *term, void *bitset);

RustResult tantivy_term_query_keyword_i64(void *ptr, const char *term);

RustResult tantivy_lower_bound_range_query_keyword(void *ptr,
                                                   const char *lower_bound,
                                                   bool inclusive,
                                                   void *bitset);

RustResult tantivy_upper_bound_range_query_keyword(void *ptr,
                                                   const char *upper_bound,
                                                   bool inclusive,
                                                   void *bitset);

RustResult tantivy_range_query_keyword(void *ptr,
                                       const char *lower_bound,
                                       const char *upper_bound,
                                       bool lb_inclusive,
                                       bool ub_inclusive,
                                       void *bitset);

RustResult tantivy_prefix_query_keyword(void *ptr, const char *prefix, void *bitset);

RustResult tantivy_regex_query(void *ptr, const char *pattern, void *bitset);

RustResult tantivy_match_query(void *ptr, const char *query, void *bitset);

RustResult tantivy_phrase_match_query(void *ptr, const char *query, uint32_t slop, void *bitset);

RustResult tantivy_register_tokenizer(void *ptr,
                                      const char *tokenizer_name,
                                      const char *analyzer_params);

RustResult tantivy_create_index(const char *field_name,
                                TantivyDataType data_type,
                                const char *path,
                                uint32_t tantivy_index_version,
                                uintptr_t num_threads,
                                uintptr_t overall_memory_budget_in_bytes);

RustResult tantivy_create_index_with_single_segment(const char *field_name,
                                                    TantivyDataType data_type,
                                                    const char *path);

void tantivy_free_index_writer(void *ptr);

RustResult tantivy_finish_index(void *ptr);

RustResult tantivy_commit_index(void *ptr);

RustResult tantivy_create_reader_from_writer(void *ptr, SetBitsetFn set_bitset);

RustResult tantivy_index_add_int8s(void *ptr,
                                   const int8_t *array,
                                   uintptr_t len,
                                   int64_t offset_begin);

RustResult tantivy_index_add_int8s_by_single_segment_writer(void *ptr,
                                                            const int8_t *array,
                                                            uintptr_t len);

RustResult tantivy_index_add_int16s(void *ptr,
                                    const int16_t *array,
                                    uintptr_t len,
                                    int64_t offset_begin);

RustResult tantivy_index_add_int16s_by_single_segment_writer(void *ptr,
                                                             const int16_t *array,
                                                             uintptr_t len);

RustResult tantivy_index_add_int32s(void *ptr,
                                    const int32_t *array,
                                    uintptr_t len,
                                    int64_t offset_begin);

RustResult tantivy_index_add_int32s_by_single_segment_writer(void *ptr,
                                                             const int32_t *array,
                                                             uintptr_t len);

RustResult tantivy_index_add_int64s(void *ptr,
                                    const int64_t *array,
                                    uintptr_t len,
                                    int64_t offset_begin);

RustResult tantivy_index_add_int64s_by_single_segment_writer(void *ptr,
                                                             const int64_t *array,
                                                             uintptr_t len);

RustResult tantivy_index_add_f32s(void *ptr,
                                  const float *array,
                                  uintptr_t len,
                                  int64_t offset_begin);

RustResult tantivy_index_add_f32s_by_single_segment_writer(void *ptr,
                                                           const float *array,
                                                           uintptr_t len);

RustResult tantivy_index_add_f64s(void *ptr,
                                  const double *array,
                                  uintptr_t len,
                                  int64_t offset_begin);

RustResult tantivy_index_add_f64s_by_single_segment_writer(void *ptr,
                                                           const double *array,
                                                           uintptr_t len);

RustResult tantivy_index_add_bools(void *ptr,
                                   const bool *array,
                                   uintptr_t len,
                                   int64_t offset_begin);

RustResult tantivy_index_add_bools_by_single_segment_writer(void *ptr,
                                                            const bool *array,
                                                            uintptr_t len);

RustResult tantivy_index_add_strings(void *ptr,
                                     const char *const *array,
                                     uintptr_t len,
                                     int64_t offset);

RustResult tantivy_index_add_strings_by_single_segment_writer(void *ptr,
                                                              const char *const *array,
                                                              uintptr_t len);

RustResult tantivy_index_add_json_key_stats_data_by_batch(void *ptr,
                                                          const char *const *keys,
                                                          const int64_t *const *json_offsets,
                                                          const uintptr_t *json_offsets_len,
                                                          uintptr_t len);

RustResult tantivy_index_add_array_int8s(void *ptr,
                                         const int8_t *array,
                                         uintptr_t len,
                                         int64_t offset);

RustResult tantivy_index_add_array_int8s_by_single_segment_writer(void *ptr,
                                                                  const int8_t *array,
                                                                  uintptr_t len);

RustResult tantivy_index_add_array_int16s(void *ptr,
                                          const int16_t *array,
                                          uintptr_t len,
                                          int64_t offset);

RustResult tantivy_index_add_array_int16s_by_single_segment_writer(void *ptr,
                                                                   const int16_t *array,
                                                                   uintptr_t len);

RustResult tantivy_index_add_array_int32s(void *ptr,
                                          const int32_t *array,
                                          uintptr_t len,
                                          int64_t offset);

RustResult tantivy_index_add_array_int32s_by_single_segment_writer(void *ptr,
                                                                   const int32_t *array,
                                                                   uintptr_t len);

RustResult tantivy_index_add_array_int64s(void *ptr,
                                          const int64_t *array,
                                          uintptr_t len,
                                          int64_t offset);

RustResult tantivy_index_add_array_int64s_by_single_segment_writer(void *ptr,
                                                                   const int64_t *array,
                                                                   uintptr_t len);

RustResult tantivy_index_add_array_f32s(void *ptr,
                                        const float *array,
                                        uintptr_t len,
                                        int64_t offset);

RustResult tantivy_index_add_array_f32s_by_single_segment_writer(void *ptr,
                                                                 const float *array,
                                                                 uintptr_t len);

RustResult tantivy_index_add_array_f64s(void *ptr,
                                        const double *array,
                                        uintptr_t len,
                                        int64_t offset);

RustResult tantivy_index_add_array_f64s_by_single_segment_writer(void *ptr,
                                                                 const double *array,
                                                                 uintptr_t len);

RustResult tantivy_index_add_array_bools(void *ptr,
                                         const bool *array,
                                         uintptr_t len,
                                         int64_t offset);

RustResult tantivy_index_add_array_bools_by_single_segment_writer(void *ptr,
                                                                  const bool *array,
                                                                  uintptr_t len);

RustResult tantivy_index_add_array_keywords(void *ptr,
                                            const char *const *array,
                                            uintptr_t len,
                                            int64_t offset);

RustResult tantivy_index_add_array_keywords_by_single_segment_writer(void *ptr,
                                                                     const char *const *array,
                                                                     uintptr_t len);

RustResult tantivy_create_text_writer(const char *field_name,
                                      const char *path,
                                      uint32_t tantivy_index_version,
                                      const char *tokenizer_name,
                                      const char *analyzer_params,
                                      uintptr_t num_threads,
                                      uintptr_t overall_memory_budget_in_bytes,
                                      bool in_ram);

void free_rust_string(const char *ptr);

void *tantivy_create_token_stream(void *tokenizer, const char *text);

void tantivy_free_token_stream(void *token_stream);

bool tantivy_token_stream_advance(void *token_stream);

const char *tantivy_token_stream_get_token(void *token_stream);

TantivyToken tantivy_token_stream_get_detailed_token(void *token_stream);

RustResult tantivy_create_analyzer(const char *analyzer_params);

void *tantivy_clone_analyzer(void *ptr);

void tantivy_free_analyzer(void *tokenizer);

bool tantivy_index_exist(const char *path);

} // extern "C"
