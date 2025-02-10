#pragma once

#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

enum class TantivyDataType : uint8_t {
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

struct Value {
  enum class Tag {
    None,
    RustArray,
    U32,
    Ptr,
  };

  struct None_Body {

  };

  struct RustArray_Body {
    RustArray _0;
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
    U32_Body u32;
    Ptr_Body ptr;
  };
};

struct RustResult {
  bool success;
  Value value;
  const char *error;
};

extern "C" {

void free_rust_array(RustArray array);

void free_rust_result(RustResult result);

void free_rust_error(const char *error);

RustResult test_enum_with_array();

RustResult test_enum_with_ptr();

void free_test_ptr(void *ptr);

void print_vector_of_strings(const char *const *ptr, uintptr_t len);

RustResult tantivy_load_index(const char *path);

void tantivy_free_index_reader(void *ptr);

RustResult tantivy_index_count(void *ptr);

RustResult tantivy_term_query_i64(void *ptr, int64_t term);

RustResult tantivy_lower_bound_range_query_i64(void *ptr, int64_t lower_bound, bool inclusive);

RustResult tantivy_upper_bound_range_query_i64(void *ptr, int64_t upper_bound, bool inclusive);

RustResult tantivy_range_query_i64(void *ptr,
                                   int64_t lower_bound,
                                   int64_t upper_bound,
                                   bool lb_inclusive,
                                   bool ub_inclusive);

RustResult tantivy_term_query_f64(void *ptr, double term);

RustResult tantivy_lower_bound_range_query_f64(void *ptr, double lower_bound, bool inclusive);

RustResult tantivy_upper_bound_range_query_f64(void *ptr, double upper_bound, bool inclusive);

RustResult tantivy_range_query_f64(void *ptr,
                                   double lower_bound,
                                   double upper_bound,
                                   bool lb_inclusive,
                                   bool ub_inclusive);

RustResult tantivy_term_query_bool(void *ptr, bool term);

RustResult tantivy_term_query_keyword(void *ptr, const char *term);

RustResult tantivy_lower_bound_range_query_keyword(void *ptr,
                                                   const char *lower_bound,
                                                   bool inclusive);

RustResult tantivy_upper_bound_range_query_keyword(void *ptr,
                                                   const char *upper_bound,
                                                   bool inclusive);

RustResult tantivy_range_query_keyword(void *ptr,
                                       const char *lower_bound,
                                       const char *upper_bound,
                                       bool lb_inclusive,
                                       bool ub_inclusive);

RustResult tantivy_prefix_query_keyword(void *ptr, const char *prefix);

RustResult tantivy_regex_query(void *ptr, const char *pattern);

RustResult tantivy_create_index(const char *field_name,
                                TantivyDataType data_type,
                                const char *path);

void tantivy_free_index_writer(void *ptr);

RustResult tantivy_finish_index(void *ptr);

RustResult tantivy_index_add_int8s(void *ptr, const int8_t *array, uintptr_t len);

RustResult tantivy_index_add_int16s(void *ptr, const int16_t *array, uintptr_t len);

RustResult tantivy_index_add_int32s(void *ptr, const int32_t *array, uintptr_t len);

RustResult tantivy_index_add_int64s(void *ptr, const int64_t *array, uintptr_t len);

RustResult tantivy_index_add_f32s(void *ptr, const float *array, uintptr_t len);

RustResult tantivy_index_add_f64s(void *ptr, const double *array, uintptr_t len);

RustResult tantivy_index_add_bools(void *ptr, const bool *array, uintptr_t len);

RustResult tantivy_index_add_keyword(void *ptr, const char *s);

RustResult tantivy_index_add_multi_int8s(void *ptr, const int8_t *array, uintptr_t len);

RustResult tantivy_index_add_multi_int16s(void *ptr, const int16_t *array, uintptr_t len);

RustResult tantivy_index_add_multi_int32s(void *ptr, const int32_t *array, uintptr_t len);

RustResult tantivy_index_add_multi_int64s(void *ptr, const int64_t *array, uintptr_t len);

RustResult tantivy_index_add_multi_f32s(void *ptr, const float *array, uintptr_t len);

RustResult tantivy_index_add_multi_f64s(void *ptr, const double *array, uintptr_t len);

RustResult tantivy_index_add_multi_bools(void *ptr, const bool *array, uintptr_t len);

RustResult tantivy_index_add_multi_keywords(void *ptr, const char *const *array, uintptr_t len);

void free_rust_string(const char *ptr);

bool tantivy_index_exist(const char *path);

} // extern "C"
