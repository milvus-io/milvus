#pragma once

#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

enum class TantivyDataType : uint8_t {
  Text,
  Keyword,
  U64,
  I64,
  F64,
  Bool,
};

extern "C" {

void *tantivy_create_index(const char *field_name, TantivyDataType data_type, const char *path);

void tantivy_index_add_int64s(void *ptr, const int64_t *array, uintptr_t len);

void tantivy_finish_index(void *ptr);

void tantivy_free_index(void *ptr);

} // extern "C"
