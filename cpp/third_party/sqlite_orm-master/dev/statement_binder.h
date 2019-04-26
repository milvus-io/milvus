#pragma once

#include <sqlite3.h>
#include <type_traits>  //  std::enable_if_t, std::is_arithmetic, std::is_same
#include <string>   //  std::string, std::wstring
#ifndef SQLITE_ORM_OMITS_CODECVT
#include <codecvt>  //  std::wstring_convert, std::codecvt_utf8_utf16
#endif  //  SQLITE_ORM_OMITS_CODECVT
#include <vector>   //  std::vector
#include <cstddef>  //  std::nullptr_t

#include "is_std_ptr.h"

namespace sqlite_orm {
    
    /**
     *  Helper class used for binding fields to sqlite3 statements.
     */
    template<class V, typename Enable = void>
    struct statement_binder {
        int bind(sqlite3_stmt *stmt, int index, const V &value);
    };
    
    /**
     *  Specialization for arithmetic types.
     */
    template<class V>
    struct statement_binder<
    V,
    std::enable_if_t<std::is_arithmetic<V>::value>
    >
    {
        int bind(sqlite3_stmt *stmt, int index, const V &value) {
            return bind(stmt, index, value, tag());
        }
        
    private:
        using tag = arithmetic_tag_t<V>;
        
        int bind(sqlite3_stmt *stmt, int index, const V &value, const int_or_smaller_tag&) {
            return sqlite3_bind_int(stmt, index, static_cast<int>(value));
        }
        
        int bind(sqlite3_stmt *stmt, int index, const V &value, const bigint_tag&) {
            return sqlite3_bind_int64(stmt, index, static_cast<sqlite3_int64>(value));
        }
        
        int bind(sqlite3_stmt *stmt, int index, const V &value, const real_tag&) {
            return sqlite3_bind_double(stmt, index, static_cast<double>(value));
        }
    };
    
    
    /**
     *  Specialization for std::string and C-string.
     */
    template<class V>
    struct statement_binder<
    V,
    std::enable_if_t<
    std::is_same<V, std::string>::value
    ||
    std::is_same<V, const char*>::value
    >
    >
    {
        int bind(sqlite3_stmt *stmt, int index, const V &value) {
            return sqlite3_bind_text(stmt, index, string_data(value), -1, SQLITE_TRANSIENT);
        }
        
    private:
        const char* string_data(const std::string& s) const {
            return s.c_str();
        }
        
        const char* string_data(const char* s) const{
            return s;
        }
    };
#ifndef SQLITE_ORM_OMITS_CODECVT
    /**
     *  Specialization for std::wstring and C-wstring.
     */
    template<class V>
    struct statement_binder<
    V,
    std::enable_if_t<
    std::is_same<V, std::wstring>::value
    ||
    std::is_same<V, const wchar_t*>::value
    >
    >
    {
        int bind(sqlite3_stmt *stmt, int index, const V &value) {
            std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> converter;
            std::string utf8Str = converter.to_bytes(value);
            return statement_binder<decltype(utf8Str)>().bind(stmt, index, utf8Str);
        }
    };
#endif  //  SQLITE_ORM_OMITS_CODECVT
    /**
     *  Specialization for std::nullptr_t.
     */
    template<class V>
    struct statement_binder<
    V,
    std::enable_if_t<std::is_same<V, std::nullptr_t>::value>
    >
    {
        int bind(sqlite3_stmt *stmt, int index, const V &) {
            return sqlite3_bind_null(stmt, index);
        }
    };
    
    template<class V>
    struct statement_binder<
    V,
    std::enable_if_t<is_std_ptr<V>::value>
    >
    {
        using value_type = typename V::element_type;
        
        int bind(sqlite3_stmt *stmt, int index, const V &value) {
            if(value){
                return statement_binder<value_type>().bind(stmt, index, *value);
            }else{
                return statement_binder<std::nullptr_t>().bind(stmt, index, nullptr);
            }
        }
    };
    
    /**
     *  Specialization for optional type (std::vector<char>).
     */
    template<class V>
    struct statement_binder<
    V,
    std::enable_if_t<std::is_same<V, std::vector<char>>::value>
    >
    {
        int bind(sqlite3_stmt *stmt, int index, const V &value) {
            if (value.size()) {
                return sqlite3_bind_blob(stmt, index, (const void *)&value.front(), int(value.size()), SQLITE_TRANSIENT);
            }else{
                return sqlite3_bind_blob(stmt, index, "", 0, SQLITE_TRANSIENT);
            }
        }
    };
}
