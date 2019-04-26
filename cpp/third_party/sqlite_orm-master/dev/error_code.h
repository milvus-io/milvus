#pragma once

#include <system_error>  // std::error_code, std::system_error
#include <string>   //  std::string
#include <sqlite3.h>
#include <stdexcept>

namespace sqlite_orm {
    
    enum class orm_error_code {
        not_found = 1,
        type_is_not_mapped_to_storage,
        trying_to_dereference_null_iterator,
        too_many_tables_specified,
        incorrect_set_fields_specified,
        column_not_found,
        table_has_no_primary_key_column,
        cannot_start_a_transaction_within_a_transaction,
        no_active_transaction,
        incorrect_journal_mode_string,
    };
    
}

namespace sqlite_orm {
    
    class orm_error_category : public std::error_category {
    public:
        
        const char *name() const noexcept override final {
            return "ORM error";
        }
        
        std::string message(int c) const override final {
            switch (static_cast<orm_error_code>(c)) {
                case orm_error_code::not_found:
                    return "Not found";
                case orm_error_code::type_is_not_mapped_to_storage:
                    return "Type is not mapped to storage";
                case orm_error_code::trying_to_dereference_null_iterator:
                    return "Trying to dereference null iterator";
                case orm_error_code::too_many_tables_specified:
                    return "Too many tables specified";
                case orm_error_code::incorrect_set_fields_specified:
                    return "Incorrect set fields specified";
                case orm_error_code::column_not_found:
                    return "Column not found";
                case orm_error_code::table_has_no_primary_key_column:
                    return "Table has no primary key column";
                case orm_error_code::cannot_start_a_transaction_within_a_transaction:
                    return "Cannot start a transaction within a transaction";
                case orm_error_code::no_active_transaction:
                    return "No active transaction";
                default:
                    return "unknown error";
            }
        }
    };
    
    class sqlite_error_category : public std::error_category {
    public:
        
        const char *name() const noexcept override final {
            return "SQLite error";
        }
        
        std::string message(int c) const override final {
            return sqlite3_errstr(c);
        }
    };
    
    inline const orm_error_category& get_orm_error_category() {
        static orm_error_category res;
        return res;
    }
    
    inline const sqlite_error_category& get_sqlite_error_category() {
        static sqlite_error_category res;
        return res;
    }
}

namespace std
{
    template <>
    struct is_error_code_enum<sqlite_orm::orm_error_code> : std::true_type{};
    
    inline std::error_code make_error_code(sqlite_orm::orm_error_code errorCode) {
        return std::error_code(static_cast<int>(errorCode), sqlite_orm::get_orm_error_category());
    }
}
