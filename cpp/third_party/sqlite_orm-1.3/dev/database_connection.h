#pragma once

#include <string>   //  std::string
#include <sqlite3.h>
#include <system_error> //  std::error_code, std::system_error

#include "error_code.h"

namespace sqlite_orm {
    
    namespace internal {
        
        struct database_connection {
            
            database_connection(const std::string &filename) {
                auto rc = sqlite3_open(filename.c_str(), &this->db);
                if(rc != SQLITE_OK){
                    throw std::system_error(std::error_code(sqlite3_errcode(this->db), get_sqlite_error_category()));
                }
            }
            
            ~database_connection() {
                sqlite3_close(this->db);
            }
            
            sqlite3* get_db() {
                return this->db;
            }
            
        protected:
            sqlite3 *db = nullptr;
        };
    }
}
