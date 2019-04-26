#pragma once

#include <string>   //  std::string
#include <sqlite3.h>

#include "error_code.h"
#include "row_extractor.h"
#include "journal_mode.h"

namespace sqlite_orm {
    
    template<class S>
    struct pragma_t {
        using storage_type = S;
        
        pragma_t(storage_type &storage_): storage(storage_) {}
        
        sqlite_orm::journal_mode journal_mode() {
            return this->get_pragma<sqlite_orm::journal_mode>("journal_mode");
        }
        
        void journal_mode(sqlite_orm::journal_mode value) {
            this->_journal_mode = -1;
            this->set_pragma("journal_mode", value);
            this->_journal_mode = static_cast<decltype(this->_journal_mode)>(value);
        }
        
        int synchronous() {
            return this->get_pragma<int>("synchronous");
        }
        
        void synchronous(int value) {
            this->_synchronous = -1;
            this->set_pragma("synchronous", value);
            this->_synchronous = value;
        }
        
        int user_version() {
            return this->get_pragma<int>("user_version");
        }
        
        void user_version(int value) {
            this->set_pragma("user_version", value);
        }
        
        int auto_vacuum() {
            return this->get_pragma<int>("auto_vacuum");
        }
        
        void auto_vacuum(int value) {
            this->set_pragma("auto_vacuum", value);
        }
        
        friend storage_type;
        
    protected:
        storage_type &storage;
        int _synchronous = -1;
        char _journal_mode = -1; //  if != -1 stores static_cast<sqlite_orm::journal_mode>(journal_mode)
        
        template<class T>
        T get_pragma(const std::string &name) {
            auto connection = this->storage.get_or_create_connection();
            auto query = "PRAGMA " + name;
            T res;
            auto rc = sqlite3_exec(connection->get_db(),
                                   query.c_str(),
                                   [](void *data, int argc, char **argv, char **) -> int {
                                       auto &res = *(T*)data;
                                       if(argc){
                                           res = row_extractor<T>().extract(argv[0]);
                                       }
                                       return 0;
                                   }, &res, nullptr);
            if(rc == SQLITE_OK){
                return res;
            }else{
                throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
            }
        }
        
        /**
         *  Yevgeniy Zakharov: I wanted to refactored this function with statements and value bindings
         *  but it turns out that bindings in pragma statements are not supported.
         */
        template<class T>
        void set_pragma(const std::string &name, const T &value, sqlite3 *db = nullptr) {
            std::shared_ptr<internal::database_connection> connection;
            if(!db){
                connection = this->storage.get_or_create_connection();
                db = connection->get_db();
            }
            std::stringstream ss;
            ss << "PRAGMA " << name << " = " << this->storage.string_from_expression(value);
            auto query = ss.str();
            auto rc = sqlite3_exec(db, query.c_str(), nullptr, nullptr, nullptr);
            if(rc != SQLITE_OK) {
                throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
            }
        }
        
        void set_pragma(const std::string &name, const sqlite_orm::journal_mode &value, sqlite3 *db = nullptr) {
            std::shared_ptr<internal::database_connection> connection;
            if(!db){
                connection = this->storage.get_or_create_connection();
                db = connection->get_db();
            }
            std::stringstream ss;
            ss << "PRAGMA " << name << " = " << internal::to_string(value);
            auto query = ss.str();
            auto rc = sqlite3_exec(db, query.c_str(), nullptr, nullptr, nullptr);
            if(rc != SQLITE_OK) {
                throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
            }
        }
    };
}
