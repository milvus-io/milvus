#pragma once

#include <string>   //  std::string
#include <sqlite3.h>    
#include <cstddef>  //  std::nullptr_t
#include <system_error> //  std::system_error, std::error_code
#include <sstream>  //  std::stringstream
#include <cstdlib>  //  std::atoi
#include <type_traits>  //  std::forward, std::enable_if, std::is_same, std::remove_reference
#include <utility>  //  std::pair, std::make_pair
#include <vector>   //  std::vector
#include <algorithm>    //  std::find_if

#include "error_code.h"
#include "statement_finalizer.h"
#include "row_extractor.h"
#include "constraints.h"
#include "select_constraints.h"
#include "field_printer.h"
#include "table_info.h"
#include "sync_schema_result.h"
#include "sqlite_type.h"

namespace sqlite_orm {
    
    namespace internal {
        
        /**
         *  This is a generic implementation. Used as a tail in storage_impl inheritance chain
         */
        template<class ...Ts>
        struct storage_impl {
            
            template<class L>
            void for_each(L) {}
            
            int foreign_keys_count() {
                return 0;
            }
            
            template<class O>
            std::string dump(const O &, sqlite3 *, std::nullptr_t) {
                throw std::system_error(std::make_error_code(orm_error_code::type_is_not_mapped_to_storage));
            }
            
            bool table_exists(const std::string &tableName, sqlite3 *db) {
                auto res = false;
                std::stringstream ss;
                ss << "SELECT COUNT(*) FROM sqlite_master WHERE type = '" << "table" << "' AND name = '" << tableName << "'";
                auto query = ss.str();
                auto rc = sqlite3_exec(db,
                                       query.c_str(),
                                       [](void *data, int argc, char **argv,char ** /*azColName*/) -> int {
                                           auto &res = *(bool*)data;
                                           if(argc){
                                               res = !!std::atoi(argv[0]);
                                           }
                                           return 0;
                                       }, &res, nullptr);
                if(rc != SQLITE_OK) {
                    throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                }
                return res;
            }
            
            void begin_transaction(sqlite3 *db) {
                std::stringstream ss;
                ss << "BEGIN TRANSACTION";
                auto query = ss.str();
                sqlite3_stmt *stmt;
                if (sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
                    statement_finalizer finalizer{stmt};
                    if (sqlite3_step(stmt) == SQLITE_DONE) {
                        //  done..
                    }else{
                        throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                    }
                }else {
                    throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                }
            }
            
            void commit(sqlite3 *db) {
                std::stringstream ss;
                ss << "COMMIT";
                auto query = ss.str();
                sqlite3_stmt *stmt;
                if (sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
                    statement_finalizer finalizer{stmt};
                    if (sqlite3_step(stmt) == SQLITE_DONE) {
                        //  done..
                    }else{
                        throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                    }
                }else {
                    throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                }
            }
            
            void rollback(sqlite3 *db) {
                std::stringstream ss;
                ss << "ROLLBACK";
                auto query = ss.str();
                sqlite3_stmt *stmt;
                if (sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
                    statement_finalizer finalizer{stmt};
                    if (sqlite3_step(stmt) == SQLITE_DONE) {
                        //  done..
                    }else{
                        throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                    }
                }else {
                    throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                }
            }
            
            void rename_table(sqlite3 *db, const std::string &oldName, const std::string &newName) {
                std::stringstream ss;
                ss << "ALTER TABLE " << oldName << " RENAME TO " << newName;
                auto query = ss.str();
                sqlite3_stmt *stmt;
                if (sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
                    statement_finalizer finalizer{stmt};
                    if (sqlite3_step(stmt) == SQLITE_DONE) {
                        //  done..
                    }else{
                        throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                    }
                }else {
                    throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                }
            }
            
            std::string current_timestamp(sqlite3 *db) {
                std::string res;
                std::stringstream ss;
                ss << "SELECT CURRENT_TIMESTAMP";
                auto query = ss.str();
                auto rc = sqlite3_exec(db,
                                       query.c_str(),
                                       [](void *data, int argc, char **argv, char **) -> int {
                                           auto &res = *(std::string*)data;
                                           if(argc){
                                               if(argv[0]){
                                                   res = row_extractor<std::string>().extract(argv[0]);
                                               }
                                           }
                                           return 0;
                                       }, &res, nullptr);
                if(rc != SQLITE_OK) {
                    throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                }
                return res;
            }
        };
        
        template<class H, class ...Ts>
        struct storage_impl<H, Ts...> : public storage_impl<Ts...> {
            using table_type = H;
            
            storage_impl(H h, Ts ...ts) : super(std::forward<Ts>(ts)...), table(std::move(h)) {}
            
            table_type table;
            
            template<class L>
            void for_each(L l) {
                this->super::for_each(l);
                l(this);
            }
            
#if SQLITE_VERSION_NUMBER >= 3006019
            
            /**
             *  Returns foreign keys count in table definition
             */
            int foreign_keys_count() {
                auto res = 0;
                this->table.for_each_column_with_constraints([&res](auto c){
                    if(internal::is_foreign_key<decltype(c)>::value) {
                        ++res;
                    }
                });
                return res;
            }
            
#endif
            
            /**
             *  Is used to get column name by member pointer to a base class.
             *  Main difference between `column_name` and `column_name_simple` is that
             *  `column_name` has SFINAE check for type equality but `column_name_simple` has not.
             */
            template<class O, class F>
            std::string column_name_simple(F O::*m) {
                return this->table.find_column_name(m);
            }
            
            /**
             *  Same thing as above for getter.
             */
            template<class T, typename std::enable_if<is_getter<T>::value>::type>
            std::string column_name_simple(T g) {
                return this->table.find_column_name(g);
            }
            
            /**
             *  Same thing as above for setter.
             */
            template<class T, typename std::enable_if<is_setter<T>::value>::type>
            std::string column_name_simple(T s) {
                return this->table.find_column_name(s);
            }
            
            /**
             *  Cute function used to find column name by its type and member pointer. Uses SFINAE to
             *  skip inequal type O.
             */
            template<class O, class F, class HH = typename H::object_type>
            std::string column_name(F O::*m, typename std::enable_if<std::is_same<O, HH>::value>::type * = nullptr) {
                return this->table.find_column_name(m);
            }
            
            /**
             *  Opposite version of function defined above. Just calls same function in superclass.
             */
            template<class O, class F, class HH = typename H::object_type>
            std::string column_name(F O::*m, typename std::enable_if<!std::is_same<O, HH>::value>::type * = nullptr) {
                return this->super::column_name(m);
            }
            
            /**
             *  Cute function used to find column name by its type and getter pointer. Uses SFINAE to
             *  skip inequal type O.
             */
            template<class O, class F, class HH = typename H::object_type>
            std::string column_name(const F& (O::*g)() const, typename std::enable_if<std::is_same<O, HH>::value>::type * = nullptr) {
                return this->table.find_column_name(g);
            }
            
            /**
             *  Opposite version of function defined above. Just calls same function in superclass.
             */
            template<class O, class F, class HH = typename H::object_type>
            std::string column_name(const F& (O::*g)() const, typename std::enable_if<!std::is_same<O, HH>::value>::type * = nullptr) {
                return this->super::column_name(g);
            }
            
            /**
             *  Cute function used to find column name by its type and setter pointer. Uses SFINAE to
             *  skip inequal type O.
             */
            template<class O, class F, class HH = typename H::object_type>
            std::string column_name(void (O::*s)(F), typename std::enable_if<std::is_same<O, HH>::value>::type * = nullptr) {
                return this->table.find_column_name(s);
            }
            
            /**
             *  Opposite version of function defined above. Just calls same function in superclass.
             */
            template<class O, class F, class HH = typename H::object_type>
            std::string column_name(void (O::*s)(F), typename std::enable_if<!std::is_same<O, HH>::value>::type * = nullptr) {
                return this->super::column_name(s);
            }
            
            template<class T, class F, class HH = typename H::object_type>
            std::string column_name(const column_pointer<T, F> &c, typename std::enable_if<std::is_same<T, HH>::value>::type * = nullptr) {
                return this->column_name_simple(c.field);
            }
            
            template<class T, class F, class HH = typename H::object_type>
            std::string column_name(const column_pointer<T, F> &c, typename std::enable_if<!std::is_same<T, HH>::value>::type * = nullptr) {
                return this->super::column_name(c);
            }
            
            template<class O, class HH = typename H::object_type>
            auto& get_impl(typename std::enable_if<std::is_same<O, HH>::value>::type * = nullptr) {
                return *this;
            }
            
            template<class O, class HH = typename H::object_type>
            auto& get_impl(typename std::enable_if<!std::is_same<O, HH>::value>::type * = nullptr) {
                return this->super::template get_impl<O>();
            }
            
            template<class O, class HH = typename H::object_type>
            std::string find_table_name(typename std::enable_if<std::is_same<O, HH>::value>::type * = nullptr) {
                return this->table.name;
            }
            
            template<class O, class HH = typename H::object_type>
            std::string find_table_name(typename std::enable_if<!std::is_same<O, HH>::value>::type * = nullptr) {
                return this->super::template find_table_name<O>();
            }
            
            template<class O, class HH = typename H::object_type>
            std::string dump(const O &o, typename std::enable_if<!std::is_same<O, HH>::value>::type * = nullptr) {
                return this->super::dump(o, nullptr);
            }
            
            template<class O, class HH = typename H::object_type>
            std::string dump(const O &o, typename std::enable_if<std::is_same<O, HH>::value>::type * = nullptr) {
                std::stringstream ss;
                ss << "{ ";
                std::vector<std::pair<std::string, std::string>> pairs;
                this->table.for_each_column([&pairs, &o] (auto &c) {
                    using field_type = typename std::remove_reference<decltype(c)>::type::field_type;
                    const field_type *value = nullptr;
                    if(c.member_pointer){
                        value = &(o.*c.member_pointer);
                    }else{
                        value = &((o).*(c.getter))();
                    }
                    pairs.push_back(std::make_pair(c.name, field_printer<field_type>()(*value)));
                });
                for(size_t i = 0; i < pairs.size(); ++i) {
                    auto &p = pairs[i];
                    ss << p.first << " : '" << p.second << "'";
                    if(i < pairs.size() - 1) {
                        ss << ", ";
                    }else{
                        ss << " }";
                    }
                }
                return ss.str();
            }
            
            std::vector<table_info> get_table_info(const std::string &tableName, sqlite3 *db) {
                std::vector<table_info> res;
                auto query = "PRAGMA table_info('" + tableName + "')";
                auto rc = sqlite3_exec(db,
                                       query.c_str(),
                                       [](void *data, int argc, char **argv,char **) -> int {
                                           auto &res = *(std::vector<table_info>*)data;
                                           if(argc){
                                               auto index = 0;
                                               auto cid = std::atoi(argv[index++]);
                                               std::string name = argv[index++];
                                               std::string type = argv[index++];
                                               bool notnull = !!std::atoi(argv[index++]);
                                               std::string dflt_value = argv[index] ? argv[index] : "";
                                               index++;
                                               auto pk = std::atoi(argv[index++]);
                                               res.push_back(table_info{cid, name, type, notnull, dflt_value, pk});
                                           }
                                           return 0;
                                       }, &res, nullptr);
                if(rc != SQLITE_OK) {
                    throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                }
                return res;
            }
            
            void add_column(const table_info &ti, sqlite3 *db) {
                std::stringstream ss;
                ss << "ALTER TABLE " << this->table.name << " ADD COLUMN " << ti.name << " ";
                ss << ti.type << " ";
                if(ti.pk){
                    ss << "PRIMARY KEY ";
                }
                if(ti.notnull){
                    ss << "NOT NULL ";
                }
                if(ti.dflt_value.length()) {
                    ss << "DEFAULT " << ti.dflt_value << " ";
                }
                auto query = ss.str();
                sqlite3_stmt *stmt;
                auto prepareResult = sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, nullptr);
                if (prepareResult == SQLITE_OK) {
                    statement_finalizer finalizer{stmt};
                    if (sqlite3_step(stmt) == SQLITE_DONE) {
                        //..
                    }else{
                        throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                    }
                }else{
                    throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                }
            }
            
            /**
             *  Copies current table to another table with a given **name**.
             *  Performs CREATE TABLE %name% AS SELECT %this->table.columns_names()% FROM &this->table.name%;
             */
            void copy_table(sqlite3 *db, const std::string &name) {
                std::stringstream ss;
                std::vector<std::string> columnNames;
                this->table.for_each_column([&columnNames] (auto c) {
                    columnNames.emplace_back(c.name);
                });
                auto columnNamesCount = columnNames.size();
                ss << "INSERT INTO " << name << " (";
                for(size_t i = 0; i < columnNamesCount; ++i) {
                    ss << columnNames[i];
                    if(i < columnNamesCount - 1) {
                        ss << ", ";
                    }else{
                        ss << " ";
                    }
                }
                ss << ") ";
                ss << "SELECT ";
                for(size_t i = 0; i < columnNamesCount; ++i) {
                    ss << columnNames[i];
                    if(i < columnNamesCount - 1) {
                        ss << ", ";
                    }else{
                        ss << " ";
                    }
                }
                ss << " FROM '" << this->table.name << "' ";
                auto query = ss.str();
                sqlite3_stmt *stmt;
                if (sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
                    statement_finalizer finalizer{stmt};
                    if (sqlite3_step(stmt) == SQLITE_DONE) {
                        //..
                    }else{
                        throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                    }
                }else{
                    throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                }
            }
            
            sync_schema_result schema_status(sqlite3 *db, bool preserve) {
                
                auto res = sync_schema_result::already_in_sync;
                
                //  first let's see if table with such name exists..
                auto gottaCreateTable = !this->table_exists(this->table.name, db);
                if(!gottaCreateTable){
                    
                    //  get table info provided in `make_table` call..
                    auto storageTableInfo = this->table.get_table_info();
                    
                    //  now get current table info from db using `PRAGMA table_info` query..
                    auto dbTableInfo = get_table_info(this->table.name, db);
                    
                    //  this vector will contain pointers to columns that gotta be added..
                    std::vector<table_info*> columnsToAdd;
                    
                    if(get_remove_add_columns(columnsToAdd, storageTableInfo, dbTableInfo)) {
                        gottaCreateTable = true;
                    }
                    
                    if(!gottaCreateTable){  //  if all storage columns are equal to actual db columns but there are excess columns at the db..
                        if(dbTableInfo.size() > 0){
                            //extra table columns than storage columns
                            if(!preserve){
                                gottaCreateTable = true;
                            }else{
                                res = decltype(res)::old_columns_removed;
                            }
                        }
                    }
                    if(gottaCreateTable){
                        res = decltype(res)::dropped_and_recreated;
                    }else{
                        if(columnsToAdd.size()){
                            //extra storage columns than table columns
                            for(auto columnPointer : columnsToAdd) {
                                if(columnPointer->notnull && columnPointer->dflt_value.empty()){
                                    gottaCreateTable = true;
                                    break;
                                }
                            }
                            if(!gottaCreateTable){
                                if(res == decltype(res)::old_columns_removed) {
                                    res = decltype(res)::new_columns_added_and_old_columns_removed;
                                }else{
                                    res = decltype(res)::new_columns_added;
                                }
                            }else{
                                res = decltype(res)::dropped_and_recreated;
                            }
                        }else{
                            if(res != decltype(res)::old_columns_removed){
                                res = decltype(res)::already_in_sync;
                            }
                        }
                    }
                }else{
                    res = decltype(res)::new_table_created;
                }
                return res;
            }
            
            static bool get_remove_add_columns(std::vector<table_info*>& columnsToAdd,
                                               std::vector<table_info>& storageTableInfo,
                                               std::vector<table_info>& dbTableInfo)
            {
                bool notEqual = false;
                
                //  iterate through storage columns
                for(size_t storageColumnInfoIndex = 0; storageColumnInfoIndex < storageTableInfo.size(); ++storageColumnInfoIndex) {
                    
                    //  get storage's column info
                    auto &storageColumnInfo = storageTableInfo[storageColumnInfoIndex];
                    auto &columnName = storageColumnInfo.name;
                    
                    //  search for a column in db eith the same name
                    auto dbColumnInfoIt = std::find_if(dbTableInfo.begin(),
                                                       dbTableInfo.end(),
                                                       [&columnName](auto &ti){
                                                           return ti.name == columnName;
                                                       });
                    if(dbColumnInfoIt != dbTableInfo.end()){
                        auto &dbColumnInfo = *dbColumnInfoIt;
                        auto dbColumnInfoType = to_sqlite_type(dbColumnInfo.type);
                        auto storageColumnInfoType = to_sqlite_type(storageColumnInfo.type);
                        if(dbColumnInfoType && storageColumnInfoType) {
                            auto columnsAreEqual = dbColumnInfo.name == storageColumnInfo.name &&
                            *dbColumnInfoType == *storageColumnInfoType &&
                            dbColumnInfo.notnull == storageColumnInfo.notnull &&
                            bool(dbColumnInfo.dflt_value.length()) == bool(storageColumnInfo.dflt_value.length()) &&
                            dbColumnInfo.pk == storageColumnInfo.pk;
                            if(!columnsAreEqual){
                                notEqual = true;
                                break;
                            }
                            dbTableInfo.erase(dbColumnInfoIt);
                            storageTableInfo.erase(storageTableInfo.begin() + storageColumnInfoIndex);
                            --storageColumnInfoIndex;
                        }else{
                            
                            //  undefined type/types
                            notEqual = true;
                            break;
                        }
                    }else{
                        columnsToAdd.push_back(&storageColumnInfo);
                    }
                }
                return notEqual;
            }
            
            
        private:
            using super = storage_impl<Ts...>;
            using self = storage_impl<H, Ts...>;
        };
    }
}
