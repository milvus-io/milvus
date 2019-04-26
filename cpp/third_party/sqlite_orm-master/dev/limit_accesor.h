#pragma once

#include <sqlite3.h>
#include <map>  //  std::map

namespace sqlite_orm {
    namespace internal {
        
        template<class S>
        struct limit_accesor {
            using storage_type = S;
            
            limit_accesor(storage_type &storage_): storage(storage_) {}
            
            int length() {
                return this->get(SQLITE_LIMIT_LENGTH);
            }
            
            void length(int newValue) {
                this->set(SQLITE_LIMIT_LENGTH, newValue);
            }
            
            int sql_length() {
                return this->get(SQLITE_LIMIT_SQL_LENGTH);
            }
            
            void sql_length(int newValue) {
                this->set(SQLITE_LIMIT_SQL_LENGTH, newValue);
            }
            
            int column() {
                return this->get(SQLITE_LIMIT_COLUMN);
            }
            
            void column(int newValue) {
                this->set(SQLITE_LIMIT_COLUMN, newValue);
            }
            
            int expr_depth() {
                return this->get(SQLITE_LIMIT_EXPR_DEPTH);
            }
            
            void expr_depth(int newValue) {
                this->set(SQLITE_LIMIT_EXPR_DEPTH, newValue);
            }
            
            int compound_select() {
                return this->get(SQLITE_LIMIT_COMPOUND_SELECT);
            }
            
            void compound_select(int newValue) {
                this->set(SQLITE_LIMIT_COMPOUND_SELECT, newValue);
            }
            
            int vdbe_op() {
                return this->get(SQLITE_LIMIT_VDBE_OP);
            }
            
            void vdbe_op(int newValue) {
                this->set(SQLITE_LIMIT_VDBE_OP, newValue);
            }
            
            int function_arg() {
                return this->get(SQLITE_LIMIT_FUNCTION_ARG);
            }
            
            void function_arg(int newValue) {
                this->set(SQLITE_LIMIT_FUNCTION_ARG, newValue);
            }
            
            int attached() {
                return this->get(SQLITE_LIMIT_ATTACHED);
            }
            
            void attached(int newValue) {
                this->set(SQLITE_LIMIT_ATTACHED, newValue);
            }
            
            int like_pattern_length() {
                return this->get(SQLITE_LIMIT_LIKE_PATTERN_LENGTH);
            }
            
            void like_pattern_length(int newValue) {
                this->set(SQLITE_LIMIT_LIKE_PATTERN_LENGTH, newValue);
            }
            
            int variable_number() {
                return this->get(SQLITE_LIMIT_VARIABLE_NUMBER);
            }
            
            void variable_number(int newValue) {
                this->set(SQLITE_LIMIT_VARIABLE_NUMBER, newValue);
            }
            
            int trigger_depth() {
                return this->get(SQLITE_LIMIT_TRIGGER_DEPTH);
            }
            
            void trigger_depth(int newValue) {
                this->set(SQLITE_LIMIT_TRIGGER_DEPTH, newValue);
            }
            
            int worker_threads() {
                return this->get(SQLITE_LIMIT_WORKER_THREADS);
            }
            
            void worker_threads(int newValue) {
                this->set(SQLITE_LIMIT_WORKER_THREADS, newValue);
            }
            
        protected:
            storage_type &storage;
            
            template<class ...Ts>
            friend struct storage_t;
            
            /**
             *  Stores limit set between connections.
             */
            std::map<int, int> limits;
            
            int get(int id) {
                auto connection = this->storage.get_or_create_connection();
                return sqlite3_limit(connection->get_db(), id, -1);
            }
            
            void set(int id, int newValue) {
                this->limits[id] = newValue;
                auto connection = this->storage.get_or_create_connection();
                sqlite3_limit(connection->get_db(), id, newValue);
            }
        };
    }
}
