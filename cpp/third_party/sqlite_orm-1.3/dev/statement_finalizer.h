#pragma once

#include <sqlite3.h>

namespace sqlite_orm {
    
    /**
     *  Guard class which finalizes `sqlite3_stmt` in dtor
     */
    struct statement_finalizer {
        sqlite3_stmt *stmt = nullptr;
        
        statement_finalizer(decltype(stmt) stmt_): stmt(stmt_) {}
        
        inline ~statement_finalizer() {
            sqlite3_finalize(this->stmt);
        }
    };
}
