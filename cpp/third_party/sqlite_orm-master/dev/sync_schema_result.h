#pragma once

#include <ostream>

namespace sqlite_orm {
    
    enum class sync_schema_result {
        
        /**
         *  created new table, table with the same tablename did not exist
         */
        new_table_created,
        
        /**
         *  table schema is the same as storage, nothing to be done
         */
        already_in_sync,
        
        /**
         *  removed excess columns in table (than storage) without dropping a table
         */
        old_columns_removed,
        
        /**
         *  lacking columns in table (than storage) added without dropping a table
         */
        new_columns_added,
        
        /**
         *  both old_columns_removed and new_columns_added
         */
        new_columns_added_and_old_columns_removed,
        
        /**
         *  old table is dropped and new is recreated. Reasons :
         *      1. delete excess columns in the table than storage if preseve = false
         *      2. Lacking columns in the table cannot be added due to NULL and DEFAULT constraint
         *      3. Reasons 1 and 2 both together
         *      4. data_type mismatch between table and storage.
         */
        dropped_and_recreated,
    };
    
    
    inline std::ostream& operator<<(std::ostream &os, sync_schema_result value) {
        switch(value){
            case sync_schema_result::new_table_created: return os << "new table created";
            case sync_schema_result::already_in_sync: return os << "table and storage is already in sync.";
            case sync_schema_result::old_columns_removed: return os << "old excess columns removed";
            case sync_schema_result::new_columns_added: return os << "new columns added";
            case sync_schema_result::new_columns_added_and_old_columns_removed: return os << "old excess columns removed and new columns added";
            case sync_schema_result::dropped_and_recreated: return os << "old table dropped and recreated";
        }
    }
}
