#pragma once

#include <string>   //  std::string

namespace sqlite_orm {
    
    namespace internal {
        
        struct rowid_t {
            operator std::string() const {
                return "rowid";
            }
        };
        
        struct oid_t {
            operator std::string() const {
                return "oid";
            }
        };
        
        struct _rowid_t {
            operator std::string() const {
                return "_rowid_";
            }
        };
        
        template<class T>
        struct table_rowid_t : public rowid_t {
            using type = T;
        };
        
        template<class T>
        struct table_oid_t : public oid_t {
            using type = T;
        };
        template<class T>
        struct table__rowid_t : public _rowid_t {
            using type = T;
        };
        
    }
    
    inline internal::rowid_t rowid() {
        return {};
    }
    
    inline internal::oid_t oid() {
        return {};
    }
    
    inline internal::_rowid_t _rowid_() {
        return {};
    }
    
    template<class T>
    internal::table_rowid_t<T> rowid() {
        return {};
    }
    
    template<class T>
    internal::table_oid_t<T> oid() {
        return {};
    }
    
    template<class T>
    internal::table__rowid_t<T> _rowid_() {
        return {};
    }
}
