#pragma once

namespace sqlite_orm {
    
    namespace aggregate_functions {
        
        template<class T>
        struct avg_t {
            T t;
            
            operator std::string() const {
                return "AVG";
            }
        };
        
        template<class T>
        struct count_t {
            T t;
            
            operator std::string() const {
                return "COUNT";
            }
        };
        
        /**
         *  T is use to specify type explicitly for queries like
         *  SELECT COUNT(*) FROM table_name;
         *  T can be omitted with void.
         */
        template<class T>
        struct count_asterisk_t {
            using type = T;
            
            operator std::string() const {
                return "COUNT";
            }
        };
        
        struct count_asterisk_without_type {
            operator std::string() const {
                return "COUNT";
            }
        };
        
        template<class T>
        struct sum_t {
            T t;
            
            operator std::string() const {
                return "SUM";
            }
        };
        
        template<class T>
        struct total_t {
            T t;
            
            operator std::string() const {
                return "TOTAL";
            }
        };
        
        template<class T>
        struct max_t {
            T t;
            
            operator std::string() const {
                return "MAX";
            }
        };
        
        template<class T>
        struct min_t {
            T t;
            
            operator std::string() const {
                return "MIN";
            }
        };
        
        template<class T>
        struct group_concat_single_t {
            T t;
            
            operator std::string() const {
                return "GROUP_CONCAT";
            }
        };
        
        template<class T>
        struct group_concat_double_t {
            T t;
            std::string y;
            
            operator std::string() const {
                return "GROUP_CONCAT";
            }
        };
        
    }
    
    template<class T>
    aggregate_functions::avg_t<T> avg(T t) {
        return {t};
    }
    
    template<class T>
    aggregate_functions::count_t<T> count(T t) {
        return {t};
    }
    
    inline aggregate_functions::count_asterisk_without_type count() {
        return {};
    }
    
    template<class T>
    aggregate_functions::count_asterisk_t<T> count() {
        return {};
    }
    
    template<class T>
    aggregate_functions::sum_t<T> sum(T t) {
        return {t};
    }
    
    template<class T>
    aggregate_functions::max_t<T> max(T t) {
        return {t};
    }
    
    template<class T>
    aggregate_functions::min_t<T> min(T t) {
        return {t};
    }
    
    template<class T>
    aggregate_functions::total_t<T> total(T t) {
        return {t};
    }
    
    template<class T>
    aggregate_functions::group_concat_single_t<T> group_concat(T t) {
        return {t};
    }
    
    template<class T, class Y>
    aggregate_functions::group_concat_double_t<T> group_concat(T t, Y y) {
        return {t, y};
    }
}
