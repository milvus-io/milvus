#pragma once

#include <tuple>    //  std::tuple, std::make_tuple
#include <string>   //  std::string

namespace sqlite_orm {
    
    namespace internal {
        
        template<class ...Cols>
        struct index_t {
            using columns_type = std::tuple<Cols...>;
            using object_type = void;
            
            std::string name;
            bool unique;
            columns_type columns;
            
            template<class L>
            void for_each_column_with_constraints(L) {}
        };
    }
    
    template<class ...Cols>
    internal::index_t<Cols...> make_index(const std::string &name, Cols ...cols) {
        return {name, false, std::make_tuple(cols...)};
    }
    
    template<class ...Cols>
    internal::index_t<Cols...> make_unique_index(const std::string &name, Cols ...cols) {
        return {name, true, std::make_tuple(cols...)};
    }
}
