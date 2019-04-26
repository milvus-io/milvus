#pragma once

#include <type_traits>  //  std::enable_if

#include "column.h"

namespace sqlite_orm {
    namespace internal {
        
        template<class T, class SFINAE = void>
        struct field_value_holder;
        
        template<class T>
        struct field_value_holder<T, typename std::enable_if<getter_traits<T>::returns_lvalue>::type> {
            using type = typename getter_traits<T>::field_type;
            
            const type &value;
        };
        
        template<class T>
        struct field_value_holder<T, typename std::enable_if<!getter_traits<T>::returns_lvalue>::type> {
            using type = typename getter_traits<T>::field_type;
            
            type value;
        };
    }
}
