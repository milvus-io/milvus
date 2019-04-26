#pragma once

#include <type_traits>  //  std::enable_if, std::is_member_pointer

#include "select_constraints.h"
#include "column.h"

namespace sqlite_orm {
    
    namespace internal {
        
        /**
         *  Trait class used to define table mapped type by setter/getter/member
         *  T - member pointer
         */
        template<class T, class SFINAE = void>
        struct table_type;
        
        template<class O, class F>
        struct table_type<F O::*, typename std::enable_if<std::is_member_pointer<F O::*>::value && !std::is_member_function_pointer<F O::*>::value>::type> {
            using type = O;
        };
        
        template<class T>
        struct table_type<T, typename std::enable_if<is_getter<T>::value>::type> {
            using type = typename getter_traits<T>::object_type;
        };
        
        template<class T>
        struct table_type<T, typename std::enable_if<is_setter<T>::value>::type> {
            using type = typename setter_traits<T>::object_type;
        };
        
        template<class T, class F>
        struct table_type<column_pointer<T, F>, void> {
            using type = T;
        };
    }
}
