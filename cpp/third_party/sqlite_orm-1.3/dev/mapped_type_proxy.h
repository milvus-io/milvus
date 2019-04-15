#pragma once

#include "alias.h"

namespace sqlite_orm {
    
    namespace internal {
        
        /**
         *  If T is alias than mapped_type_proxy<T>::type is alias::type
         *  otherwise T is T.
         */
        template<class T>
        struct mapped_type_proxy {
            using type = T;
        };
        
        template<class T, char A>
        struct mapped_type_proxy<alias<T, A>> {
            using type = T;
        };
    }
}
