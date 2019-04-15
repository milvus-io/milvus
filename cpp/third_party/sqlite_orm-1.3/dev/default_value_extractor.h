#pragma once

#include <memory>   //  std::shared_ptr
#include <string>   //  std::string
#include <sstream>  //  std::stringstream

#include "constraints.h"

namespace sqlite_orm {
    
    namespace internal {
        
        /**
         *  This class is used in tuple interation to know whether tuple constains `default_value_t`
         *  constraint class and what it's value if it is
         */
        struct default_value_extractor {
            
            template<class A>
            std::shared_ptr<std::string> operator() (const A &) {
                return {};
            }
            
            template<class T>
            std::shared_ptr<std::string> operator() (const constraints::default_t<T> &t) {
                std::stringstream ss;
                ss << t.value;
                return std::make_shared<std::string>(ss.str());
            }
        };
        
    }
    
}
