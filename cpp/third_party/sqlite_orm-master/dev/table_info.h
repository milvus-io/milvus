#pragma once

#include <string>   //  std::string

namespace sqlite_orm {
    
    struct table_info {
        int cid;
        std::string name;
        std::string type;
        bool notnull;
        std::string dflt_value;
        int pk;
    };
    
}
