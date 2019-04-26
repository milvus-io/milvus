#pragma once

#include <string>   //  std::string
#include <memory>   //  std::unique_ptr
#include <array>    //  std::array
#include <algorithm>    //  std::transform
#include <locale>   // std::toupper

namespace sqlite_orm {
    
    /**
     *  Caps case cause of 1) delete keyword; 2) https://www.sqlite.org/pragma.html#pragma_journal_mode original spelling
     */
    #ifdef DELETE
        #undef DELETE
    #endif
    enum class journal_mode : char {
        DELETE = 0,
        TRUNCATE = 1,
        PERSIST = 2,
        MEMORY = 3,
        WAL = 4,
        OFF = 5,
        };
    
    namespace internal {
        
        inline const std::string& to_string(journal_mode j) {
            static std::string res[] = {
                "DELETE",
                "TRUNCATE",
                "PERSIST",
                "MEMORY",
                "WAL",
                "OFF",
            };
            return res[static_cast<int>(j)];
        }
        
        inline std::unique_ptr<journal_mode> journal_mode_from_string(const std::string &str) {
            std::string upper_str;
            std::transform(str.begin(), str.end(), std::back_inserter(upper_str), ::toupper);
            static std::array<journal_mode, 6> all = {
                journal_mode::DELETE,
                journal_mode::TRUNCATE,
                journal_mode::PERSIST,
                journal_mode::MEMORY,
                journal_mode::WAL,
                journal_mode::OFF,
            };
            for(auto j : all) {
                if(to_string(j) == upper_str){
                    return std::make_unique<journal_mode>(j);
                }
            }
            return {};
        }
    }
}
