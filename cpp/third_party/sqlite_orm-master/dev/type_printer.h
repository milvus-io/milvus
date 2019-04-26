#pragma once

#include <string>   //  std::string
#include <memory>   //  std::shared_ptr, std::unique_ptr
#include <vector>   //  std::vector

namespace sqlite_orm {
    
    /**
     *  This class accepts c++ type and transfers it to sqlite name (int -> INTEGER, std::string -> TEXT)
     */
    template<class T>
    struct type_printer;
    
    struct integer_printer {
        inline const std::string& print() {
            static const std::string res = "INTEGER";
            return res;
        }
    };
    
    struct text_printer {
        inline const std::string& print() {
            static const std::string res = "TEXT";
            return res;
        }
    };
    
    struct real_printer {
        inline const std::string& print() {
            static const std::string res = "REAL";
            return res;
        }
    };
    
    struct blob_printer {
        inline const std::string& print() {
            static const std::string res = "BLOB";
            return res;
        }
    };
    
    //Note unsigned/signed char and simple char used for storing integer values, not char values.
    template<>
    struct type_printer<unsigned char> : public integer_printer {};
    
    template<>
    struct type_printer<signed char> : public integer_printer {};
    
    template<>
    struct type_printer<char> : public integer_printer {};
    
    template<>
    struct type_printer<unsigned short int> : public integer_printer {};
    
    template<>
    struct type_printer<short> : public integer_printer {};
    
    template<>
    struct type_printer<unsigned int> : public integer_printer {};
    
    template<>
    struct type_printer<int> : public integer_printer {};
    
    template<>
    struct type_printer<unsigned long> : public integer_printer {};
    
    template<>
    struct type_printer<long> : public integer_printer {};
    
    template<>
    struct type_printer<unsigned long long> : public integer_printer {};
    
    template<>
    struct type_printer<long long> : public integer_printer {};
    
    template<>
    struct type_printer<bool> : public integer_printer {};
    
    template<>
    struct type_printer<std::string> : public text_printer {};
    
    template<>
    struct type_printer<std::wstring> : public text_printer {};
    
    template<>
    struct type_printer<const char*> : public text_printer {};
    
    template<>
    struct type_printer<float> : public real_printer {};
    
    template<>
    struct type_printer<double> : public real_printer {};
    
    template<class T>
    struct type_printer<std::shared_ptr<T>> : public type_printer<T> {};
    
    template<class T>
    struct type_printer<std::unique_ptr<T>> : public type_printer<T> {};
    
    template<>
    struct type_printer<std::vector<char>> : public blob_printer {};
}
