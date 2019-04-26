#pragma once

#include <type_traits>  //  std::enable_if, std::is_base_of, std::is_member_pointer
#include <sstream>  //  std::stringstream
#include <string>   //  std::string

namespace sqlite_orm {
    
    /**
     *  This is base class for every class which is used as a custom table alias.
     *  For more information please look through self_join.cpp example
     */
    struct alias_tag {};
    
    namespace internal {
        
        /**
         *  This is a common built-in class used for custom single character table aliases.
         *  Also you can use language aliases `alias_a`, `alias_b` etc. instead
         */
        template<class T, char A>
        struct table_alias : alias_tag {
            using type = T;
            
            static char get() {
                return A;
            }
        };
        
        /**
         *  Column expression with table alias attached like 'C.ID'. This is not a column alias
         */
        template<class T, class C>
        struct alias_column_t {
            using alias_type = T;
            using column_type = C;
            
            column_type column;
            
            alias_column_t() {};
            
            alias_column_t(column_type column_): column(column_) {}
        };
        
        template<class T, class SFINAE = void>
        struct alias_extractor;
        
        template<class T>
        struct alias_extractor<T, typename std::enable_if<std::is_base_of<alias_tag, T>::value>::type> {
            static std::string get() {
                std::stringstream ss;
                ss << T::get();
                return ss.str();
            }
        };
        
        template<class T>
        struct alias_extractor<T, typename std::enable_if<!std::is_base_of<alias_tag, T>::value>::type> {
            static std::string get() {
                return {};
            }
        };
        
        template<class T, class E>
        struct as_t {
            using alias_type = T;
            using expression_type = E;
            
            expression_type expression;
        };
        
        template<class T>
        struct alias_holder {
            using type = T;
        };
    }
    
    /**
     *  @return column with table alias attached. Place it instead of a column statement in case you need to specify a
     *  column with table alias prefix like 'a.column'. For more information please look through self_join.cpp example
     */
    template<class T, class C>
    internal::alias_column_t<T, C> alias_column(C c) {
        static_assert(std::is_member_pointer<C>::value, "alias_column argument must be a member pointer mapped to a storage");
        return {c};
    }
    
    template<class T, class E>
    internal::as_t<T, E> as(E expression) {
        return {std::move(expression)};
    }
    
    template<class T>
    internal::alias_holder<T> get() {
        return {};
    }
    
    template<class T> using alias_a = internal::table_alias<T, 'a'>;
    template<class T> using alias_b = internal::table_alias<T, 'b'>;
    template<class T> using alias_c = internal::table_alias<T, 'c'>;
    template<class T> using alias_d = internal::table_alias<T, 'd'>;
    template<class T> using alias_e = internal::table_alias<T, 'e'>;
    template<class T> using alias_f = internal::table_alias<T, 'f'>;
    template<class T> using alias_g = internal::table_alias<T, 'g'>;
    template<class T> using alias_h = internal::table_alias<T, 'h'>;
    template<class T> using alias_i = internal::table_alias<T, 'i'>;
    template<class T> using alias_j = internal::table_alias<T, 'j'>;
    template<class T> using alias_k = internal::table_alias<T, 'k'>;
    template<class T> using alias_l = internal::table_alias<T, 'l'>;
    template<class T> using alias_m = internal::table_alias<T, 'm'>;
    template<class T> using alias_n = internal::table_alias<T, 'n'>;
    template<class T> using alias_o = internal::table_alias<T, 'o'>;
    template<class T> using alias_p = internal::table_alias<T, 'p'>;
    template<class T> using alias_q = internal::table_alias<T, 'q'>;
    template<class T> using alias_r = internal::table_alias<T, 'r'>;
    template<class T> using alias_s = internal::table_alias<T, 's'>;
    template<class T> using alias_t = internal::table_alias<T, 't'>;
    template<class T> using alias_u = internal::table_alias<T, 'u'>;
    template<class T> using alias_v = internal::table_alias<T, 'v'>;
    template<class T> using alias_w = internal::table_alias<T, 'w'>;
    template<class T> using alias_x = internal::table_alias<T, 'x'>;
    template<class T> using alias_y = internal::table_alias<T, 'y'>;
    template<class T> using alias_z = internal::table_alias<T, 'z'>;
}
