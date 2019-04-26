#pragma once

#include <string>   //  std::string
#include <tuple>    //  std::make_tuple
#include <type_traits>  //  std::forward, std::is_base_of, std::enable_if

#include "conditions.h"
#include "operators.h"

namespace sqlite_orm {
    
    namespace core_functions {
        
        /**
         *  Base class for operator overloading
         */
        struct core_function_t {};
        
        /**
         *  LENGTH(x) function https://sqlite.org/lang_corefunc.html#length
         */
        template<class T>
        struct length_t : public core_function_t {
            T t;
            
            length_t() = default;
            
            length_t(T t_): t(t_) {}
            
            operator std::string() const {
                return "LENGTH";
            }
        };
        
        /**
         *  ABS(x) function https://sqlite.org/lang_corefunc.html#abs
         */
        template<class T>
        struct abs_t : public core_function_t {
            T t;
            
            abs_t() = default;
            
            abs_t(T t_): t(t_) {}
            
            operator std::string() const {
                return "ABS";
            }
        };
        
        /**
         *  LOWER(x) function https://sqlite.org/lang_corefunc.html#lower
         */
        template<class T>
        struct lower_t : public core_function_t {
            T t;
            
            lower_t() = default;
            
            lower_t(T t_): t(t_) {}
            
            operator std::string() const {
                return "LOWER";
            }
        };
        
        /**
         *  UPPER(x) function https://sqlite.org/lang_corefunc.html#upper
         */
        template<class T>
        struct upper_t : public core_function_t {
            T t;
            
            upper_t() = default;
            
            upper_t(T t_): t(t_) {}
            
            operator std::string() const {
                return "UPPER";
            }
        };
        
        /**
         *  CHANGES() function https://sqlite.org/lang_corefunc.html#changes
         */
        struct changes_t : public core_function_t {
            
            operator std::string() const {
                return "CHANGES";
            }
        };
        
        /**
         *  TRIM(X) function https://sqlite.org/lang_corefunc.html#trim
         */
        template<class X>
        struct trim_single_t : public core_function_t {
            X x;
            
            trim_single_t() = default;
            
            trim_single_t(X x_): x(x_) {}
            
            operator std::string() const {
                return "TRIM";
            }
        };
        
        /**
         *  TRIM(X,Y) function https://sqlite.org/lang_corefunc.html#trim
         */
        template<class X, class Y>
        struct trim_double_t : public core_function_t {
            X x;
            Y y;
            
            trim_double_t() = default;
            
            trim_double_t(X x_, Y y_): x(x_), y(y_) {}
            
            operator std::string() const {
                return static_cast<std::string>(trim_single_t<X>(0));
            }
        };
        
        /**
         *  LTRIM(X) function https://sqlite.org/lang_corefunc.html#ltrim
         */
        template<class X>
        struct ltrim_single_t : public core_function_t {
            X x;
            
            ltrim_single_t() = default;
            
            ltrim_single_t(X x_): x(x_) {}
            
            operator std::string() const {
                return "LTRIM";
            }
        };
        
        /**
         *  LTRIM(X,Y) function https://sqlite.org/lang_corefunc.html#ltrim
         */
        template<class X, class Y>
        struct ltrim_double_t : public core_function_t {
            X x;
            Y y;
            
            ltrim_double_t() = default;
            
            ltrim_double_t(X x_, Y y_): x(x_), y(y_) {}
            
            operator std::string() const {
                return static_cast<std::string>(ltrim_single_t<X>(0));
            }
        };
        
        /**
         *  RTRIM(X) function https://sqlite.org/lang_corefunc.html#rtrim
         */
        template<class X>
        struct rtrim_single_t : public core_function_t {
            X x;
            
            rtrim_single_t() = default;
            
            rtrim_single_t(X x_): x(x_) {}
            
            operator std::string() const {
                return "RTRIM";
            }
        };
        
        /**
         *  RTRIM(X,Y) function https://sqlite.org/lang_corefunc.html#rtrim
         */
        template<class X, class Y>
        struct rtrim_double_t : public core_function_t {
            X x;
            Y y;
            
            rtrim_double_t() = default;
            
            rtrim_double_t(X x_, Y y_): x(x_), y(y_) {}
            
            operator std::string() const {
                return static_cast<std::string>(rtrim_single_t<X>(0));
            }
        };
        
        
        
#if SQLITE_VERSION_NUMBER >= 3007016
        
        /**
         *  CHAR(X1,X2,...,XN) function https://sqlite.org/lang_corefunc.html#char
         */
        template<class ...Args>
        struct char_t_ : public core_function_t {
            using args_type = std::tuple<Args...>;
            
            args_type args;
            
            char_t_() = default;
            
            char_t_(args_type args_): args(args_) {}
            
            operator std::string() const {
                return "CHAR";
            }
        };
        
        struct random_t : core_function_t, internal::arithmetic_t {
            
            operator std::string() const {
                return "RANDOM";
            }
        };
        
#endif
        template<class T, class ...Args>
        struct date_t : core_function_t {
            using modifiers_type = std::tuple<Args...>;
            
            T timestring;
            modifiers_type modifiers;
            
            date_t() = default;
            
            date_t(T timestring_, modifiers_type modifiers_): timestring(timestring_), modifiers(modifiers_) {}
            
            operator std::string() const {
                return "DATE";
            }
        };
        
        template<class T, class ...Args>
        struct datetime_t : core_function_t {
            using modifiers_type = std::tuple<Args...>;
            
            T timestring;
            modifiers_type modifiers;
            
            datetime_t() = default;
            
            datetime_t(T timestring_, modifiers_type modifiers_): timestring(timestring_), modifiers(modifiers_) {}
            
            operator std::string() const {
                return "DATETIME";
            }
        };
        
        template<class T, class ...Args>
        struct julianday_t : core_function_t, internal::arithmetic_t {
            using modifiers_type = std::tuple<Args...>;
            
            T timestring;
            modifiers_type modifiers;
            
            julianday_t() = default;
            
            julianday_t(T timestring_, modifiers_type modifiers_): timestring(timestring_), modifiers(modifiers_) {}
            
            operator std::string() const {
                return "JULIANDAY";
            }
        };
    }
    
    /**
     *  Cute operators for core functions
     */
    
    template<
    class F,
    class R,
    typename = typename std::enable_if<std::is_base_of<core_functions::core_function_t, F>::value>::type>
    conditions::lesser_than_t<F, R> operator<(F f, R r) {
        return {f, r};
    }
    
    template<
    class F,
    class R,
    typename = typename std::enable_if<std::is_base_of<core_functions::core_function_t, F>::value>::type>
    conditions::lesser_or_equal_t<F, R> operator<=(F f, R r) {
        return {f, r};
    }
    
    template<
    class F,
    class R,
    typename = typename std::enable_if<std::is_base_of<core_functions::core_function_t, F>::value>::type>
    conditions::greater_than_t<F, R> operator>(F f, R r) {
        return {f, r};
    }
    
    template<
    class F,
    class R,
    typename = typename std::enable_if<std::is_base_of<core_functions::core_function_t, F>::value>::type>
    conditions::greater_or_equal_t<F, R> operator>=(F f, R r) {
        return {f, r};
    }
    
    template<
    class F,
    class R,
    typename = typename std::enable_if<std::is_base_of<core_functions::core_function_t, F>::value>::type>
    conditions::is_equal_t<F, R> operator==(F f, R r) {
        return {f, r};
    }
    
    template<
    class F,
    class R,
    typename = typename std::enable_if<std::is_base_of<core_functions::core_function_t, F>::value>::type>
    conditions::is_not_equal_t<F, R> operator!=(F f, R r) {
        return {f, r};
    }
    
    inline core_functions::random_t random() {
        return {};
    }
    
    template<class T, class ...Args, class Res = core_functions::date_t<T, Args...>>
    Res date(T timestring, Args ...modifiers) {
        return Res(timestring, std::make_tuple(std::forward<Args>(modifiers)...));
    }
    
    template<class T, class ...Args, class Res = core_functions::datetime_t<T, Args...>>
    Res datetime(T timestring, Args ...modifiers) {
        return Res(timestring, std::make_tuple(std::forward<Args>(modifiers)...));
    }
    
    template<class T, class ...Args, class Res = core_functions::julianday_t<T, Args...>>
    Res julianday(T timestring, Args ...modifiers) {
        return Res(timestring, std::make_tuple(std::forward<Args>(modifiers)...));
    }
    
#if SQLITE_VERSION_NUMBER >= 3007016
    
    template<class ...Args>
    core_functions::char_t_<Args...> char_(Args&& ...args) {
        using result_type = core_functions::char_t_<Args...>;
        return result_type(std::make_tuple(std::forward<Args>(args)...));
    }
    
#endif
    
    template<class X, class Res = core_functions::trim_single_t<X>>
    Res trim(X x) {
        return Res(x);
    }
    
    template<class X, class Y, class Res = core_functions::trim_double_t<X, Y>>
    Res trim(X x, Y y) {
        return Res(x, y);
    }
    
    template<class X, class Res = core_functions::ltrim_single_t<X>>
    Res ltrim(X x) {
        return Res(x);
    }
    
    template<class X, class Y, class Res = core_functions::ltrim_double_t<X, Y>>
    Res ltrim(X x, Y y) {
        return Res(x, y);
    }
    
    template<class X, class Res = core_functions::rtrim_single_t<X>>
    Res rtrim(X x) {
        return Res(x);
    }
    
    template<class X, class Y, class Res = core_functions::rtrim_double_t<X, Y>>
    Res rtrim(X x, Y y) {
        return Res(x, y);
    }
    
    inline core_functions::changes_t changes() {
        return {};
    }
    
    template<class T>
    core_functions::length_t<T> length(T t) {
        using result_type = core_functions::length_t<T>;
        return result_type(t);
    }
    
    template<class T>
    core_functions::abs_t<T> abs(T t) {
        using result_type = core_functions::abs_t<T>;
        return result_type(t);
    }
    
    template<class T, class Res = core_functions::lower_t<T>>
    Res lower(T t) {
        return Res(t);
    }
    
    template<class T, class Res = core_functions::upper_t<T>>
    Res upper(T t) {
        return Res(t);
    }
    
    template<
    class L,
    class R,
    typename = typename std::enable_if<(std::is_base_of<internal::arithmetic_t, L>::value + std::is_base_of<internal::arithmetic_t, R>::value > 0)>::type>
    internal::add_t<L, R> operator+(L l, R r) {
        return {std::move(l), std::move(r)};
    }
    
    template<
    class L,
    class R,
    typename = typename std::enable_if<(std::is_base_of<internal::arithmetic_t, L>::value + std::is_base_of<internal::arithmetic_t, R>::value > 0)>::type>
    internal::sub_t<L, R> operator-(L l, R r) {
        return {std::move(l), std::move(r)};
    }
    
    template<
    class L,
    class R,
    typename = typename std::enable_if<(std::is_base_of<internal::arithmetic_t, L>::value + std::is_base_of<internal::arithmetic_t, R>::value > 0)>::type>
    internal::mul_t<L, R> operator*(L l, R r) {
        return {std::move(l), std::move(r)};
    }
    
    template<
    class L,
    class R,
    typename = typename std::enable_if<(std::is_base_of<internal::arithmetic_t, L>::value + std::is_base_of<internal::arithmetic_t, R>::value > 0)>::type>
    internal::div_t<L, R> operator/(L l, R r) {
        return {std::move(l), std::move(r)};
    }
    
    template<
    class L,
    class R,
    typename = typename std::enable_if<(std::is_base_of<internal::arithmetic_t, L>::value + std::is_base_of<internal::arithmetic_t, R>::value > 0)>::type>
    internal::mod_t<L, R> operator%(L l, R r) {
        return {std::move(l), std::move(r)};
    }
}
