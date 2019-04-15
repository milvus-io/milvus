#pragma once

#if defined(_MSC_VER)
# if defined(min)
__pragma(push_macro("min"))
# undef min
# define __RESTORE_MIN__
# endif
# if defined(max)
__pragma(push_macro("max"))
# undef max
# define __RESTORE_MAX__
# endif
#endif // defined(_MSC_VER)

#pragma once

#include <system_error>  // std::error_code, std::system_error
#include <string>   //  std::string
#include <sqlite3.h>
#include <stdexcept>

namespace sqlite_orm {
    
    enum class orm_error_code {
        not_found = 1,
        type_is_not_mapped_to_storage,
        trying_to_dereference_null_iterator,
        too_many_tables_specified,
        incorrect_set_fields_specified,
        column_not_found,
        table_has_no_primary_key_column,
        cannot_start_a_transaction_within_a_transaction,
        no_active_transaction,
    };
    
}

namespace sqlite_orm {
    
    class orm_error_category : public std::error_category {
    public:
        
        const char *name() const noexcept override final {
            return "ORM error";
        }
        
        std::string message(int c) const override final {
            switch (static_cast<orm_error_code>(c)) {
                case orm_error_code::not_found:
                    return "Not found";
                case orm_error_code::type_is_not_mapped_to_storage:
                    return "Type is not mapped to storage";
                case orm_error_code::trying_to_dereference_null_iterator:
                    return "Trying to dereference null iterator";
                case orm_error_code::too_many_tables_specified:
                    return "Too many tables specified";
                case orm_error_code::incorrect_set_fields_specified:
                    return "Incorrect set fields specified";
                case orm_error_code::column_not_found:
                    return "Column not found";
                case orm_error_code::table_has_no_primary_key_column:
                    return "Table has no primary key column";
                case orm_error_code::cannot_start_a_transaction_within_a_transaction:
                    return "Cannot start a transaction within a transaction";
                case orm_error_code::no_active_transaction:
                    return "No active transaction";
                default:
                    return "unknown error";
            }
        }
    };
    
    class sqlite_error_category : public std::error_category {
    public:
        
        const char *name() const noexcept override final {
            return "SQLite error";
        }
        
        std::string message(int c) const override final {
            return sqlite3_errstr(c);
        }
    };
    
    inline const orm_error_category& get_orm_error_category() {
        static orm_error_category res;
        return res;
    }
    
    inline const sqlite_error_category& get_sqlite_error_category() {
        static sqlite_error_category res;
        return res;
    }
}

namespace std
{
    template <>
    struct is_error_code_enum<sqlite_orm::orm_error_code> : std::true_type{};
    
    inline std::error_code make_error_code(sqlite_orm::orm_error_code errorCode) {
        return std::error_code(static_cast<int>(errorCode), sqlite_orm::get_orm_error_category());
    }
}
#pragma once

#include <map>  //  std::map
#include <string>   //  std::string
#include <regex>    //  std::regex, std::regex_match
#include <memory>   //  std::make_shared, std::shared_ptr
#include <vector>   //  std::vector

namespace sqlite_orm {
    using int64 = sqlite_int64;
    using uint64 = sqlite_uint64;
    
    //  numeric and real are the same for c++
    enum class sqlite_type {
        INTEGER,
        TEXT,
        BLOB,
        REAL,
    };
    
    /**
     *  @param str case doesn't matter - it is uppercased before comparing.
     */
    inline std::shared_ptr<sqlite_type> to_sqlite_type(const std::string &str) {
        auto asciiStringToUpper = [](std::string &s){
            std::transform(s.begin(),
                           s.end(),
                           s.begin(),
                           [](char c){
                               return std::toupper(c);
                           });
        };
        auto upperStr = str;
        asciiStringToUpper(upperStr);
        
        static std::map<sqlite_type, std::vector<std::regex>> typeMap = {
            { sqlite_type::INTEGER, {
                std::regex("INT"),
                std::regex("INT.*"),
                std::regex("TINYINT"),
                std::regex("SMALLINT"),
                std::regex("MEDIUMINT"),
                std::regex("BIGINT"),
                std::regex("UNSIGNED BIG INT"),
                std::regex("INT2"),
                std::regex("INT8"),
            } }, { sqlite_type::TEXT, {
                std::regex("CHARACTER\\([[:digit:]]+\\)"),
                std::regex("VARCHAR\\([[:digit:]]+\\)"),
                std::regex("VARYING CHARACTER\\([[:digit:]]+\\)"),
                std::regex("NCHAR\\([[:digit:]]+\\)"),
                std::regex("NATIVE CHARACTER\\([[:digit:]]+\\)"),
                std::regex("NVARCHAR\\([[:digit:]]+\\)"),
                std::regex("CLOB"),
                std::regex("TEXT"),
            } }, { sqlite_type::BLOB, {
                std::regex("BLOB"),
            } }, { sqlite_type::REAL, {
                std::regex("REAL"),
                std::regex("DOUBLE"),
                std::regex("DOUBLE PRECISION"),
                std::regex("FLOAT"),
                std::regex("NUMERIC"),
                std::regex("DECIMAL\\([[:digit:]]+,[[:digit:]]+\\)"),
                std::regex("BOOLEAN"),
                std::regex("DATE"),
                std::regex("DATETIME"),
            } },
        };
        for(auto &p : typeMap) {
            for(auto &r : p.second) {
                if(std::regex_match(upperStr, r)){
                    return std::make_shared<sqlite_type>(p.first);
                }
            }
        }
        
        return {};
    }
}
#pragma once

#include <tuple>    //  std::tuple
#include <type_traits>  //  std::false_type, std::true_type
#include <utility>  //  std::index_sequence, std::index_sequence_for

namespace sqlite_orm {
    
    //  got from here http://stackoverflow.com/questions/25958259/how-do-i-find-out-if-a-tuple-contains-a-type
    namespace tuple_helper {
        
        template <typename T, typename Tuple>
        struct has_type;
        
        template <typename T>
        struct has_type<T, std::tuple<>> : std::false_type {};
        
        template <typename T, typename U, typename... Ts>
        struct has_type<T, std::tuple<U, Ts...>> : has_type<T, std::tuple<Ts...>> {};
        
        template <typename T, typename... Ts>
        struct has_type<T, std::tuple<T, Ts...>> : std::true_type {};
        
        template <typename T, typename Tuple>
        using tuple_contains_type = typename has_type<T, Tuple>::type;
        
        template<size_t N, class ...Args>
        struct iterator {
            
            template<class L>
            void operator()(const std::tuple<Args...> &t, L l, bool reverse = true) {
                if(reverse){
                    l(std::get<N>(t));
                    iterator<N - 1, Args...>()(t, l, reverse);
                }else{
                    iterator<N - 1, Args...>()(t, l, reverse);
                    l(std::get<N>(t));
                }
            }
        };
        
        template<class ...Args>
        struct iterator<0, Args...>{
            
            template<class L>
            void operator()(const std::tuple<Args...> &t, L l, bool /*reverse*/ = true) {
                l(std::get<0>(t));
            }
        };
        
        template<size_t N>
        struct iterator<N> {
            
            template<class L>
            void operator()(const std::tuple<> &, L, bool /*reverse*/ = true) {
                //..
            }
        };
        
        template <class F, typename T, std::size_t... I>
        void tuple_for_each_impl(F&& f, const T& t, std::index_sequence<I...>){
            int _[] = { (f(std::get<I>(t)), int{}) ... };
            (void)_;
        }
        
        template <typename F, typename ...Args>
        void tuple_for_each(const std::tuple<Args...>& t, F&& f){
            tuple_for_each_impl(std::forward<F>(f), t, std::index_sequence_for<Args...>{});
        }
    }
}
#pragma once

#include <type_traits>  //  std::false_type, std::true_type, std::integral_constant

namespace sqlite_orm {
    
    //  got from here https://stackoverflow.com/questions/37617677/implementing-a-compile-time-static-if-logic-for-different-string-types-in-a-co
    namespace static_magic {
        
        template <typename T, typename F>
        auto static_if(std::true_type, T t, F f) { return t; }
        
        template <typename T, typename F>
        auto static_if(std::false_type, T t, F f) { return f; }
        
        template <bool B, typename T, typename F>
        auto static_if(T t, F f) { return static_if(std::integral_constant<bool, B>{}, t, f); }
        
        template <bool B, typename T>
        auto static_if(T t) { return static_if(std::integral_constant<bool, B>{}, t, [](auto&&...){}); }
    }
    
}
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
    struct type_printer<double> : public real_printer {};
    
    template<class T>
    struct type_printer<std::shared_ptr<T>> : public type_printer<T> {};
    
    template<class T>
    struct type_printer<std::unique_ptr<T>> : public type_printer<T> {};
    
    template<>
    struct type_printer<std::vector<char>> : public blob_printer {};
}
#pragma once

namespace sqlite_orm {
    
    namespace internal {
        
        enum class collate_argument {
            binary,
            nocase,
            rtrim,
        };
    }
    
}
#pragma once

#include <string>   //  std::string
#include <tuple>    //  std::tuple
#include <sstream>  //  std::stringstream
#include <type_traits>  //  std::is_base_of, std::false_type, std::true_type

namespace sqlite_orm {
    
    namespace constraints {
        
        /**
         *  AUTOINCREMENT constraint class.
         */
        struct autoincrement_t {
            
            operator std::string() const {
                return "AUTOINCREMENT";
            }
        };
        
        /**
         *  PRIMARY KEY constraint class.
         *  Cs is parameter pack which contains columns (member pointer and/or function pointers). Can be empty when used withen `make_column` function.
         */
        template<class ...Cs>
        struct primary_key_t {
            std::tuple<Cs...> columns;
            enum class order_by {
                unspecified,
                ascending,
                descending,
            };
            order_by asc_option = order_by::unspecified;
            
            primary_key_t(decltype(columns) c):columns(std::move(c)){}
            
            using field_type = void;    //  for column iteration. Better be deleted
            using constraints_type = std::tuple<>;
            
            operator std::string() const {
                std::string res = "PRIMARY KEY";
                switch(this->asc_option){
                    case order_by::ascending:
                        res += " ASC";
                        break;
                    case order_by::descending:
                        res += " DESC";
                        break;
                    default:
                        break;
                }
                return res;
            }
            
            primary_key_t<Cs...> asc() const {
                auto res = *this;
                res.asc_option = order_by::ascending;
                return res;
            }
            
            primary_key_t<Cs...> desc() const {
                auto res = *this;
                res.asc_option = order_by::descending;
                return res;
            }
        };
        
        /**
         *  UNIQUE constraint class.
         */
        struct unique_t {
            
            operator std::string() const {
                return "UNIQUE";
            }
        };
        
        /**
         *  DEFAULT constraint class.
         *  T is a value type.
         */
        template<class T>
        struct default_t {
            typedef T value_type;
            
            value_type value;
            
            operator std::string() const {
                std::stringstream ss;
                ss << "DEFAULT ";
                auto needQuotes = std::is_base_of<text_printer, type_printer<T>>::value;
                if(needQuotes){
                    ss << "'";
                }
                ss << this->value;
                if(needQuotes){
                    ss << "'";
                }
                return ss.str();
            }
        };
        
#if SQLITE_VERSION_NUMBER >= 3006019
        
        /**
         *  FOREIGN KEY constraint class.
         *  C is column which has foreign key
         *  R is column which C references to
         *  Available in SQLite 3.6.19 or higher
         */
        template<class C, class R>
        struct foreign_key_t {
            C m = nullptr;
            R r = nullptr;
            
            foreign_key_t(C m_, R r_): m(m_), r(r_) {}
            
            using field_type = void;    //  for column iteration. Better be deleted
            using constraints_type = std::tuple<>;
            
            template<class L>
            void for_each_column(L) {}
            
            template<class ...Opts>
            constexpr bool has_every() const  {
                return false;
            }
        };
        
        /**
         *  C can be a class member pointer, a getter function member pointer or setter
         *  func member pointer
         *  Available in SQLite 3.6.19 or higher
         */
        template<class C>
        struct foreign_key_intermediate_t {
            C m = nullptr;
            
            foreign_key_intermediate_t(C m_): m(m_) {}
            
            template<class T>
            foreign_key_t<C, T> references(T t) {
                using ret_type = foreign_key_t<C, T>;
                return ret_type(this->m, t);
            }
        };
#endif
        
        struct collate_t {
            internal::collate_argument argument;
            
            collate_t(internal::collate_argument argument_): argument(argument_) {}
            
            operator std::string() const {
                std::string res = "COLLATE " + string_from_collate_argument(this->argument);
                return res;
            }
            
            static std::string string_from_collate_argument(internal::collate_argument argument){
                switch(argument){
                    case decltype(argument)::binary: return "BINARY";
                    case decltype(argument)::nocase: return "NOCASE";
                    case decltype(argument)::rtrim: return "RTRIM";
                }
            }
        };
        
        template<class T>
        struct is_constraint : std::false_type {};
        
        template<>
        struct is_constraint<autoincrement_t> : std::true_type {};
        
        template<class ...Cs>
        struct is_constraint<primary_key_t<Cs...>> : std::true_type {};
        
        template<>
        struct is_constraint<unique_t> : std::true_type {};
        
        template<class T>
        struct is_constraint<default_t<T>> : std::true_type {};
        
        template<class C, class R>
        struct is_constraint<foreign_key_t<C, R>> : std::true_type {};
        
        template<>
        struct is_constraint<collate_t> : std::true_type {};
        
        template<class ...Args>
        struct constraints_size;
        
        template<>
        struct constraints_size<> {
            static constexpr const int value = 0;
        };
        
        template<class H, class ...Args>
        struct constraints_size<H, Args...> {
            static constexpr const int value = is_constraint<H>::value + constraints_size<Args...>::value;
        };
    }
    
#if SQLITE_VERSION_NUMBER >= 3006019
    
    /**
     *  FOREIGN KEY constraint construction function that takes member pointer as argument
     *  Available in SQLite 3.6.19 or higher
     */
    template<class O, class F>
    constraints::foreign_key_intermediate_t<F O::*> foreign_key(F O::*m) {
        return {m};
    }
    
    /**
     *  FOREIGN KEY constraint construction function that takes getter function pointer as argument
     *  Available in SQLite 3.6.19 or higher
     */
    template<class O, class F>
    constraints::foreign_key_intermediate_t<const F& (O::*)() const> foreign_key(const F& (O::*getter)() const) {
        using ret_type = constraints::foreign_key_intermediate_t<const F& (O::*)() const>;
        return ret_type(getter);
    }
    
    /**
     *  FOREIGN KEY constraint construction function that takes setter function pointer as argument
     *  Available in SQLite 3.6.19 or higher
     */
    template<class O, class F>
    constraints::foreign_key_intermediate_t<void (O::*)(F)> foreign_key(void (O::*setter)(F)) {
        return {setter};
    }
#endif
    
    /**
     *  UNIQUE constraint builder function.
     */
    inline constraints::unique_t unique() {
        return {};
    }
    
    inline constraints::autoincrement_t autoincrement() {
        return {};
    }
    
    template<class ...Cs>
    inline constraints::primary_key_t<Cs...> primary_key(Cs ...cs) {
        using ret_type = constraints::primary_key_t<Cs...>;
        return ret_type(std::make_tuple(cs...));
    }
    
    template<class T>
    constraints::default_t<T> default_value(T t) {
        return {t};
    }
    
    inline constraints::collate_t collate_nocase() {
        return {internal::collate_argument::nocase};
    }
    
    inline constraints::collate_t collate_binary() {
        return {internal::collate_argument::binary};
    }
    
    inline constraints::collate_t collate_rtrim() {
        return {internal::collate_argument::rtrim};
    }
    
    namespace internal {
        
        /**
         *  FOREIGN KEY traits. Common case
         */
        template<class T>
        struct is_foreign_key : std::false_type {};
        
        /**
         *  FOREIGN KEY traits. Specialized case
         */
        template<class C, class R>
        struct is_foreign_key<constraints::foreign_key_t<C, R>> : std::true_type {};
        
        /**
         *  PRIMARY KEY traits. Common case
         */
        template<class T>
        struct is_primary_key : public std::false_type {};
        
        /**
         *  PRIMARY KEY traits. Specialized case
         */
        template<class ...Cs>
        struct is_primary_key<constraints::primary_key_t<Cs...>> : public std::true_type {};
    }
    
}
#pragma once

#include <type_traits>  //  std::false_type, std::true_type
#include <memory>   //  std::shared_ptr, std::unique_ptr

namespace sqlite_orm {
    
    /**
     *  This is class that tells `sqlite_orm` that type is nullable. Nullable types
     *  are mapped to sqlite database as `NULL` and not-nullable are mapped as `NOT NULL`.
     *  Default nullability status for all types is `NOT NULL`. So if you want to map
     *  custom type as `NULL` (for example: boost::optional) you have to create a specialiation
     *  of type_is_nullable for your type and derive from `std::true_type`.
     */
    template<class T>
    struct type_is_nullable : public std::false_type {
        bool operator()(const T &) const {
            return true;
        }
    };
    
    /**
     *  This is a specialization for std::shared_ptr. std::shared_ptr is nullable in sqlite_orm.
     */
    template<class T>
    struct type_is_nullable<std::shared_ptr<T>> : public std::true_type {
        bool operator()(const std::shared_ptr<T> &t) const {
            return static_cast<bool>(t);
        }
    };
    
    /**
     *  This is a specialization for std::unique_ptr. std::unique_ptr is nullable too.
     */
    template<class T>
    struct type_is_nullable<std::unique_ptr<T>> : public std::true_type {
        bool operator()(const std::unique_ptr<T> &t) const {
            return static_cast<bool>(t);
        }
    };
    
}
#pragma once

#include <memory>   //  std::shared_ptr
#include <string>   //  std::string
#include <sstream>  //  std::stringstream

// #include "constraints.h"


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
#pragma once

#include <type_traits>  //  std::false_type, std::true_type

namespace sqlite_orm {
    
    namespace internal {
        
        /**
         *  Result of concatenation || operator
         */
        template<class L, class R>
        struct conc_t {
            L l;
            R r;
        };
        
        /**
         *  Result of addition + operator
         */
        template<class L, class R>
        struct add_t {
            L l;
            R r;
        };
        
        /**
         *  Result of subscribe - operator
         */
        template<class L, class R>
        struct sub_t {
            L l;
            R r;
        };
        
        /**
         *  Result of multiply * operator
         */
        template<class L, class R>
        struct mul_t {
            L l;
            R r;
        };
        
        /**
         *  Result of divide / operator
         */
        template<class L, class R>
        struct div_t {
            L l;
            R r;
        };
        
        /**
         *  Result of mod % operator
         */
        template<class L, class R>
        struct mod_t {
            L l;
            R r;
        };
        
        /**
         *  Result of assign = operator
         */
        template<class L, class R>
        struct assign_t {
            L l;
            R r;
            
            assign_t(){}
            
            assign_t(L l_, R r_): l(l_), r(r_) {}
        };
        
        /**
         *  Assign operator traits. Common case
         */
        template<class T>
        struct is_assign_t : public std::false_type {};
        
        /**
         *  Assign operator traits. Specialized case
         */
        template<class L, class R>
        struct is_assign_t<assign_t<L, R>> : public std::true_type {};
        
        /**
         *  Is not an operator but a result of c(...) function. Has operator= overloaded which returns assign_t
         */
        template<class T>
        struct expression_t {
            T t;
            
            expression_t(T t_): t(t_) {}
            
            template<class R>
            assign_t<T, R> operator=(R r) const {
                return {this->t, r};
            }
        };
        
    }
    
    /**
     *  Public interface for syntax sugar for columns. Example: `where(c(&User::id) == 5)` or `storage.update(set(c(&User::name) = "Dua Lipa"));
     */
    template<class T>
    internal::expression_t<T> c(T t) {
        using result_type = internal::expression_t<T>;
        return result_type(t);
    }
    
    /**
     *  Public interface for || concatenation operator. Example: `select(conc(&User::name, "@gmail.com"));` => SELECT name + '@gmail.com' FROM users
     */
    template<class L, class R>
    internal::conc_t<L, R> conc(L l, R r) {
        return {l, r};
    }
    
    /**
     *  Public interface for + operator. Example: `select(add(&User::age, 100));` => SELECT age + 100 FROM users
     */
    template<class L, class R>
    internal::add_t<L, R> add(L l, R r) {
        return {l, r};
    }
    
    /**
     *  Public interface for - operator. Example: `select(add(&User::age, 1));` => SELECT age - 1 FROM users
     */
    template<class L, class R>
    internal::sub_t<L, R> sub(L l, R r) {
        return {l, r};
    }
    
    template<class L, class R>
    internal::mul_t<L, R> mul(L l, R r) {
        return {l, r};
    }
    
    template<class L, class R>
    internal::div_t<L, R> div(L l, R r) {
        return {l, r};
    }
    
    template<class L, class R>
    internal::mod_t<L, R> mod(L l, R r) {
        return {l, r};
    }
    
    template<class L, class R>
    internal::assign_t<L, R> assign(L l, R r) {
        return {std::move(l), std::move(r)};
    }
    
}
#pragma once

#include <tuple>    //  std::tuple
#include <string>   //  std::string
#include <memory>   //  std::shared_ptr
#include <type_traits>  //  std::true_type, std::false_type, std::is_same, std::enable_if

// #include "type_is_nullable.h"

// #include "tuple_helper.h"

// #include "default_value_extractor.h"

// #include "constraints.h"


namespace sqlite_orm {
    
    namespace internal {
        
        /**
         *  This class stores single column info. column_t is a pair of [column_name:member_pointer] mapped to a storage
         *  O is a mapped class, e.g. User
         *  T is a mapped class'es field type, e.g. &User::name
         *  Op... is a constraints pack, e.g. primary_key_t, autoincrement_t etc
         */
        template<class O, class T, class G/* = const T& (O::*)() const*/, class S/* = void (O::*)(T)*/, class ...Op>
        struct column_t {
            using object_type = O;
            using field_type = T;
            using constraints_type = std::tuple<Op...>;
            using member_pointer_t = field_type object_type::*;
            using getter_type = G;
            using setter_type = S;
            
            /**
             *  Column name. Specified during construction in `make_column`.
             */
            const std::string name;
            
            /**
             *  Member pointer used to read/write member
             */
            member_pointer_t member_pointer/* = nullptr*/;
            
            /**
             *  Getter member function pointer to get a value. If member_pointer is null than
             *  `getter` and `setter` must be not null
             */
            getter_type getter/* = nullptr*/;
            
            /**
             *  Setter member function
             */
            setter_type setter/* = nullptr*/;
            
            /**
             *  Constraints tuple
             */
            constraints_type constraints;
            
            /**
             *  Simplified interface for `NOT NULL` constraint
             */
            bool not_null() const {
                return !type_is_nullable<field_type>::value;
            }
            
            template<class Opt>
            constexpr bool has() const {
                return tuple_helper::tuple_contains_type<Opt, constraints_type>::value;
            }
            
            template<class O1, class O2, class ...Opts>
            constexpr bool has_every() const  {
                if(has<O1>() && has<O2>()) {
                    return true;
                }else{
                    return has_every<Opts...>();
                }
            }
            
            template<class O1>
            constexpr bool has_every() const {
                return has<O1>();
            }
            
            /**
             *  Simplified interface for `DEFAULT` constraint
             *  @return string representation of default value if it exists otherwise nullptr
             */
            std::shared_ptr<std::string> default_value() {
                std::shared_ptr<std::string> res;
                tuple_helper::iterator<std::tuple_size<constraints_type>::value - 1, Op...>()(constraints, [&res](auto &v){
                    auto dft = internal::default_value_extractor()(v);
                    if(dft){
                        res = dft;
                    }
                });
                return res;
            }
        };
        
        /**
         *  Column traits. Common case.
         */
        template<class T>
        struct is_column : public std::false_type {};
        
        /**
         *  Column traits. Specialized case case.
         */
        template<class O, class T, class ...Op>
        struct is_column<column_t<O, T, Op...>> : public std::true_type {};
        
        template<class O, class T>
        using getter_by_value_const = T (O::*)() const;
        
        template<class O, class T>
        using getter_by_value = T (O::*)();
        
        template<class O, class T>
        using getter_by_ref_const = T& (O::*)() const;
        
        template<class O, class T>
        using getter_by_ref = T& (O::*)();
        
        template<class O, class T>
        using getter_by_const_ref_const = const T& (O::*)() const;
        
        template<class O, class T>
        using getter_by_const_ref = const T& (O::*)();
        
        template<class O, class T>
        using setter_by_value = void (O::*)(T);
        
        template<class O, class T>
        using setter_by_ref = void (O::*)(T&);
        
        template<class O, class T>
        using setter_by_const_ref = void (O::*)(const T&);
        
        template<class T>
        struct is_getter : std::false_type {};
        
        template<class O, class T>
        struct is_getter<getter_by_value_const<O, T>> : std::true_type {};
        
        template<class O, class T>
        struct is_getter<getter_by_value<O, T>> : std::true_type {};
        
        template<class O, class T>
        struct is_getter<getter_by_ref_const<O, T>> : std::true_type {};
        
        template<class O, class T>
        struct is_getter<getter_by_ref<O, T>> : std::true_type {};
        
        template<class O, class T>
        struct is_getter<getter_by_const_ref_const<O, T>> : std::true_type {};
        
        template<class O, class T>
        struct is_getter<getter_by_const_ref<O, T>> : std::true_type {};
        
        template<class T>
        struct is_setter : std::false_type {};
        
        template<class O, class T>
        struct is_setter<setter_by_value<O, T>> : std::true_type {};
        
        template<class O, class T>
        struct is_setter<setter_by_ref<O, T>> : std::true_type {};
        
        template<class O, class T>
        struct is_setter<setter_by_const_ref<O, T>> : std::true_type {};
        
        template<class T>
        struct getter_traits;
        
        template<class O, class T>
        struct getter_traits<getter_by_value_const<O, T>> {
            using object_type = O;
            using field_type = T;
        };
        
        template<class O, class T>
        struct getter_traits<getter_by_value<O, T>> {
            using object_type = O;
            using field_type = T;
        };
        
        template<class O, class T>
        struct getter_traits<getter_by_ref_const<O, T>> {
            using object_type = O;
            using field_type = T;
        };
        
        template<class O, class T>
        struct getter_traits<getter_by_ref<O, T>> {
            using object_type = O;
            using field_type = T;
        };
        
        template<class O, class T>
        struct getter_traits<getter_by_const_ref_const<O, T>> {
            using object_type = O;
            using field_type = T;
        };
        
        template<class O, class T>
        struct getter_traits<getter_by_const_ref<O, T>> {
            using object_type = O;
            using field_type = T;
        };
        
        template<class T>
        struct setter_traits;
        
        template<class O, class T>
        struct setter_traits<setter_by_value<O, T>> {
            using object_type = O;
            using field_type = T;
        };
        
        template<class O, class T>
        struct setter_traits<setter_by_ref<O, T>> {
            using object_type = O;
            using field_type = T;
        };
        
        template<class O, class T>
        struct setter_traits<setter_by_const_ref<O, T>> {
            using object_type = O;
            using field_type = T;
        };
    }
    
    /**
     *  Column builder function. You should use it to create columns instead of constructor
     */
    template<class O, class T,
    typename = typename std::enable_if<!std::is_member_function_pointer<T O::*>::value>::type,
    class ...Op>
    internal::column_t<O, T, const T& (O::*)() const, void (O::*)(T), Op...> make_column(const std::string &name, T O::*m, Op ...constraints){
        static_assert(constraints::constraints_size<Op...>::value == std::tuple_size<std::tuple<Op...>>::value, "Incorrect constraints pack");
        return {name, m, nullptr, nullptr, std::make_tuple(constraints...)};
    }
    
    /**
     *  Column builder function with setter and getter. You should use it to create columns instead of constructor
     */
    template<class G, class S,
    typename = typename std::enable_if<internal::is_getter<G>::value>::type,
    typename = typename std::enable_if<internal::is_setter<S>::value>::type,
    class ...Op>
    internal::column_t<
    typename internal::setter_traits<S>::object_type,
    typename internal::setter_traits<S>::field_type,
    G, S, Op...> make_column(const std::string &name,
                             S setter,
                             G getter,
                             Op ...constraints)
    {
        static_assert(std::is_same<typename internal::setter_traits<S>::field_type, typename internal::getter_traits<G>::field_type>::value,
                      "Getter and setter must get and set same data type");
        static_assert(constraints::constraints_size<Op...>::value == std::tuple_size<std::tuple<Op...>>::value, "Incorrect constraints pack");
        return {name, nullptr, getter, setter, std::make_tuple(constraints...)};
    }
    
    /**
     *  Column builder function with getter and setter (reverse order). You should use it to create columns instead of constructor
     */
    template<class G, class S,
    typename = typename std::enable_if<internal::is_getter<G>::value>::type,
    typename = typename std::enable_if<internal::is_setter<S>::value>::type,
    class ...Op>
    internal::column_t<
    typename internal::setter_traits<S>::object_type,
    typename internal::setter_traits<S>::field_type,
    G, S, Op...> make_column(const std::string &name,
                             G getter,
                             S setter,
                             Op ...constraints)
    {
        static_assert(std::is_same<typename internal::setter_traits<S>::field_type, typename internal::getter_traits<G>::field_type>::value,
                      "Getter and setter must get and set same data type");
        static_assert(constraints::constraints_size<Op...>::value == std::tuple_size<std::tuple<Op...>>::value, "Incorrect constraints pack");
        return {name, nullptr, getter, setter, std::make_tuple(constraints...)};
    }
    
}
#pragma once

#include <string>   //  std::string
#include <sstream>  //  std::stringstream
#include <vector>   //  std::vector
#include <cstddef>  //  std::nullptr_t
#include <memory>   //  std::shared_ptr, std::unique_ptr

namespace sqlite_orm {
    
    /**
     *  Is used to print members mapped to objects in storage_t::dump member function.
     *  Other developers can create own specialization to map custom types
     */
    template<class T>
    struct field_printer {
        std::string operator()(const T &t) const {
            std::stringstream stream;
            stream << t;
            return stream.str();
        }
    };
    
    /**
     *  Upgrade to integer is required when using unsigned char(uint8_t)
     */
    template<>
    struct field_printer<unsigned char> {
        std::string operator()(const unsigned char &t) const {
            std::stringstream stream;
            stream << +t;
            return stream.str();
        }
    };
    
    /**
     *  Upgrade to integer is required when using signed char(int8_t)
     */
    template<>
    struct field_printer<signed char> {
        std::string operator()(const signed char &t) const {
            std::stringstream stream;
            stream << +t;
            return stream.str();
        }
    };
    
    /**
     *  char is neigher signer char nor unsigned char so it has its own specialization
     */
    template<>
    struct field_printer<char> {
        std::string operator()(const char &t) const {
            std::stringstream stream;
            stream << +t;
            return stream.str();
        }
    };
    
    template<>
    struct field_printer<std::string> {
        std::string operator()(const std::string &t) const {
            return t;
        }
    };
    
    template<>
    struct field_printer<std::vector<char>> {
        std::string operator()(const std::vector<char> &t) const {
            std::stringstream ss;
            ss << std::hex;
            for(auto c : t) {
                ss << c;
            }
            return ss.str();
        }
    };
    
    template<>
    struct field_printer<std::nullptr_t> {
        std::string operator()(const std::nullptr_t &) const {
            return "null";
        }
    };
    
    template<class T>
    struct field_printer<std::shared_ptr<T>> {
        std::string operator()(const std::shared_ptr<T> &t) const {
            if(t){
                return field_printer<T>()(*t);
            }else{
                return field_printer<std::nullptr_t>()(nullptr);
            }
        }
    };
    
    template<class T>
    struct field_printer<std::unique_ptr<T>> {
        std::string operator()(const std::unique_ptr<T> &t) const {
            if(t){
                return field_printer<T>()(*t);
            }else{
                return field_printer<std::nullptr_t>()(nullptr);
            }
        }
    };
}
#pragma once

#include <string>   //  std::string

// #include "collate_argument.h"

// #include "constraints.h"


namespace sqlite_orm {
    
    namespace conditions {
        
        /**
         *  Stores LIMIT/OFFSET info
         */
        struct limit_t {
            int lim = 0;
            bool has_offset = false;
            bool offset_is_implicit = false;
            int off = 0;
            
            limit_t(){}
            
            limit_t(decltype(lim) lim_): lim(lim_) {}
            
            limit_t(decltype(lim) lim_,
                    decltype(has_offset) has_offset_,
                    decltype(offset_is_implicit) offset_is_implicit_,
                    decltype(off) off_):
            lim(lim_),
            has_offset(has_offset_),
            offset_is_implicit(offset_is_implicit_),
            off(off_){}
            
            operator std::string () const {
                return "LIMIT";
            }
        };
        
        /**
         *  Stores OFFSET only info
         */
        struct offset_t {
            int off;
        };
        
        /**
         *  Inherit from this class if target class can be chained with other conditions with '&&' and '||' operators
         */
        struct condition_t {};
        
        /**
         *  Collated something
         */
        template<class T>
        struct collate_t : public condition_t {
            T expr;
            internal::collate_argument argument;
            
            collate_t(T expr_, internal::collate_argument argument_): expr(expr_), argument(argument_) {}
            
            operator std::string () const {
                return constraints::collate_t{this->argument};
            }
        };
        
        /**
         *  Collated something with custom collate function
         */
        template<class T>
        struct named_collate {
            T expr;
            std::string name;
            
            named_collate() = default;
            
            named_collate(T expr_, std::string name_): expr(expr_), name(std::move(name_)) {}
            
            operator std::string () const {
                return "COLLATE " + this->name;
            }
        };
        
        /**
         *  Result of not operator
         */
        template<class C>
        struct negated_condition_t : public condition_t {
            C c;
            
            negated_condition_t(){}
            
            negated_condition_t(C c_): c(c_) {}
            
            operator std::string () const {
                return "NOT";
            }
        };
        
        /**
         *  Result of and operator
         */
        template<class L, class R>
        struct and_condition_t : public condition_t {
            L l;
            R r;
            
            and_condition_t(){}
            
            and_condition_t(L l_, R r_): l(l_), r(r_) {}
            
            operator std::string () const {
                return "AND";
            }
        };
        
        /**
         *  Result of or operator
         */
        template<class L, class R>
        struct or_condition_t : public condition_t {
            L l;
            R r;
            
            or_condition_t(){}
            
            or_condition_t(L l_, R r_): l(l_), r(r_) {}
            
            operator std::string () const {
                return "OR";
            }
        };
        
        /**
         *  Base class for binary conditions
         */
        template<class L, class R>
        struct binary_condition : public condition_t {
            L l;
            R r;
            
            binary_condition(){}
            
            binary_condition(L l_, R r_): l(l_), r(r_) {}
        };
        
        /**
         *  = and == operators object
         */
        template<class L, class R>
        struct is_equal_t : public binary_condition<L, R> {
            using self = is_equal_t<L, R>;
            
            using binary_condition<L, R>::binary_condition;
            
            operator std::string () const {
                return "=";
            }
            
            negated_condition_t<self> operator!() const {
                return {*this};
            }
            
            collate_t<self> collate_binary() const {
                return {*this, internal::collate_argument::binary};
            }
            
            collate_t<self> collate_nocase() const {
                return {*this, internal::collate_argument::nocase};
            }
            
            collate_t<self> collate_rtrim() const {
                return {*this, internal::collate_argument::rtrim};
            }
            
            named_collate<self> collate(std::string name) const {
                return {*this, std::move(name)};
            }
            
        };
        
        /**
         *  != operator object
         */
        template<class L, class R>
        struct is_not_equal_t : public binary_condition<L, R> {
            using self = is_not_equal_t<L, R>;
            
            using binary_condition<L, R>::binary_condition;
            
            operator std::string () const {
                return "!=";
            }
            
            negated_condition_t<self> operator!() const {
                return {*this};
            }
            
            collate_t<self> collate_binary() const {
                return {*this, internal::collate_argument::binary};
            }
            
            collate_t<self> collate_nocase() const {
                return {*this, internal::collate_argument::nocase};
            }
            
            collate_t<self> collate_rtrim() const {
                return {*this, internal::collate_argument::rtrim};
            }
        };
        
        /**
         *  > operator object.
         */
        template<class L, class R>
        struct greater_than_t : public binary_condition<L, R> {
            using self = greater_than_t<L, R>;
            
            using binary_condition<L, R>::binary_condition;
            
            operator std::string () const {
                return ">";
            }
            
            negated_condition_t<self> operator!() const {
                return {*this};
            }
            
            collate_t<self> collate_binary() const {
                return {*this, internal::collate_argument::binary};
            }
            
            collate_t<self> collate_nocase() const {
                return {*this, internal::collate_argument::nocase};
            }
            
            collate_t<self> collate_rtrim() const {
                return {*this, internal::collate_argument::rtrim};
            }
        };
        
        /**
         *  >= operator object.
         */
        template<class L, class R>
        struct greater_or_equal_t : public binary_condition<L, R> {
            using self = greater_or_equal_t<L, R>;
            
            using binary_condition<L, R>::binary_condition;
            
            operator std::string () const {
                return ">=";
            }
            
            negated_condition_t<self> operator!() const {
                return {*this};
            }
            
            collate_t<self> collate_binary() const {
                return {*this, internal::collate_argument::binary};
            }
            
            collate_t<self> collate_nocase() const {
                return {*this, internal::collate_argument::nocase};
            }
            
            collate_t<self> collate_rtrim() const {
                return {*this, internal::collate_argument::rtrim};
            }
        };
        
        /**
         *  < operator object.
         */
        template<class L, class R>
        struct lesser_than_t : public binary_condition<L, R> {
            using self = lesser_than_t<L, R>;
            
            using binary_condition<L, R>::binary_condition;
            
            operator std::string () const {
                return "<";
            }
            
            negated_condition_t<self> operator!() const {
                return {*this};
            }
            
            collate_t<self> collate_binary() const {
                return {*this, internal::collate_argument::binary};
            }
            
            collate_t<self> collate_nocase() const {
                return {*this, internal::collate_argument::nocase};
            }
            
            collate_t<self> collate_rtrim() const {
                return {*this, internal::collate_argument::rtrim};
            }
        };
        
        /**
         *  <= operator object.
         */
        template<class L, class R>
        struct lesser_or_equal_t : public binary_condition<L, R> {
            using self = lesser_or_equal_t<L, R>;
            
            using binary_condition<L, R>::binary_condition;
            
            operator std::string () const {
                return "<=";
            }
            
            negated_condition_t<lesser_or_equal_t<L, R>> operator!() const {
                return {*this};
            }
            
            collate_t<self> collate_binary() const {
                return {*this, internal::collate_argument::binary};
            }
            
            collate_t<self> collate_nocase() const {
                return {*this, internal::collate_argument::nocase};
            }
            
            collate_t<self> collate_rtrim() const {
                return {*this, internal::collate_argument::rtrim};
            }
        };
        
        template<class L, class E>
        struct in_t : public condition_t {
            using self = in_t<L, E>;
            
            L l;    //  left expression..
            std::vector<E> values;       //  values..
            
            in_t(L l_, std::vector<E> values_): l(l_), values(std::move(values_)) {}
            
            negated_condition_t<self> operator!() const {
                return {*this};
            }
            
            operator std::string () const {
                return "IN";
            }
        };
        
        template<class T>
        struct is_null_t {
            using self = is_null_t<T>;
            T t;
            
            negated_condition_t<self> operator!() const {
                return {*this};
            }
            
            operator std::string () const {
                return "IS NULL";
            }
        };
        
        template<class T>
        struct is_not_null_t {
            using self = is_not_null_t<T>;
            
            T t;
            
            negated_condition_t<self> operator!() const {
                return {*this};
            }
            
            operator std::string () const {
                return "IS NOT NULL";
            }
        };
        
        template<class C>
        struct where_t {
            C c;
            
            operator std::string () const {
                return "WHERE";
            }
        };
        
        template<class O>
        struct order_by_t {
            using self = order_by_t<O>;
            
            O o;
            int asc_desc = 0;   //  1: asc, -1: desc
            std::string _collate_argument;
            
            order_by_t(): o() {}
            
            order_by_t(O o_): o(o_) {}
            
            operator std::string() const {
                return "ORDER BY";
            }
            
            self asc() {
                auto res = *this;
                res.asc_desc = 1;
                return res;
            }
            
            self desc() {
                auto res = *this;
                res.asc_desc = -1;
                return res;
            }
            
            self collate_binary() const {
                auto res = *this;
                res._collate_argument = constraints::collate_t::string_from_collate_argument(internal::collate_argument::binary);
                return res;
            }
            
            self collate_nocase() const {
                auto res = *this;
                res._collate_argument = constraints::collate_t::string_from_collate_argument(internal::collate_argument::nocase);
                return res;
            }
            
            self collate_rtrim() const {
                auto res = *this;
                res._collate_argument = constraints::collate_t::string_from_collate_argument(internal::collate_argument::rtrim);
                return res;
            }
            
            self collate(std::string name) const {
                auto res = *this;
                res._collate_argument = std::move(name);
                return res;
            }
        };
        
        template<class ...Args>
        struct multi_order_by_t {
            std::tuple<Args...> args;
            
            operator std::string() const {
                return static_cast<std::string>(order_by_t<void*>());
            }
        };
        
        template<class ...Args>
        struct group_by_t {
            std::tuple<Args...> args;
            
            operator std::string() const {
                return "GROUP BY";
            }
        };
        
        template<class A, class T>
        struct between_t : public condition_t {
            A expr;
            T b1;
            T b2;
            
            between_t(A expr_, T b1_, T b2_): expr(expr_), b1(b1_), b2(b2_) {}
            
            operator std::string() const {
                return "BETWEEN";
            }
        };
        
        template<class A, class T>
        struct like_t : public condition_t {
            A a;
            T t;
            
            like_t(){}
            
            like_t(A a_, T t_): a(a_), t(t_) {}
            
            operator std::string() const {
                return "LIKE";
            }
        };
        
        template<class T>
        struct cross_join_t {
            using type = T;
            
            operator std::string() const {
                return "CROSS JOIN";
            }
        };
        
        template<class T>
        struct natural_join_t {
            using type = T;
            
            operator std::string() const {
                return "NATURAL JOIN";
            }
        };
        
        template<class T, class O>
        struct left_join_t {
            using type = T;
            using on_type = O;
            
            on_type constraint;
            
            operator std::string() const {
                return "LEFT JOIN";
            }
        };
        
        template<class T, class O>
        struct join_t {
            using type = T;
            using on_type = O;
            
            on_type constraint;
            
            operator std::string() const {
                return "JOIN";
            }
        };
        
        template<class T, class O>
        struct left_outer_join_t {
            using type = T;
            using on_type = O;
            
            on_type constraint;
            
            operator std::string() const {
                return "LEFT OUTER JOIN";
            }
        };
        
        template<class T>
        struct on_t {
            T t;
            
            operator std::string() const {
                return "ON";
            }
        };
        
        template<class F, class O>
        struct using_t {
            F O::*column;
            
            operator std::string() const {
                return "USING";
            }
        };
        
        template<class T, class O>
        struct inner_join_t {
            using type = T;
            using on_type = O;
            
            on_type constraint;
            
            operator std::string() const {
                return "INNER JOIN";
            }
        };
        
    }
    
    /**
     *  Cute operators for columns
     */
    
    template<class T, class R>
    conditions::lesser_than_t<T, R> operator<(internal::expression_t<T> expr, R r) {
        return {expr.t, r};
    }
    
    template<class L, class T>
    conditions::lesser_than_t<L, T> operator<(L l, internal::expression_t<T> expr) {
        return {l, expr.t};
    }
    
    template<class T, class R>
    conditions::lesser_or_equal_t<T, R> operator<=(internal::expression_t<T> expr, R r) {
        return {expr.t, r};
    }
    
    template<class L, class T>
    conditions::lesser_or_equal_t<L, T> operator<=(L l, internal::expression_t<T> expr) {
        return {l, expr.t};
    }
    
    template<class T, class R>
    conditions::greater_than_t<T, R> operator>(internal::expression_t<T> expr, R r) {
        return {expr.t, r};
    }
    
    template<class L, class T>
    conditions::greater_than_t<L, T> operator>(L l, internal::expression_t<T> expr) {
        return {l, expr.t};
    }
    
    template<class T, class R>
    conditions::greater_or_equal_t<T, R> operator>=(internal::expression_t<T> expr, R r) {
        return {expr.t, r};
    }
    
    template<class L, class T>
    conditions::greater_or_equal_t<L, T> operator>=(L l, internal::expression_t<T> expr) {
        return {l, expr.t};
    }
    
    template<class T, class R>
    conditions::is_equal_t<T, R> operator==(internal::expression_t<T> expr, R r) {
        return {expr.t, r};
    }
    
    template<class L, class T>
    conditions::is_equal_t<L, T> operator==(L l, internal::expression_t<T> expr) {
        return {l, expr.t};
    }
    
    template<class T, class R>
    conditions::is_not_equal_t<T, R> operator!=(internal::expression_t<T> expr, R r) {
        return {expr.t, r};
    }
    
    template<class L, class T>
    conditions::is_not_equal_t<L, T> operator!=(L l, internal::expression_t<T> expr) {
        return {l, expr.t};
    }
    
    template<class T, class R>
    internal::conc_t<T, R> operator||(internal::expression_t<T> expr, R r) {
        return {expr.t, r};
    }
    
    template<class L, class T>
    internal::conc_t<L, T> operator||(L l, internal::expression_t<T> expr) {
        return {l, expr.t};
    }
    
    template<class L, class R>
    internal::conc_t<L, R> operator||(internal::expression_t<L> l, internal::expression_t<R> r) {
        return {l.t, r.t};
    }
    
    template<class T, class R>
    internal::add_t<T, R> operator+(internal::expression_t<T> expr, R r) {
        return {expr.t, r};
    }
    
    template<class L, class T>
    internal::add_t<L, T> operator+(L l, internal::expression_t<T> expr) {
        return {l, expr.t};
    }
    
    template<class L, class R>
    internal::add_t<L, R> operator+(internal::expression_t<L> l, internal::expression_t<R> r) {
        return {l.t, r.t};
    }
    
    template<class T, class R>
    internal::sub_t<T, R> operator-(internal::expression_t<T> expr, R r) {
        return {expr.t, r};
    }
    
    template<class L, class T>
    internal::sub_t<L, T> operator-(L l, internal::expression_t<T> expr) {
        return {l, expr.t};
    }
    
    template<class L, class R>
    internal::sub_t<L, R> operator-(internal::expression_t<L> l, internal::expression_t<R> r) {
        return {l.t, r.t};
    }
    
    template<class T, class R>
    internal::mul_t<T, R> operator*(internal::expression_t<T> expr, R r) {
        return {expr.t, r};
    }
    
    template<class L, class T>
    internal::mul_t<L, T> operator*(L l, internal::expression_t<T> expr) {
        return {l, expr.t};
    }
    
    template<class L, class R>
    internal::mul_t<L, R> operator*(internal::expression_t<L> l, internal::expression_t<R> r) {
        return {l.t, r.t};
    }
    
    template<class T, class R>
    internal::div_t<T, R> operator/(internal::expression_t<T> expr, R r) {
        return {expr.t, r};
    }
    
    template<class L, class T>
    internal::div_t<L, T> operator/(L l, internal::expression_t<T> expr) {
        return {l, expr.t};
    }
    
    template<class L, class R>
    internal::div_t<L, R> operator/(internal::expression_t<L> l, internal::expression_t<R> r) {
        return {l.t, r.t};
    }
    
    template<class T, class R>
    internal::mod_t<T, R> operator%(internal::expression_t<T> expr, R r) {
        return {expr.t, r};
    }
    
    template<class L, class T>
    internal::mod_t<L, T> operator%(L l, internal::expression_t<T> expr) {
        return {l, expr.t};
    }
    
    template<class L, class R>
    internal::mod_t<L, R> operator%(internal::expression_t<L> l, internal::expression_t<R> r) {
        return {l.t, r.t};
    }
    
    template<class F, class O>
    conditions::using_t<F, O> using_(F O::*p) {
        return {p};
    }
    
    template<class T>
    conditions::on_t<T> on(T t) {
        return {t};
    }
    
    template<class T>
    conditions::cross_join_t<T> cross_join() {
        return {};
    }
    
    template<class T>
    conditions::natural_join_t<T> natural_join() {
        return {};
    }
    
    template<class T, class O>
    conditions::left_join_t<T, O> left_join(O o) {
        return {o};
    }
    
    template<class T, class O>
    conditions::join_t<T, O> join(O o) {
        return {o};
    }
    
    template<class T, class O>
    conditions::left_outer_join_t<T, O> left_outer_join(O o) {
        return {o};
    }
    
    template<class T, class O>
    conditions::inner_join_t<T, O> inner_join(O o) {
        return {o};
    }
    
    inline conditions::offset_t offset(int off) {
        return {off};
    }
    
    inline conditions::limit_t limit(int lim) {
        return {lim};
    }
    
    inline conditions::limit_t limit(int off, int lim) {
        return {lim, true, true, off};
    }
    
    inline conditions::limit_t limit(int lim, conditions::offset_t offt) {
        return {lim, true, false, offt.off };
    }
    
    template<
    class L,
    class R,
    typename = typename std::enable_if<std::is_base_of<conditions::condition_t, L>::value && std::is_base_of<conditions::condition_t, R>::value>::type
    >
    conditions::and_condition_t<L, R> operator &&(const L &l, const R &r) {
        return {l, r};
    }
    
    template<
    class L,
    class R,
    typename = typename std::enable_if<std::is_base_of<conditions::condition_t, L>::value && std::is_base_of<conditions::condition_t, R>::value>::type
    >
    conditions::or_condition_t<L, R> operator ||(const L &l, const R &r) {
        return {l, r};
    }
    
    template<class T>
    conditions::is_not_null_t<T> is_not_null(T t) {
        return {t};
    }
    
    template<class T>
    conditions::is_null_t<T> is_null(T t) {
        return {t};
    }
    
    template<class L, class E>
    conditions::in_t<L, E> in(L l, std::vector<E> values) {
        return {std::move(l), std::move(values)};
    }
    
    template<class L, class E>
    conditions::in_t<L, E> in(L l, std::initializer_list<E> values) {
        return {std::move(l), std::move(values)};
    }
    
    template<class L, class R>
    conditions::is_equal_t<L, R> is_equal(L l, R r) {
        return {l, r};
    }
    
    template<class L, class R>
    conditions::is_equal_t<L, R> eq(L l, R r) {
        return {l, r};
    }
    
    template<class L, class R>
    conditions::is_not_equal_t<L, R> is_not_equal(L l, R r) {
        return {l, r};
    }
    
    template<class L, class R>
    conditions::is_not_equal_t<L, R> ne(L l, R r) {
        return {l, r};
    }
    
    template<class L, class R>
    conditions::greater_than_t<L, R> greater_than(L l, R r) {
        return {l, r};
    }
    
    template<class L, class R>
    conditions::greater_than_t<L, R> gt(L l, R r) {
        return {l, r};
    }
    
    template<class L, class R>
    conditions::greater_or_equal_t<L, R> greater_or_equal(L l, R r) {
        return {l, r};
    }
    
    template<class L, class R>
    conditions::greater_or_equal_t<L, R> ge(L l, R r) {
        return {l, r};
    }
    
    template<class L, class R>
    conditions::lesser_than_t<L, R> lesser_than(L l, R r) {
        return {l, r};
    }
    
    template<class L, class R>
    conditions::lesser_than_t<L, R> lt(L l, R r) {
        return {l, r};
    }
    
    template<class L, class R>
    conditions::lesser_or_equal_t<L, R> lesser_or_equal(L l, R r) {
        return {l, r};
    }
    
    template<class L, class R>
    conditions::lesser_or_equal_t<L, R> le(L l, R r) {
        return {l, r};
    }
    
    template<class C>
    conditions::where_t<C> where(C c) {
        return {c};
    }
    
    template<class O>
    conditions::order_by_t<O> order_by(O o) {
        return {o};
    }
    
    template<class ...Args>
    conditions::multi_order_by_t<Args...> multi_order_by(Args&& ...args) {
        return {std::make_tuple(std::forward<Args>(args)...)};
    }
    
    template<class ...Args>
    conditions::group_by_t<Args...> group_by(Args&& ...args) {
        return {std::make_tuple(std::forward<Args>(args)...)};
    }
    
    template<class A, class T>
    conditions::between_t<A, T> between(A expr, T b1, T b2) {
        return {expr, b1, b2};
    }
    
    template<class A, class T>
    conditions::like_t<A, T> like(A a, T t) {
        return {a, t};
    }
}
#pragma once

#include <type_traits>  //  std::enable_if, std::is_base_of
#include <sstream>  //  std::stringstream

namespace sqlite_orm {
    
    struct alias_tag {};
    
    template<class T, char A>
    struct alias : alias_tag {
        using type = T;
        
        static char get() {
            return A;
        }
    };
    
    namespace internal {
        
        template<class T, class C>
        struct alias_column_t {
            using alias_type = T;
            using column_type = C;
            
            column_type column;
            
            alias_column_t() {};
            
            alias_column_t(column_type column_): column(column_) {}
        };
        
        template<class T, class SFINAE = void>
        struct alias_exractor;
        
        template<class T>
        struct alias_exractor<T, typename std::enable_if<std::is_base_of<alias_tag, T>::value>::type> {
            static std::string get() {
                std::stringstream ss;
                ss << T::get();
                return ss.str();
            }
        };
        
        template<class T>
        struct alias_exractor<T, typename std::enable_if<!std::is_base_of<alias_tag, T>::value>::type> {
            static std::string get() {
                return {};
            }
        };
    }
    
    template<class T, class C>
    internal::alias_column_t<T, C> alias_column(C c) {
        return {c};
    }
    
    template<class T> using alias_a = alias<T, 'a'>;
    template<class T> using alias_b = alias<T, 'b'>;
    template<class T> using alias_c = alias<T, 'c'>;
    template<class T> using alias_d = alias<T, 'd'>;
    template<class T> using alias_e = alias<T, 'e'>;
    template<class T> using alias_f = alias<T, 'f'>;
    template<class T> using alias_g = alias<T, 'g'>;
    template<class T> using alias_h = alias<T, 'h'>;
    template<class T> using alias_i = alias<T, 'i'>;
    template<class T> using alias_j = alias<T, 'j'>;
    template<class T> using alias_k = alias<T, 'k'>;
    template<class T> using alias_l = alias<T, 'l'>;
    template<class T> using alias_m = alias<T, 'm'>;
    template<class T> using alias_n = alias<T, 'n'>;
    template<class T> using alias_o = alias<T, 'o'>;
    template<class T> using alias_p = alias<T, 'p'>;
    template<class T> using alias_q = alias<T, 'q'>;
    template<class T> using alias_r = alias<T, 'r'>;
    template<class T> using alias_s = alias<T, 's'>;
    template<class T> using alias_t = alias<T, 't'>;
    template<class T> using alias_u = alias<T, 'u'>;
    template<class T> using alias_v = alias<T, 'v'>;
    template<class T> using alias_w = alias<T, 'w'>;
    template<class T> using alias_x = alias<T, 'x'>;
    template<class T> using alias_y = alias<T, 'y'>;
    template<class T> using alias_z = alias<T, 'z'>;
}
#pragma once

// #include "conditions.h"


namespace sqlite_orm {
    
    namespace internal {
        
        template<class ...Args>
        struct join_iterator {
            
            template<class L>
            void operator()(L) {
                //..
            }
        };
        
        template<>
        struct join_iterator<> {
            
            template<class L>
            void operator()(L) {
                //..
            }
        };
        
        template<class H, class ...Tail>
        struct join_iterator<H, Tail...> : public join_iterator<Tail...>{
            using super = join_iterator<Tail...>;
            
            H h;
            
            template<class L>
            void operator()(L l) {
                this->super::operator()(l);
            }
            
        };
        
        template<class T, class ...Tail>
        struct join_iterator<conditions::cross_join_t<T>, Tail...> : public join_iterator<Tail...>{
            using super = join_iterator<Tail...>;
            
            conditions::cross_join_t<T> h;
            
            template<class L>
            void operator()(L l) {
                l(h);
                this->super::operator()(l);
            }
        };
        
        template<class T, class ...Tail>
        struct join_iterator<conditions::natural_join_t<T>, Tail...> : public join_iterator<Tail...>{
            using super = join_iterator<Tail...>;
            
            conditions::natural_join_t<T> h;
            
            template<class L>
            void operator()(L l) {
                l(h);
                this->super::operator()(l);
            }
        };
        
        template<class T, class O, class ...Tail>
        struct join_iterator<conditions::left_join_t<T, O>, Tail...> : public join_iterator<Tail...> {
            using super = join_iterator<Tail...>;
            
            conditions::left_join_t<T, O> h;
            
            template<class L>
            void operator()(L l) {
                l(h);
                this->super::operator()(l);
            }
        };
        
        template<class T, class O, class ...Tail>
        struct join_iterator<conditions::join_t<T, O>, Tail...> : public join_iterator<Tail...> {
            using super = join_iterator<Tail...>;
            
            conditions::join_t<T, O> h;
            
            template<class L>
            void operator()(L l) {
                l(h);
                this->super::operator()(l);
            }
        };
        
        template<class T, class O, class ...Tail>
        struct join_iterator<conditions::left_outer_join_t<T, O>, Tail...> : public join_iterator<Tail...> {
            using super = join_iterator<Tail...>;
            
            conditions::left_outer_join_t<T, O> h;
            
            template<class L>
            void operator()(L l) {
                l(h);
                this->super::operator()(l);
            }
        };
        
        template<class T, class O, class ...Tail>
        struct join_iterator<conditions::inner_join_t<T, O>, Tail...> : public join_iterator<Tail...> {
            using super = join_iterator<Tail...>;
            
            conditions::inner_join_t<T, O> h;
            
            template<class L>
            void operator()(L l) {
                l(h);
                this->super::operator()(l);
            }
        };
    }
}
#pragma once

#include <string>   //  std::string
#include <tuple>    //  std::make_tuple
#include <type_traits>  //  std::forward, std::is_base_of, std::enable_if

// #include "conditions.h"


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
        
        struct random_t : public core_function_t {
            
            operator std::string() const {
                return "RANDOM";
            }
        };
        
#endif
        template<class T, class ...Args>
        struct date_t : public core_function_t {
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
        struct datetime_t : public core_function_t {
            using modifiers_type = std::tuple<Args...>;
            
            T timestring;
            modifiers_type modifiers;
            
            datetime_t() = default;
            
            datetime_t(T timestring_, modifiers_type modifiers_): timestring(timestring_), modifiers(modifiers_) {}
            
            operator std::string() const {
                return "DATETIME";
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
        return Res(timestring, std::make_tuple(modifiers...));
    }
    
    template<class T, class ...Args, class Res = core_functions::datetime_t<T, Args...>>
    Res datetime(T timestring, Args ...modifiers) {
        return Res(timestring, std::make_tuple(modifiers...));
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
}
#pragma once

namespace sqlite_orm {
    
    namespace aggregate_functions {
        
        template<class T>
        struct avg_t {
            T t;
            
            operator std::string() const {
                return "AVG";
            }
        };
        
        template<class T>
        struct count_t {
            T t;
            
            operator std::string() const {
                return "COUNT";
            }
        };
        
        struct count_asterisk_t {
            
            operator std::string() const {
                return "COUNT";
            }
            
        };
        
        template<class T>
        struct sum_t {
            T t;
            
            operator std::string() const {
                return "SUM";
            }
        };
        
        template<class T>
        struct total_t {
            T t;
            
            operator std::string() const {
                return "TOTAL";
            }
        };
        
        template<class T>
        struct max_t {
            T t;
            
            operator std::string() const {
                return "MAX";
            }
        };
        
        template<class T>
        struct min_t {
            T t;
            
            operator std::string() const {
                return "MIN";
            }
        };
        
        template<class T>
        struct group_concat_single_t {
            T t;
            
            operator std::string() const {
                return "GROUP_CONCAT";
            }
        };
        
        template<class T>
        struct group_concat_double_t {
            T t;
            std::string y;
            
            operator std::string() const {
                return "GROUP_CONCAT";
            }
        };
        
    }
    
    template<class T>
    aggregate_functions::avg_t<T> avg(T t) {
        return {t};
    }
    
    template<class T>
    aggregate_functions::count_t<T> count(T t) {
        return {t};
    }
    
    inline aggregate_functions::count_asterisk_t count() {
        return {};
    }
    
    template<class T>
    aggregate_functions::sum_t<T> sum(T t) {
        return {t};
    }
    
    template<class T>
    aggregate_functions::max_t<T> max(T t) {
        return {t};
    }
    
    template<class T>
    aggregate_functions::min_t<T> min(T t) {
        return {t};
    }
    
    template<class T>
    aggregate_functions::total_t<T> total(T t) {
        return {t};
    }
    
    template<class T>
    aggregate_functions::group_concat_single_t<T> group_concat(T t) {
        return {t};
    }
    
    template<class T, class Y>
    aggregate_functions::group_concat_double_t<T> group_concat(T t, Y y) {
        return {t, y};
    }
}
#pragma once

namespace sqlite_orm {
    
    namespace internal {
        
        /**
         *  Cute class used to compare setters/getters and member pointers with each other.
         */
        template<class L, class R>
        struct typed_comparator {
            bool operator()(const L &, const R &) const {
                return false;
            }
        };
        
        template<class O>
        struct typed_comparator<O, O> {
            bool operator()(const O &lhs, const O &rhs) const {
                return lhs == rhs;
            }
        };
        
        template<class L, class R>
        bool compare_any(const L &lhs, const R &rhs) {
            return typed_comparator<L, R>()(lhs, rhs);
        }
    }
}
#pragma once

#include <string>   //  std::string

namespace sqlite_orm {
    
    namespace internal {
        
        /**
         *  DISCTINCT generic container.
         */
        template<class T>
        struct distinct_t {
            T t;
            
            operator std::string() const {
                return "DISTINCT";
            }
        };
        
        /**
         *  ALL generic container.
         */
        template<class T>
        struct all_t {
            T t;
            
            operator std::string() const {
                return "ALL";
            }
        };
        
        template<class ...Args>
        struct columns_t {
            bool distinct = false;
            
            template<class L>
            void for_each(L) const {
                //..
            }
            
            int count() const {
                return 0;
            }
        };
        
        template<class T, class ...Args>
        struct columns_t<T, Args...> : public columns_t<Args...> {
            T m;
            
            columns_t(decltype(m) m_, Args&& ...args): super(std::forward<Args>(args)...), m(m_) {}
            
            template<class L>
            void for_each(L l) const {
                l(this->m);
                this->super::for_each(l);
            }
            
            int count() const {
                return 1 + this->super::count();
            }
        private:
            using super = columns_t<Args...>;
        };
        
        template<class ...Args>
        struct set_t {
            
            operator std::string() const {
                return "SET";
            }
            
            template<class F>
            void for_each(F) {
                //..
            }
        };
        
        template<class L, class ...Args>
        struct set_t<L, Args...> : public set_t<Args...> {
            static_assert(is_assign_t<typename std::remove_reference<L>::type>::value, "set_t argument must be assign_t");
            
            L l;
            
            using super = set_t<Args...>;
            using self = set_t<L, Args...>;
            
            set_t(L l_, Args&& ...args) : super(std::forward<Args>(args)...), l(std::forward<L>(l_)) {}
            
            template<class F>
            void for_each(F f) {
                f(l);
                this->super::for_each(f);
            }
        };
        
        /**
         *  This class is used to store explicit mapped type T and its column descriptor (member pointer/getter/setter).
         *  Is useful when mapped type is derived from other type and base class has members mapped to a storage.
         */
        template<class T, class F>
        struct column_pointer {
            using type = T;
            using field_type = F;
            
            field_type field;
        };
        
        /**
         *  Subselect object type.
         */
        template<class T, class ...Args>
        struct select_t {
            using return_type = T;
            using conditions_type = std::tuple<Args...>;
            
            return_type col;
            conditions_type conditions;
        };
        
        /**
         *  Union object type.
         */
        template<class L, class R>
        struct union_t {
            using left_type = L;
            using right_type = R;
            
            left_type left;
            right_type right;
            bool all = false;
            
            union_t(left_type l, right_type r, decltype(all) all_): left(std::move(l)), right(std::move(r)), all(all_) {}
            
            union_t(left_type l, right_type r): left(std::move(l)), right(std::move(r)) {}

            operator std::string() const {
                if(!this->all){
                    return "UNION";
                }else{
                    return "UNION ALL";
                }
            }
        };
        
        /**
         *  Generic way to get DISTINCT value from any type.
         */
        template<class T>
        bool get_distinct(const T &t) {
            return false;
        }
        
        template<class ...Args>
        bool get_distinct(const columns_t<Args...> &cols) {
            return cols.distinct;
        }
    }
    
    template<class T>
    internal::distinct_t<T> distinct(T t) {
        return {t};
    }
    
    template<class T>
    internal::all_t<T> all(T t) {
        return {t};
    }
    
    template<class ...Args>
    internal::columns_t<Args...> distinct(internal::columns_t<Args...> cols) {
        cols.distinct = true;
        return cols;
    }
    
    /**
     *  SET keyword used in UPDATE ... SET queries.
     *  Args must have `assign_t` type. E.g. set(assign(&User::id, 5)) or set(c(&User::id) = 5)
     */
    template<class ...Args>
    internal::set_t<Args...> set(Args&& ...args) {
        return {std::forward<Args>(args)...};
    }
    
    template<class ...Args>
    internal::columns_t<Args...> columns(Args&& ...args) {
        return {std::forward<Args>(args)...};
    }
    
    /**
     *  Use it like this:
     *  struct MyType : BaseType { ... };
     *  storage.select(column<MyType>(&BaseType::id));
     */
    template<class T, class F>
    internal::column_pointer<T, F> column(F f) {
        return {f};
    }
    
    /**
     *  Public function for subselect query. Is useful in UNION queries.
     */
    template<class T, class ...Args>
    internal::select_t<T, Args...> select(T t, Args ...args) {
        return {std::move(t), std::make_tuple<Args...>(std::forward<Args>(args)...)};
    }
    
    /**
     *  Public function for UNION operator.
     *  lhs and rhs are subselect objects.
     *  Look through example in examples/union.cpp
     */
    template<class L, class R>
    internal::union_t<L, R> union_(L lhs, R rhs) {
        return {std::move(lhs), std::move(rhs)};
    }
    
    /**
     *  Public function for UNION ALL operator.
     *  lhs and rhs are subselect objects.
     *  Look through example in examples/union.cpp
     */
    template<class L, class R>
    internal::union_t<L, R> union_all(L lhs, R rhs) {
        return {std::move(lhs), std::move(rhs), true};
    }
}
#pragma once

#include <string>   //  std::string
#include <sqlite3.h>
#include <system_error> //  std::error_code, std::system_error

// #include "error_code.h"


namespace sqlite_orm {
    
    namespace internal {
        
        struct database_connection {
            
            database_connection(const std::string &filename) {
                auto rc = sqlite3_open(filename.c_str(), &this->db);
                if(rc != SQLITE_OK){
                    throw std::system_error(std::error_code(sqlite3_errcode(this->db), get_sqlite_error_category()));
                }
            }
            
            ~database_connection() {
                sqlite3_close(this->db);
            }
            
            sqlite3* get_db() {
                return this->db;
            }
            
        protected:
            sqlite3 *db = nullptr;
        };
    }
}
#pragma once

#include <type_traits>  //  std::enable_if, std::is_member_pointer

// #include "select_constraints.h"

// #include "column.h"


namespace sqlite_orm {
    
    namespace internal {
        
        /**
         *  Trait class used to define table mapped type by setter/getter/member
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
#pragma once

#include <sqlite3.h>

namespace sqlite_orm {
    
    /**
     *  Guard class which finalizes `sqlite3_stmt` in dtor
     */
    struct statement_finalizer {
        sqlite3_stmt *stmt = nullptr;
        
        statement_finalizer(decltype(stmt) stmt_): stmt(stmt_) {}
        
        inline ~statement_finalizer() {
            sqlite3_finalize(this->stmt);
        }
    };
}
#pragma once

namespace sqlite_orm {
    
    /**
     *  Helper classes used by statement_binder and row_extractor.
     */
    struct int_or_smaller_tag{};
    struct bigint_tag{};
    struct real_tag{};
    
    template<class V>
    struct arithmetic_tag
    {
        using type = std::conditional_t<
        std::is_integral<V>::value,
        // Integer class
        std::conditional_t<
        sizeof(V) <= sizeof(int),
        int_or_smaller_tag,
        bigint_tag
        >,
        // Floating-point class
        real_tag
        >;
    };
    
    template<class V>
    using arithmetic_tag_t = typename arithmetic_tag<V>::type;
}
#pragma once

namespace sqlite_orm {
    
    /**
     *  Specialization for optional type (std::shared_ptr / std::unique_ptr).
     */
    template <typename T>
    struct is_std_ptr : std::false_type {};
    
    template <typename T>
    struct is_std_ptr<std::shared_ptr<T>> : std::true_type {
        static std::shared_ptr<T> make(const T& v) {
            return std::make_shared<T>(v);
        }
    };
    
    template <typename T>
    struct is_std_ptr<std::unique_ptr<T>> : std::true_type {
        static std::unique_ptr<T> make(const T& v) {
            return std::make_unique<T>(v);
        }
    };
}
#pragma once

#include <sqlite3.h>
#include <type_traits>  //  std::enable_if_t, std::is_arithmetic, std::is_same
#include <string>   //  std::string, std::wstring
#include <codecvt>  //  std::wstring_convert, std::codecvt_utf8_utf16
#include <vector>   //  std::vector
#include <cstddef>  //  std::nullptr_t

// #include "is_std_ptr.h"


namespace sqlite_orm {
    
    /**
     *  Helper class used for binding fields to sqlite3 statements.
     */
    template<class V, typename Enable = void>
    struct statement_binder {
        int bind(sqlite3_stmt *stmt, int index, const V &value);
    };
    
    /**
     *  Specialization for arithmetic types.
     */
    template<class V>
    struct statement_binder<
    V,
    std::enable_if_t<std::is_arithmetic<V>::value>
    >
    {
        int bind(sqlite3_stmt *stmt, int index, const V &value) {
            return bind(stmt, index, value, tag());
        }
        
    private:
        using tag = arithmetic_tag_t<V>;
        
        int bind(sqlite3_stmt *stmt, int index, const V &value, const int_or_smaller_tag&) {
            return sqlite3_bind_int(stmt, index, static_cast<int>(value));
        }
        
        int bind(sqlite3_stmt *stmt, int index, const V &value, const bigint_tag&) {
            return sqlite3_bind_int64(stmt, index, static_cast<sqlite3_int64>(value));
        }
        
        int bind(sqlite3_stmt *stmt, int index, const V &value, const real_tag&) {
            return sqlite3_bind_double(stmt, index, static_cast<double>(value));
        }
    };
    
    
    /**
     *  Specialization for std::string and C-string.
     */
    template<class V>
    struct statement_binder<
    V,
    std::enable_if_t<
    std::is_same<V, std::string>::value
    ||
    std::is_same<V, const char*>::value
    >
    >
    {
        int bind(sqlite3_stmt *stmt, int index, const V &value) {
            return sqlite3_bind_text(stmt, index, string_data(value), -1, SQLITE_TRANSIENT);
        }
        
    private:
        const char* string_data(const std::string& s) const {
            return s.c_str();
        }
        
        const char* string_data(const char* s) const{
            return s;
        }
    };
    
    /**
     *  Specialization for std::wstring and C-wstring.
     */
    template<class V>
    struct statement_binder<
    V,
    std::enable_if_t<
    std::is_same<V, std::wstring>::value
    ||
    std::is_same<V, const wchar_t*>::value
    >
    >
    {
        int bind(sqlite3_stmt *stmt, int index, const V &value) {
            std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> converter;
            std::string utf8Str = converter.to_bytes(value);
            return statement_binder<decltype(utf8Str)>().bind(stmt, index, utf8Str);
        }
    };
    
    /**
     *  Specialization for std::nullptr_t.
     */
    template<class V>
    struct statement_binder<
    V,
    std::enable_if_t<std::is_same<V, std::nullptr_t>::value>
    >
    {
        int bind(sqlite3_stmt *stmt, int index, const V &) {
            return sqlite3_bind_null(stmt, index);
        }
    };
    
    template<class V>
    struct statement_binder<
    V,
    std::enable_if_t<is_std_ptr<V>::value>
    >
    {
        using value_type = typename V::element_type;
        
        int bind(sqlite3_stmt *stmt, int index, const V &value) {
            if(value){
                return statement_binder<value_type>().bind(stmt, index, *value);
            }else{
                return statement_binder<std::nullptr_t>().bind(stmt, index, nullptr);
            }
        }
    };
    
    /**
     *  Specialization for optional type (std::vector<char>).
     */
    template<class V>
    struct statement_binder<
    V,
    std::enable_if_t<std::is_same<V, std::vector<char>>::value>
    >
    {
        int bind(sqlite3_stmt *stmt, int index, const V &value) {
            if (value.size()) {
                return sqlite3_bind_blob(stmt, index, (const void *)&value.front(), int(value.size()), SQLITE_TRANSIENT);
            }else{
                return sqlite3_bind_blob(stmt, index, "", 0, SQLITE_TRANSIENT);
            }
        }
    };
}
#pragma once

#include <sqlite3.h>
#include <type_traits>  //  std::enable_if_t, std::is_arithmetic, std::is_same, std::enable_if
#include <cstdlib>  //  atof, atoi, atoll
#include <string>   //  std::string, std::wstring
#include <codecvt>  //  std::wstring_convert, std::codecvt_utf8_utf16
#include <vector>   //  std::vector
#include <cstring>  //  strlen
#include <algorithm>    //  std::copy
#include <iterator> //  std::back_inserter
#include <tuple>    //  std::tuple, std::tuple_size, std::tuple_element

// #include "arithmetic_tag.h"


namespace sqlite_orm {
    
    /**
     *  Helper class used to cast values from argv to V class
     *  which depends from column type.
     *
     */
    template<class V, typename Enable = void>
    struct row_extractor
    {
        //  used in sqlite3_exec (select)
        V extract(const char *row_value);
        
        //  used in sqlite_column (iteration, get_all)
        V extract(sqlite3_stmt *stmt, int columnIndex);
    };
    
    /**
     *  Specialization for arithmetic types.
     */
    template<class V>
    struct row_extractor<
    V,
    std::enable_if_t<std::is_arithmetic<V>::value>
    >
    {
        V extract(const char *row_value) {
            return extract(row_value, tag());
        }
        
        V extract(sqlite3_stmt *stmt, int columnIndex) {
            return extract(stmt, columnIndex, tag());
        }
        
    private:
        using tag = arithmetic_tag_t<V>;
        
        V extract(const char *row_value, const int_or_smaller_tag&) {
            return static_cast<V>(atoi(row_value));
        }
        
        V extract(sqlite3_stmt *stmt, int columnIndex, const int_or_smaller_tag&) {
            return static_cast<V>(sqlite3_column_int(stmt, columnIndex));
        }
        
        V extract(const char *row_value, const bigint_tag&) {
            return static_cast<V>(atoll(row_value));
        }
        
        V extract(sqlite3_stmt *stmt, int columnIndex, const bigint_tag&) {
            return static_cast<V>(sqlite3_column_int64(stmt, columnIndex));
        }
        
        V extract(const char *row_value, const real_tag&) {
            return static_cast<V>(atof(row_value));
        }
        
        V extract(sqlite3_stmt *stmt, int columnIndex, const real_tag&) {
            return static_cast<V>(sqlite3_column_double(stmt, columnIndex));
        }
    };
    
    /**
     *  Specialization for std::string.
     */
    template<class V>
    struct row_extractor<
    V,
    std::enable_if_t<std::is_same<V, std::string>::value>
    >
    {
        std::string extract(const char *row_value) {
            if(row_value){
                return row_value;
            }else{
                return {};
            }
        }
        
        std::string extract(sqlite3_stmt *stmt, int columnIndex) {
            auto cStr = (const char*)sqlite3_column_text(stmt, columnIndex);
            if(cStr){
                return cStr;
            }else{
                return {};
            }
        }
    };
    
    /**
     *  Specialization for std::wstring.
     */
    template<class V>
    struct row_extractor<
    V,
    std::enable_if_t<std::is_same<V, std::wstring>::value>
    >
    {
        std::wstring extract(const char *row_value) {
            if(row_value){
                std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> converter;
                return converter.from_bytes(row_value);
            }else{
                return {};
            }
        }
        
        std::wstring extract(sqlite3_stmt *stmt, int columnIndex) {
            auto cStr = (const char*)sqlite3_column_text(stmt, columnIndex);
            if(cStr){
                std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> converter;
                return converter.from_bytes(cStr);
            }else{
                return {};
            }
        }
    };
    
    /**
     *  Specialization for std::vector<char>.
     */
    template<class V>
    struct row_extractor<
    V,
    std::enable_if_t<std::is_same<V, std::vector<char>>::value>
    >
    {
        std::vector<char> extract(const char *row_value) {
            if(row_value){
                auto len = ::strlen(row_value);
                return this->go(row_value, static_cast<int>(len));
            }else{
                return {};
            }
        }
        
        std::vector<char> extract(sqlite3_stmt *stmt, int columnIndex) {
            auto bytes = static_cast<const char *>(sqlite3_column_blob(stmt, columnIndex));
            auto len = sqlite3_column_bytes(stmt, columnIndex);
            return this->go(bytes, len);
        }
        
    protected:
        
        std::vector<char> go(const char *bytes, int len) {
            if(len){
                std::vector<char> res;
                res.reserve(len);
                std::copy(bytes,
                          bytes + len,
                          std::back_inserter(res));
                return res;
            }else{
                return {};
            }
        }
    };
    
    template<class V>
    struct row_extractor<
    V,
    std::enable_if_t<is_std_ptr<V>::value>
    >
    {
        using value_type = typename V::element_type;
        
        V extract(const char *row_value) {
            if(row_value){
                return is_std_ptr<V>::make(row_extractor<value_type>().extract(row_value));
            }else{
                return {};
            }
        }
        
        V extract(sqlite3_stmt *stmt, int columnIndex) {
            auto type = sqlite3_column_type(stmt, columnIndex);
            if(type != SQLITE_NULL){
                return is_std_ptr<V>::make(row_extractor<value_type>().extract(stmt, columnIndex));
            }else{
                return {};
            }
        }
    };
    
    /**
     *  Specialization for std::vector<char>.
     */
    template<>
    struct row_extractor<std::vector<char>> {
        std::vector<char> extract(const char *row_value) {
            if(row_value){
                auto len = ::strlen(row_value);
                return this->go(row_value, static_cast<int>(len));
            }else{
                return {};
            }
        }
        
        std::vector<char> extract(sqlite3_stmt *stmt, int columnIndex) {
            auto bytes = static_cast<const char *>(sqlite3_column_blob(stmt, columnIndex));
            auto len = sqlite3_column_bytes(stmt, columnIndex);
            return this->go(bytes, len);
        }
        
    protected:
        
        std::vector<char> go(const char *bytes, int len) {
            if(len){
                std::vector<char> res;
                res.reserve(len);
                std::copy(bytes,
                          bytes + len,
                          std::back_inserter(res));
                return res;
            }else{
                return {};
            }
        }
    };
    
    template<class ...Args>
    struct row_extractor<std::tuple<Args...>> {
        
        std::tuple<Args...> extract(char **argv) {
            std::tuple<Args...> res;
            this->extract<std::tuple_size<decltype(res)>::value>(res, argv);
            return res;
        }
        
        std::tuple<Args...> extract(sqlite3_stmt *stmt, int /*columnIndex*/) {
            std::tuple<Args...> res;
            this->extract<std::tuple_size<decltype(res)>::value>(res, stmt);
            return res;
        }
        
    protected:
        
        template<size_t I, typename std::enable_if<I != 0>::type * = nullptr>
        void extract(std::tuple<Args...> &t, sqlite3_stmt *stmt) {
            using tuple_type = typename std::tuple_element<I - 1, typename std::tuple<Args...>>::type;
            std::get<I - 1>(t) = row_extractor<tuple_type>().extract(stmt, I - 1);
            this->extract<I - 1>(t, stmt);
        }
        
        template<size_t I, typename std::enable_if<I == 0>::type * = nullptr>
        void extract(std::tuple<Args...> &, sqlite3_stmt *) {
            //..
        }
        
        template<size_t I, typename std::enable_if<I != 0>::type * = nullptr>
        void extract(std::tuple<Args...> &t, char **argv) {
            using tuple_type = typename std::tuple_element<I - 1, typename std::tuple<Args...>>::type;
            std::get<I - 1>(t) = row_extractor<tuple_type>().extract(argv[I - 1]);
            this->extract<I - 1>(t, argv);
        }
        
        template<size_t I, typename std::enable_if<I == 0>::type * = nullptr>
        void extract(std::tuple<Args...> &, char **) {
            //..
        }
    };
}
#pragma once

#include <ostream>

namespace sqlite_orm {
    
    enum class sync_schema_result {
        
        /**
         *  created new table, table with the same tablename did not exist
         */
        new_table_created,
        
        /**
         *  table schema is the same as storage, nothing to be done
         */
        already_in_sync,
        
        /**
         *  removed excess columns in table (than storage) without dropping a table
         */
        old_columns_removed,
        
        /**
         *  lacking columns in table (than storage) added without dropping a table
         */
        new_columns_added,
        
        /**
         *  both old_columns_removed and new_columns_added
         */
        new_columns_added_and_old_columns_removed,
        
        /**
         *  old table is dropped and new is recreated. Reasons :
         *      1. delete excess columns in the table than storage if preseve = false
         *      2. Lacking columns in the table cannot be added due to NULL and DEFAULT constraint
         *      3. Reasons 1 and 2 both together
         *      4. data_type mismatch between table and storage.
         */
        dropped_and_recreated,
    };
    
    
    inline std::ostream& operator<<(std::ostream &os, sync_schema_result value) {
        switch(value){
            case sync_schema_result::new_table_created: return os << "new table created";
            case sync_schema_result::already_in_sync: return os << "table and storage is already in sync.";
            case sync_schema_result::old_columns_removed: return os << "old excess columns removed";
            case sync_schema_result::new_columns_added: return os << "new columns added";
            case sync_schema_result::new_columns_added_and_old_columns_removed: return os << "old excess columns removed and new columns added";
            case sync_schema_result::dropped_and_recreated: return os << "old table dropped and recreated";
        }
    }
}
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
#pragma once

// #include "alias.h"


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
#pragma once

#include <string>   //  std::string

namespace sqlite_orm {
    
    namespace internal {
        
        struct rowid_t {
            operator std::string() const {
                return "rowid";
            }
        };
        
        struct oid_t {
            operator std::string() const {
                return "oid";
            }
        };
        
        struct _rowid_t {
            operator std::string() const {
                return "_rowid_";
            }
        };
        
        template<class T>
        struct table_rowid_t : public rowid_t {
            using type = T;
        };
        
        template<class T>
        struct table_oid_t : public oid_t {
            using type = T;
        };
        template<class T>
        struct table__rowid_t : public _rowid_t {
            using type = T;
        };
        
    }
    
    inline internal::rowid_t rowid() {
        return {};
    }
    
    inline internal::oid_t oid() {
        return {};
    }
    
    inline internal::_rowid_t _rowid_() {
        return {};
    }
    
    template<class T>
    internal::table_rowid_t<T> rowid() {
        return {};
    }
    
    template<class T>
    internal::table_oid_t<T> oid() {
        return {};
    }
    
    template<class T>
    internal::table__rowid_t<T> _rowid_() {
        return {};
    }
}
#pragma once

#include <type_traits>  //  std::enable_if, std::is_same
#include <tuple>    //  std::tuple

// #include "core_functions.h"

// #include "aggregate_functions.h"

// #include "select_constraints.h"

// #include "operators.h"

// #include "rowid.h"

// #include "alias.h"

// #include "column.h"


namespace sqlite_orm {
    
    namespace internal {
        
        /**
         *  This is a proxy class used to define what type must have result type depending on select
         *  arguments (member pointer, aggregate functions, etc). Below you can see specializations
         *  for different types. E.g. specialization for core_functions::length_t has `type` int cause
         *  LENGTH returns INTEGER in sqlite. Every column_result_t must have `type` type that equals
         *  c++ SELECT return type for T
         *  T - C++ type
         *  Ts - tables pack from storage. Rarely used. Required in asterisk to define columns mapped for a type
         */
        template<class T, class SFINAE = void>
        struct column_result_t;
        
        template<class O, class F>
        struct column_result_t<F O::*, typename std::enable_if<std::is_member_pointer<F O::*>::value && !std::is_member_function_pointer<F O::*>::value>::type> {
            using type = F;
        };
        
        /**
         *  Common case for all getter types. Getter types are defined in column.h file
         */
        template<class T>
        struct column_result_t<T, typename std::enable_if<is_getter<T>::value>::type> {
            using type = typename getter_traits<T>::field_type;
        };
        
        /**
         *  Common case for all setter types. Setter types are defined in column.h file
         */
        template<class T>
        struct column_result_t<T, typename std::enable_if<is_setter<T>::value>::type> {
            using type = typename setter_traits<T>::field_type;
        };
        
        template<class T>
        struct column_result_t<core_functions::length_t<T>, void> {
            using type = int;
        };
        
#if SQLITE_VERSION_NUMBER >= 3007016
        
        template<class ...Args>
        struct column_result_t<core_functions::char_t_<Args...>, void> {
            using type = std::string;
        };
#endif
        
        template<>
        struct column_result_t<core_functions::random_t, void> {
            using type = int;
        };
        
        template<>
        struct column_result_t<core_functions::changes_t, void> {
            using type = int;
        };
        
        template<class T>
        struct column_result_t<core_functions::abs_t<T>, void> {
            using type = std::shared_ptr<double>;
        };
        
        template<class T>
        struct column_result_t<core_functions::lower_t<T>, void> {
            using type = std::string;
        };
        
        template<class T>
        struct column_result_t<core_functions::upper_t<T>, void> {
            using type = std::string;
        };
        
        template<class X>
        struct column_result_t<core_functions::trim_single_t<X>, void> {
            using type = std::string;
        };
        
        template<class X, class Y>
        struct column_result_t<core_functions::trim_double_t<X, Y>, void> {
            using type = std::string;
        };
        
        template<class X>
        struct column_result_t<core_functions::ltrim_single_t<X>, void> {
            using type = std::string;
        };
        
        template<class X, class Y>
        struct column_result_t<core_functions::ltrim_double_t<X, Y>, void> {
            using type = std::string;
        };
        
        template<class X>
        struct column_result_t<core_functions::rtrim_single_t<X>, void> {
            using type = std::string;
        };
        
        template<class X, class Y>
        struct column_result_t<core_functions::rtrim_double_t<X, Y>, void> {
            using type = std::string;
        };
        
        template<class T, class ...Args>
        struct column_result_t<core_functions::date_t<T, Args...>, void> {
            using type = std::string;
        };
        
        template<class T, class ...Args>
        struct column_result_t<core_functions::datetime_t<T, Args...>, void> {
            using type = std::string;
        };
        
        template<class T>
        struct column_result_t<aggregate_functions::avg_t<T>, void> {
            using type = double;
        };
        
        template<class T>
        struct column_result_t<aggregate_functions::count_t<T>, void> {
            using type = int;
        };
        
        template<>
        struct column_result_t<aggregate_functions::count_asterisk_t, void> {
            using type = int;
        };
        
        template<class T>
        struct column_result_t<aggregate_functions::sum_t<T>, void> {
            using type = std::shared_ptr<double>;
        };
        
        template<class T>
        struct column_result_t<aggregate_functions::total_t<T>, void> {
            using type = double;
        };
        
        template<class T>
        struct column_result_t<aggregate_functions::group_concat_single_t<T>, void> {
            using type = std::string;
        };
        
        template<class T>
        struct column_result_t<aggregate_functions::group_concat_double_t<T>, void> {
            using type = std::string;
        };
        
        template<class T>
        struct column_result_t<aggregate_functions::max_t<T>, void> {
            using type = std::shared_ptr<typename column_result_t<T>::type>;
        };
        
        template<class T>
        struct column_result_t<aggregate_functions::min_t<T>, void> {
            using type = std::shared_ptr<typename column_result_t<T>::type>;
        };
        
        template<class T>
        struct column_result_t<distinct_t<T>, void> {
            using type = typename column_result_t<T>::type;
        };
        
        template<class T>
        struct column_result_t<all_t<T>, void> {
            using type = typename column_result_t<T>::type;
        };
        
        template<class L, class R>
        struct column_result_t<conc_t<L, R>, void> {
            using type = std::string;
        };
        
        template<class L, class R>
        struct column_result_t<add_t<L, R>, void> {
            using type = double;
        };
        
        template<class L, class R>
        struct column_result_t<sub_t<L, R>, void> {
            using type = double;
        };
        
        template<class L, class R>
        struct column_result_t<mul_t<L, R>, void> {
            using type = double;
        };
        
        template<class L, class R>
        struct column_result_t<div_t<L, R>, void> {
            using type = double;
        };
        
        template<class L, class R>
        struct column_result_t<mod_t<L, R>, void> {
            using type = double;
        };
        
        template<>
        struct column_result_t<rowid_t, void> {
            using type = int64;
        };
        
        template<>
        struct column_result_t<oid_t, void> {
            using type = int64;
        };
        
        template<>
        struct column_result_t<_rowid_t, void> {
            using type = int64;
        };
        
        template<class T>
        struct column_result_t<table_rowid_t<T>, void> {
            using type = int64;
        };
        
        template<class T>
        struct column_result_t<table_oid_t<T>, void> {
            using type = int64;
        };
        
        template<class T>
        struct column_result_t<table__rowid_t<T>, void> {
            using type = int64;
        };
        
        template<class T, class C>
        struct column_result_t<alias_column_t<T, C>, void> {
            using type = typename column_result_t<C>::type;
        };
        
        template<class T, class F>
        struct column_result_t<column_pointer<T, F>> : column_result_t<F, void> {};
        
        template<class ...Args>
        struct column_result_t<columns_t<Args...>, void> {
            using type = std::tuple<typename column_result_t<Args>::type...>;
        };
        
        template<class T, class ...Args>
        struct column_result_t<select_t<T, Args...>> : column_result_t<T, void> {};
        
        template<class L, class R>
        struct column_result_t<union_t<L, R>, void> {
            using left_type = typename column_result_t<L>::type;
            using right_type = typename column_result_t<R>::type;
            static_assert(std::is_same<left_type, right_type>::value, "Union subselect queries must return same types");
            using type = left_type;
        };
    }
}
#pragma once

#include <vector>   //  std::vector
#include <string>   //  std::string
#include <tuple>    //  std::tuple
#include <type_traits>  //  std::is_same, std::integral_constant, std::true_type, std::false_type

// #include "column.h"

// #include "tuple_helper.h"

// #include "constraints.h"


namespace sqlite_orm {
    
    namespace internal {
        
        /**
         *  Common case for table_impl class.
         */
        template<typename... Args>
        struct table_impl {
            
            std::vector<std::string> column_names() {
                return {};
            }
            
            template<class ...Op>
            std::vector<std::string> column_names_exept() {
                return {};
            }
            
            template<class ...Op>
            std::vector<std::string> column_names_with() {
                return {};
            }
            
            template<class L>
            void for_each_column(L) {}
            
            template<class L>
            void for_each_column_with_constraints(L) {}
            
            template<class F, class L>
            void for_each_column_with_field_type(L) {}
            
            template<class Op, class L>
            void for_each_column_exept(L) {}
            
            template<class Op, class L>
            void for_each_column_with(L) {}
            
            template<class L>
            void for_each_primary_key(L) {}
            
            int columns_count() const {
                return 0;
            }
            
        };
        
        template<typename H, typename... T>
        struct table_impl<H, T...> : private table_impl<T...> {
            using column_type = H;
            using tail_types = std::tuple<T...>;
            
            table_impl(H h, T ...t) : super(t...), col(h) {}
            
            column_type col;
            
            int columns_count() const {
                return 1 + this->super::columns_count();
            }
            
            /**
             *  column_names_with implementation. Notice that result will be reversed.
             *  It is reversed back in `table` class.
             *  @return vector of column names that have specified Op... conditions.
             */
            template<class ...Op>
            std::vector<std::string> column_names_with() {
                auto res = this->super::template column_names_with<Op...>();
                if(this->col.template has_every<Op...>()) {
                    res.emplace_back(this->col.name);
                }
                return res;
            }
            
            /**
             *  For each implementation. Calls templated lambda with its column
             *  and passed call to superclass.
             */
            template<class L>
            void for_each_column(L l){
                this->apply_to_col_if(l, internal::is_column<column_type>{});
                this->super::for_each_column(l);
            }
            
            /**
             *  For each implementation. Calls templated lambda with its column
             *  and passed call to superclass.
             */
            template<class L>
            void for_each_column_with_constraints(L l){
                l(this->col);
                this->super::for_each_column_with_constraints(l);
            }
            
            template<class F, class L>
            void for_each_column_with_field_type(L l) {
                this->apply_to_col_if(l, std::is_same<F, typename column_type::field_type>{});
                this->super::template for_each_column_with_field_type<F, L>(l);
            }
            
            /**
             *  Working version of `for_each_column_exept`. Calls lambda if column has no option and fire super's function.
             */
            template<class Op, class L>
            void for_each_column_exept(L l) {
                using has_opt = tuple_helper::tuple_contains_type<Op, typename column_type::constraints_type>;
                this->apply_to_col_if(l, std::integral_constant<bool, !has_opt::value>{});
                this->super::template for_each_column_exept<Op, L>(l);
            }
            
            /**
             *  Working version of `for_each_column_with`. Calls lambda if column has option and fire super's function.
             */
            template<class Op, class L>
            void for_each_column_with(L l) {
                this->apply_to_col_if(l, tuple_helper::tuple_contains_type<Op, typename column_type::constraints_type>{});
                this->super::template for_each_column_with<Op, L>(l);
            }
            
            /**
             *  Calls l(this->col) if H is primary_key_t
             */
            template<class L>
            void for_each_primary_key(L l) {
                this->apply_to_col_if(l, internal::is_primary_key<H>{});
                this->super::for_each_primary_key(l);
            }
            
            template<class L>
            void apply_to_col_if(L& l, std::true_type) {
                l(this->col);
            }
            
            template<class L>
            void apply_to_col_if(L&, std::false_type) {}
            
        private:
            using super = table_impl<T...>;
        };
    }
}
#pragma once

#include <string>   //  std::string
#include <type_traits>  //  std::remove_reference, std::is_same, std::is_base_of
#include <vector>   //  std::vector
#include <tuple>    //  std::tuple_size, std::tuple_element
#include <algorithm>    //  std::reverse, std::find_if

// #include "table_impl.h"

// #include "column_result.h"

// #include "static_magic.h"

// #include "typed_comparator.h"

// #include "constraints.h"

// #include "tuple_helper.h"

// #include "table_info.h"

// #include "type_printer.h"

// #include "column.h"


namespace sqlite_orm {
    
    namespace internal {
        
        /**
         *  Table interface class. Implementation is hidden in `table_impl` class.
         */
        template<class T, class ...Cs>
        struct table_t {
            using impl_type = table_impl<Cs...>;
            using object_type = T;
            
            /**
             *  Table name.
             */
            const std::string name;
            
            /**
             *  Implementation that stores columns information.
             */
            impl_type impl;
            
            table_t(decltype(name) name_, decltype(impl) impl_): name(std::move(name_)), impl(std::move(impl_)) {}
            
            bool _without_rowid = false;
            
            table_t<T, Cs...> without_rowid() const {
                auto res = *this;
                res._without_rowid = true;
                return res;
            }
            
            /**
             *  Function used to get field value from object by mapped member pointer/setter/getter
             */
            template<class F, class C>
            const F* get_object_field_pointer(const object_type &obj, C c) {
                const F *res = nullptr;
                using field_type = typename internal::column_result_t<C>::type;
                this->for_each_column_with_field_type<field_type>([&res, &c, &obj, this](auto &col){
                    using namespace static_magic;
                    using column_type = typename std::remove_reference<decltype(col)>::type;
                    using member_pointer_t = typename column_type::member_pointer_t;
                    using getter_type = typename column_type::getter_type;
                    using setter_type = typename column_type::setter_type;
                    if(!res){
                        static_if<std::is_same<C, member_pointer_t>{}>([&res, &obj, &col, &c]{
                            if(compare_any(col.member_pointer, c)){
                                res = &(obj.*col.member_pointer);
                            }
                        })();
                    }
                    if(!res){
                        static_if<std::is_same<C, getter_type>{}>([&res, &obj, &col, &c]{
                            if(compare_any(col.getter, c)){
                                res = &((obj).*(col.getter))();
                            }
                        })();
                    }
                    if(!res){
                        static_if<std::is_same<C, setter_type>{}>([&res, &obj, &col, &c]{
                            if(compare_any(col.setter, c)){
                                res = &((obj).*(col.getter))();
                            }
                        })();
                    }
                });
                return res;
            }
            
            /**
             *  @return vector of column names of table.
             */
            std::vector<std::string> column_names() {
                std::vector<std::string> res;
                this->impl.for_each_column([&res](auto &c){
                    res.push_back(c.name);
                });
                return res;
            }
            
            std::vector<std::string> composite_key_columns_names() {
                std::vector<std::string> res;
                this->impl.for_each_primary_key([this, &res](auto c){
                    res = this->composite_key_columns_names(c);
                });
                return res;
            }
            
            std::vector<std::string> primary_key_column_names() {
                std::vector<std::string> res;
                this->impl.template for_each_column_with<constraints::primary_key_t<>>([&res](auto &c){
                    res.push_back(c.name);
                });
                if(!res.size()){
                    res = this->composite_key_columns_names();
                }
                return res;
            }
            
            template<class ...Args>
            std::vector<std::string> composite_key_columns_names(constraints::primary_key_t<Args...> pk) {
                std::vector<std::string> res;
                using pk_columns_tuple = decltype(pk.columns);
                res.reserve(std::tuple_size<pk_columns_tuple>::value);
                tuple_helper::iterator<std::tuple_size<pk_columns_tuple>::value - 1, Args...>()(pk.columns, [this, &res](auto &v){
                    res.push_back(this->find_column_name(v));
                });
                return res;
            }
            
            int columns_count() const {
                return this->impl.columns_count();
            }
            
            /**
             *  Searches column name by class member pointer passed as first argument.
             *  @return column name or empty string if nothing found.
             */
            template<
            class F,
            class O,
            typename = typename std::enable_if<std::is_member_pointer<F O::*>::value && !std::is_member_function_pointer<F O::*>::value>::type>
            std::string find_column_name(F O::*m) {
                std::string res;
                this->template for_each_column_with_field_type<F>([&res, m](auto c) {
                    if(c.member_pointer == m) {
                        res = c.name;
                    }
                });
                return res;
            }
            
            /**
             *  Searches column name by class getter function member pointer passed as first argument.
             *  @return column name or empty string if nothing found.
             */
            template<class G>
            std::string find_column_name(G getter, typename std::enable_if<is_getter<G>::value>::type * = nullptr) {
                std::string res;
                using field_type = typename getter_traits<G>::field_type;
                this->template for_each_column_with_field_type<field_type>([&res, getter](auto c) {
                    if(c.getter == getter) {
                        res = c.name;
                    }
                });
                return res;
            }
            
            /**
             *  Searches column name by class setter function member pointer passed as first argument.
             *  @return column name or empty string if nothing found.
             */
            template<class S>
            std::string find_column_name(S setter, typename std::enable_if<is_setter<S>::value>::type * = nullptr) {
                std::string res;
                using field_type = typename setter_traits<S>::field_type;
                this->template for_each_column_with_field_type<field_type>([&res, setter](auto c) {
                    if(c.setter == setter) {
                        res = c.name;
                    }
                });
                return res;
            }
            
            /**
             *  @return vector of column names that have constraints provided as template arguments (not_null, autoincrement).
             */
            template<class ...Op>
            std::vector<std::string> column_names_with() {
                auto res = this->impl.template column_names_with<Op...>();
                std::reverse(res.begin(),
                             res.end());
                return res;
            }
            
            /**
             *  Iterates all columns and fires passed lambda. Lambda must have one and only templated argument Otherwise code will
             *  not compile. Excludes table constraints (e.g. foreign_key_t) at the end of the columns list. To iterate columns with
             *  table constraints use for_each_column_with_constraints instead.
             *  L is lambda type. Do not specify it explicitly.
             *  @param l Lambda to be called per column itself. Must have signature like this [] (auto col) -> void {}
             */
            template<class L>
            void for_each_column(L l) {
                this->impl.for_each_column(l);
            }
            
            template<class L>
            void for_each_column_with_constraints(L l) {
                this->impl.for_each_column_with_constraints(l);
            }
            
            template<class F, class L>
            void for_each_column_with_field_type(L l) {
                this->impl.template for_each_column_with_field_type<F, L>(l);
            }
            
            /**
             *  Iterates all columns exept ones that have specified constraints and fires passed lambda.
             *  Lambda must have one and only templated argument Otherwise code will not compile.
             *  L is lambda type. Do not specify it explicitly.
             *  @param l Lambda to be called per column itself. Must have signature like this [] (auto col) -> void {}
             */
            template<class Op, class L>
            void for_each_column_exept(L l) {
                this->impl.template for_each_column_exept<Op>(l);
            }
            
            /**
             *  Iterates all columns that have specified constraints and fires passed lambda.
             *  Lambda must have one and only templated argument Otherwise code will not compile.
             *  L is lambda type. Do not specify it explicitly.
             *  @param l Lambda to be called per column itself. Must have signature like this [] (auto col) -> void {}
             */
            template<class Op, class L>
            void for_each_column_with(L l) {
                this->impl.template for_each_column_with<Op>(l);
            }
            
            std::vector<table_info> get_table_info() {
                std::vector<table_info> res;
                res.reserve(size_t(this->columns_count()));
                this->for_each_column([&res](auto &col){
                    std::string dft;
                    using field_type = typename std::remove_reference<decltype(col)>::type::field_type;
                    if(auto d = col.default_value()) {
                        auto needQuotes = std::is_base_of<text_printer, type_printer<field_type>>::value;
                        if(needQuotes){
                            dft = "'" + *d + "'";
                        }else{
                            dft = *d;
                        }
                    }
                    table_info i{
                        -1,
                        col.name,
                        type_printer<field_type>().print(),
                        col.not_null(),
                        dft,
                        col.template has<constraints::primary_key_t<>>(),
                    };
                    res.emplace_back(i);
                });
                std::vector<std::string> compositeKeyColumnNames;
                this->impl.for_each_primary_key([this, &compositeKeyColumnNames](auto c){
                    compositeKeyColumnNames = this->composite_key_columns_names(c);
                });
                for(size_t i = 0; i < compositeKeyColumnNames.size(); ++i) {
                    auto &columnName = compositeKeyColumnNames[i];
                    auto it = std::find_if(res.begin(),
                                           res.end(),
                                           [&columnName](const table_info &ti) {
                                               return ti.name == columnName;
                                           });
                    if(it != res.end()){
                        it->pk = static_cast<int>(i + 1);
                    }
                }
                return res;
            }
            
        };
    }
    
    /**
     *  Function used for table creation. Do not use table constructor - use this function
     *  cause table class is templated and its constructing too (just like std::make_shared or std::make_pair).
     */
    template<class ...Cs, class T = typename std::tuple_element<0, std::tuple<Cs...>>::type::object_type>
    internal::table_t<T, Cs...> make_table(const std::string &name, Cs&& ...args) {
        return {name, internal::table_impl<Cs...>(std::forward<Cs>(args)...)};
    }
    
    template<class T, class ...Cs>
    internal::table_t<T, Cs...> make_table(const std::string &name, Cs&& ...args) {
        return {name, internal::table_impl<Cs...>(std::forward<Cs>(args)...)};
    }
}
#pragma once

#include <string>   //  std::string
#include <sqlite3.h>    
#include <cstddef>  //  std::nullptr_t
#include <system_error> //  std::system_error, std::error_code
#include <sstream>  //  std::stringstream
#include <cstdlib>  //  std::atoi
#include <type_traits>  //  std::forward, std::enable_if, std::is_same, std::remove_reference
#include <utility>  //  std::pair, std::make_pair
#include <vector>   //  std::vector
#include <algorithm>    //  std::find_if

// #include "error_code.h"

// #include "statement_finalizer.h"

// #include "row_extractor.h"

// #include "constraints.h"

// #include "select_constraints.h"

// #include "field_printer.h"

// #include "table_info.h"

// #include "sync_schema_result.h"

// #include "sqlite_type.h"


namespace sqlite_orm {
    
    namespace internal {
        
        /**
         *  This is a generic implementation. Used as a tail in storage_impl inheritance chain
         */
        template<class ...Ts>
        struct storage_impl {
            
            template<class L>
            void for_each(L) {}
            
            int foreign_keys_count() {
                return 0;
            }
            
            template<class O>
            std::string dump(const O &, sqlite3 *, std::nullptr_t) {
                throw std::system_error(std::make_error_code(orm_error_code::type_is_not_mapped_to_storage));
            }
            
            bool table_exists(const std::string &tableName, sqlite3 *db) {
                auto res = false;
                std::stringstream ss;
                ss << "SELECT COUNT(*) FROM sqlite_master WHERE type = '" << "table" << "' AND name = '" << tableName << "'";
                auto query = ss.str();
                auto rc = sqlite3_exec(db,
                                       query.c_str(),
                                       [](void *data, int argc, char **argv,char ** /*azColName*/) -> int {
                                           auto &res = *(bool*)data;
                                           if(argc){
                                               res = !!std::atoi(argv[0]);
                                           }
                                           return 0;
                                       }, &res, nullptr);
                if(rc != SQLITE_OK) {
                    throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                }
                return res;
            }
            
            void begin_transaction(sqlite3 *db) {
                std::stringstream ss;
                ss << "BEGIN TRANSACTION";
                auto query = ss.str();
                sqlite3_stmt *stmt;
                if (sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
                    statement_finalizer finalizer{stmt};
                    if (sqlite3_step(stmt) == SQLITE_DONE) {
                        //  done..
                    }else{
                        throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                    }
                }else {
                    throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                }
            }
            
            void commit(sqlite3 *db) {
                std::stringstream ss;
                ss << "COMMIT";
                auto query = ss.str();
                sqlite3_stmt *stmt;
                if (sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
                    statement_finalizer finalizer{stmt};
                    if (sqlite3_step(stmt) == SQLITE_DONE) {
                        //  done..
                    }else{
                        throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                    }
                }else {
                    throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                }
            }
            
            void rollback(sqlite3 *db) {
                std::stringstream ss;
                ss << "ROLLBACK";
                auto query = ss.str();
                sqlite3_stmt *stmt;
                if (sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
                    statement_finalizer finalizer{stmt};
                    if (sqlite3_step(stmt) == SQLITE_DONE) {
                        //  done..
                    }else{
                        throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                    }
                }else {
                    throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                }
            }
            
            void rename_table(sqlite3 *db, const std::string &oldName, const std::string &newName) {
                std::stringstream ss;
                ss << "ALTER TABLE " << oldName << " RENAME TO " << newName;
                auto query = ss.str();
                sqlite3_stmt *stmt;
                if (sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
                    statement_finalizer finalizer{stmt};
                    if (sqlite3_step(stmt) == SQLITE_DONE) {
                        //  done..
                    }else{
                        throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                    }
                }else {
                    throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                }
            }
            
            std::string current_timestamp(sqlite3 *db) {
                std::string res;
                std::stringstream ss;
                ss << "SELECT CURRENT_TIMESTAMP";
                auto query = ss.str();
                auto rc = sqlite3_exec(db,
                                       query.c_str(),
                                       [](void *data, int argc, char **argv, char **) -> int {
                                           auto &res = *(std::string*)data;
                                           if(argc){
                                               if(argv[0]){
                                                   res = row_extractor<std::string>().extract(argv[0]);
                                               }
                                           }
                                           return 0;
                                       }, &res, nullptr);
                if(rc != SQLITE_OK) {
                    throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                }
                return res;
            }
        };
        
        template<class H, class ...Ts>
        struct storage_impl<H, Ts...> : public storage_impl<Ts...> {
            using table_type = H;
            
            storage_impl(H h, Ts ...ts) : super(std::forward<Ts>(ts)...), table(std::move(h)) {}
            
            table_type table;
            
            template<class L>
            void for_each(L l) {
                this->super::for_each(l);
                l(this);
            }
            
#if SQLITE_VERSION_NUMBER >= 3006019
            
            /**
             *  Returns foreign keys count in table definition
             */
            int foreign_keys_count() {
                auto res = 0;
                this->table.for_each_column_with_constraints([&res](auto c){
                    if(internal::is_foreign_key<decltype(c)>::value) {
                        ++res;
                    }
                });
                return res;
            }
            
#endif
            
            /**
             *  Is used to get column name by member pointer to a base class.
             *  Main difference between `column_name` and `column_name_simple` is that
             *  `column_name` has SFINAE check for type equality but `column_name_simple` has not.
             */
            template<class O, class F>
            std::string column_name_simple(F O::*m) {
                return this->table.find_column_name(m);
            }
            
            /**
             *  Same thing as above for getter.
             */
            template<class T, typename std::enable_if<is_getter<T>::value>::type>
            std::string column_name_simple(T g) {
                return this->table.find_column_name(g);
            }
            
            /**
             *  Same thing as above for setter.
             */
            template<class T, typename std::enable_if<is_setter<T>::value>::type>
            std::string column_name_simple(T s) {
                return this->table.find_column_name(s);
            }
            
            /**
             *  Cute function used to find column name by its type and member pointer. Uses SFINAE to
             *  skip inequal type O.
             */
            template<class O, class F, class HH = typename H::object_type>
            std::string column_name(F O::*m, typename std::enable_if<std::is_same<O, HH>::value>::type * = nullptr) {
                return this->table.find_column_name(m);
            }
            
            /**
             *  Opposite version of function defined above. Just calls same function in superclass.
             */
            template<class O, class F, class HH = typename H::object_type>
            std::string column_name(F O::*m, typename std::enable_if<!std::is_same<O, HH>::value>::type * = nullptr) {
                return this->super::column_name(m);
            }
            
            /**
             *  Cute function used to find column name by its type and getter pointer. Uses SFINAE to
             *  skip inequal type O.
             */
            template<class O, class F, class HH = typename H::object_type>
            std::string column_name(const F& (O::*g)() const, typename std::enable_if<std::is_same<O, HH>::value>::type * = nullptr) {
                return this->table.find_column_name(g);
            }
            
            /**
             *  Opposite version of function defined above. Just calls same function in superclass.
             */
            template<class O, class F, class HH = typename H::object_type>
            std::string column_name(const F& (O::*g)() const, typename std::enable_if<!std::is_same<O, HH>::value>::type * = nullptr) {
                return this->super::column_name(g);
            }
            
            /**
             *  Cute function used to find column name by its type and setter pointer. Uses SFINAE to
             *  skip inequal type O.
             */
            template<class O, class F, class HH = typename H::object_type>
            std::string column_name(void (O::*s)(F), typename std::enable_if<std::is_same<O, HH>::value>::type * = nullptr) {
                return this->table.find_column_name(s);
            }
            
            /**
             *  Opposite version of function defined above. Just calls same function in superclass.
             */
            template<class O, class F, class HH = typename H::object_type>
            std::string column_name(void (O::*s)(F), typename std::enable_if<!std::is_same<O, HH>::value>::type * = nullptr) {
                return this->super::column_name(s);
            }
            
            template<class T, class F, class HH = typename H::object_type>
            std::string column_name(const column_pointer<T, F> &c, typename std::enable_if<std::is_same<T, HH>::value>::type * = nullptr) {
                return this->column_name_simple(c.field);
            }
            
            template<class T, class F, class HH = typename H::object_type>
            std::string column_name(const column_pointer<T, F> &c, typename std::enable_if<!std::is_same<T, HH>::value>::type * = nullptr) {
                return this->super::column_name(c);
            }
            
            template<class O, class HH = typename H::object_type>
            auto& get_impl(typename std::enable_if<std::is_same<O, HH>::value>::type * = nullptr) {
                return *this;
            }
            
            template<class O, class HH = typename H::object_type>
            auto& get_impl(typename std::enable_if<!std::is_same<O, HH>::value>::type * = nullptr) {
                return this->super::template get_impl<O>();
            }
            
            template<class O, class HH = typename H::object_type>
            std::string find_table_name(typename std::enable_if<std::is_same<O, HH>::value>::type * = nullptr) {
                return this->table.name;
            }
            
            template<class O, class HH = typename H::object_type>
            std::string find_table_name(typename std::enable_if<!std::is_same<O, HH>::value>::type * = nullptr) {
                return this->super::template find_table_name<O>();
            }
            
            template<class O, class HH = typename H::object_type>
            std::string dump(const O &o, typename std::enable_if<!std::is_same<O, HH>::value>::type * = nullptr) {
                return this->super::dump(o, nullptr);
            }
            
            template<class O, class HH = typename H::object_type>
            std::string dump(const O &o, typename std::enable_if<std::is_same<O, HH>::value>::type * = nullptr) {
                std::stringstream ss;
                ss << "{ ";
                std::vector<std::pair<std::string, std::string>> pairs;
                this->table.for_each_column([&pairs, &o] (auto &c) {
                    using field_type = typename std::remove_reference<decltype(c)>::type::field_type;
                    const field_type *value = nullptr;
                    if(c.member_pointer){
                        value = &(o.*c.member_pointer);
                    }else{
                        value = &((o).*(c.getter))();
                    }
                    pairs.push_back(std::make_pair(c.name, field_printer<field_type>()(*value)));
                });
                for(size_t i = 0; i < pairs.size(); ++i) {
                    auto &p = pairs[i];
                    ss << p.first << " : '" << p.second << "'";
                    if(i < pairs.size() - 1) {
                        ss << ", ";
                    }else{
                        ss << " }";
                    }
                }
                return ss.str();
            }
            
            std::vector<table_info> get_table_info(const std::string &tableName, sqlite3 *db) {
                std::vector<table_info> res;
                auto query = "PRAGMA table_info('" + tableName + "')";
                auto rc = sqlite3_exec(db,
                                       query.c_str(),
                                       [](void *data, int argc, char **argv,char **) -> int {
                                           auto &res = *(std::vector<table_info>*)data;
                                           if(argc){
                                               auto index = 0;
                                               auto cid = std::atoi(argv[index++]);
                                               std::string name = argv[index++];
                                               std::string type = argv[index++];
                                               bool notnull = !!std::atoi(argv[index++]);
                                               std::string dflt_value = argv[index] ? argv[index] : "";
                                               index++;
                                               auto pk = std::atoi(argv[index++]);
                                               res.push_back(table_info{cid, name, type, notnull, dflt_value, pk});
                                           }
                                           return 0;
                                       }, &res, nullptr);
                if(rc != SQLITE_OK) {
                    throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                }
                return res;
            }
            
            void add_column(const table_info &ti, sqlite3 *db) {
                std::stringstream ss;
                ss << "ALTER TABLE " << this->table.name << " ADD COLUMN " << ti.name << " ";
                ss << ti.type << " ";
                if(ti.pk){
                    ss << "PRIMARY KEY ";
                }
                if(ti.notnull){
                    ss << "NOT NULL ";
                }
                if(ti.dflt_value.length()) {
                    ss << "DEFAULT " << ti.dflt_value << " ";
                }
                auto query = ss.str();
                sqlite3_stmt *stmt;
                auto prepareResult = sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, nullptr);
                if (prepareResult == SQLITE_OK) {
                    statement_finalizer finalizer{stmt};
                    if (sqlite3_step(stmt) == SQLITE_DONE) {
                        //..
                    }else{
                        throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                    }
                }else{
                    throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                }
            }
            
            /**
             *  Copies current table to another table with a given **name**.
             *  Performs CREATE TABLE %name% AS SELECT %this->table.columns_names()% FROM &this->table.name%;
             */
            void copy_table(sqlite3 *db, const std::string &name) {
                std::stringstream ss;
                std::vector<std::string> columnNames;
                this->table.for_each_column([&columnNames] (auto c) {
                    columnNames.emplace_back(c.name);
                });
                auto columnNamesCount = columnNames.size();
                ss << "INSERT INTO " << name << " (";
                for(size_t i = 0; i < columnNamesCount; ++i) {
                    ss << columnNames[i];
                    if(i < columnNamesCount - 1) {
                        ss << ", ";
                    }else{
                        ss << " ";
                    }
                }
                ss << ") ";
                ss << "SELECT ";
                for(size_t i = 0; i < columnNamesCount; ++i) {
                    ss << columnNames[i];
                    if(i < columnNamesCount - 1) {
                        ss << ", ";
                    }else{
                        ss << " ";
                    }
                }
                ss << " FROM '" << this->table.name << "' ";
                auto query = ss.str();
                sqlite3_stmt *stmt;
                if (sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
                    statement_finalizer finalizer{stmt};
                    if (sqlite3_step(stmt) == SQLITE_DONE) {
                        //..
                    }else{
                        throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                    }
                }else{
                    throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                }
            }
            
            sync_schema_result schema_status(sqlite3 *db, bool preserve) {
                
                auto res = sync_schema_result::already_in_sync;
                
                //  first let's see if table with such name exists..
                auto gottaCreateTable = !this->table_exists(this->table.name, db);
                if(!gottaCreateTable){
                    
                    //  get table info provided in `make_table` call..
                    auto storageTableInfo = this->table.get_table_info();
                    
                    //  now get current table info from db using `PRAGMA table_info` query..
                    auto dbTableInfo = get_table_info(this->table.name, db);
                    
                    //  this vector will contain pointers to columns that gotta be added..
                    std::vector<table_info*> columnsToAdd;
                    
                    if(get_remove_add_columns(columnsToAdd, storageTableInfo, dbTableInfo)) {
                        gottaCreateTable = true;
                    }
                    
                    if(!gottaCreateTable){  //  if all storage columns are equal to actual db columns but there are excess columns at the db..
                        if(dbTableInfo.size() > 0){
                            //extra table columns than storage columns
                            if(!preserve){
                                gottaCreateTable = true;
                            }else{
                                res = decltype(res)::old_columns_removed;
                            }
                        }
                    }
                    if(gottaCreateTable){
                        res = decltype(res)::dropped_and_recreated;
                    }else{
                        if(columnsToAdd.size()){
                            //extra storage columns than table columns
                            for(auto columnPointer : columnsToAdd) {
                                if(columnPointer->notnull && columnPointer->dflt_value.empty()){
                                    gottaCreateTable = true;
                                    break;
                                }
                            }
                            if(!gottaCreateTable){
                                if(res == decltype(res)::old_columns_removed) {
                                    res = decltype(res)::new_columns_added_and_old_columns_removed;
                                }else{
                                    res = decltype(res)::new_columns_added;
                                }
                            }else{
                                res = decltype(res)::dropped_and_recreated;
                            }
                        }else{
                            if(res != decltype(res)::old_columns_removed){
                                res = decltype(res)::already_in_sync;
                            }
                        }
                    }
                }else{
                    res = decltype(res)::new_table_created;
                }
                return res;
            }
            
            static bool get_remove_add_columns(std::vector<table_info*>& columnsToAdd,
                                               std::vector<table_info>& storageTableInfo,
                                               std::vector<table_info>& dbTableInfo)
            {
                bool notEqual = false;
                
                //  iterate through storage columns
                for(size_t storageColumnInfoIndex = 0; storageColumnInfoIndex < storageTableInfo.size(); ++storageColumnInfoIndex) {
                    
                    //  get storage's column info
                    auto &storageColumnInfo = storageTableInfo[storageColumnInfoIndex];
                    auto &columnName = storageColumnInfo.name;
                    
                    //  search for a column in db eith the same name
                    auto dbColumnInfoIt = std::find_if(dbTableInfo.begin(),
                                                       dbTableInfo.end(),
                                                       [&columnName](auto &ti){
                                                           return ti.name == columnName;
                                                       });
                    if(dbColumnInfoIt != dbTableInfo.end()){
                        auto &dbColumnInfo = *dbColumnInfoIt;
                        auto dbColumnInfoType = to_sqlite_type(dbColumnInfo.type);
                        auto storageColumnInfoType = to_sqlite_type(storageColumnInfo.type);
                        if(dbColumnInfoType && storageColumnInfoType) {
                            auto columnsAreEqual = dbColumnInfo.name == storageColumnInfo.name &&
                            *dbColumnInfoType == *storageColumnInfoType &&
                            dbColumnInfo.notnull == storageColumnInfo.notnull &&
                            bool(dbColumnInfo.dflt_value.length()) == bool(storageColumnInfo.dflt_value.length()) &&
                            dbColumnInfo.pk == storageColumnInfo.pk;
                            if(!columnsAreEqual){
                                notEqual = true;
                                break;
                            }
                            dbTableInfo.erase(dbColumnInfoIt);
                            storageTableInfo.erase(storageTableInfo.begin() + storageColumnInfoIndex);
                            --storageColumnInfoIndex;
                        }else{
                            
                            //  undefined type/types
                            notEqual = true;
                            break;
                        }
                    }else{
                        columnsToAdd.push_back(&storageColumnInfo);
                    }
                }
                return notEqual;
            }
            
            
        private:
            using super = storage_impl<Ts...>;
            using self = storage_impl<H, Ts...>;
        };
    }
}
#pragma once

#include <memory>   //  std::shared_ptr, std::make_shared
#include <string>   //  std::string
#include <sqlite3.h>
#include <type_traits>  //  std::remove_reference, std::is_base_of, std::decay
#include <cstddef>  //  std::ptrdiff_t
#include <iterator> //  std::input_iterator_tag, std::iterator_traits, std::distance
#include <system_error> //  std::system_error
#include <functional>   //  std::function
#include <sstream>  //  std::stringstream
#include <map>  //  std::map
#include <vector>   //  std::vector
#include <tuple>    //  std::tuple_size, std::tuple
#include <utility>  //  std::forward
#include <set>  //  std::set
#include <algorithm>    //  std::find

// #include "alias.h"

// #include "database_connection.h"

// #include "row_extractor.h"

// #include "statement_finalizer.h"

// #include "error_code.h"

// #include "type_printer.h"

// #include "tuple_helper.h"

// #include "constraints.h"

// #include "table_type.h"

// #include "type_is_nullable.h"

// #include "field_printer.h"

// #include "rowid.h"

// #include "aggregate_functions.h"

// #include "operators.h"

// #include "select_constraints.h"

// #include "core_functions.h"

// #include "conditions.h"

// #include "statement_binder.h"

// #include "column_result.h"

// #include "mapped_type_proxy.h"

// #include "sync_schema_result.h"

// #include "table_info.h"

// #include "storage_impl.h"

// #include "transaction_guard.h"


namespace sqlite_orm {
    
    namespace internal {
        
        /**
         *  Class used as a guard for a transaction. Calls `ROLLBACK` in destructor.
         *  Has explicit `commit()` and `rollback()` functions. After explicit function is fired
         *  guard won't do anything in d-tor. Also you can set `commit_on_destroy` to true to
         *  make it call `COMMIT` on destroy.
         *  S - storage type
         */
        template<class S>
        struct transaction_guard_t {
            using storage_type = S;
            
            /**
             *  This is a public lever to tell a guard what it must do in its destructor
             *  if `gotta_fire` is true
             */
            bool commit_on_destroy = false;
            
            transaction_guard_t(storage_type &s): storage(s) {}
            
            ~transaction_guard_t() {
                if(this->gotta_fire){
                    if(!this->commit_on_destroy){
                        this->storage.rollback();
                    }else{
                        this->storage.commit();
                    }
                }
            }
            
            /**
             *  Call `COMMIT` explicitly. After this call
             *  guard will not call `COMMIT` or `ROLLBACK`
             *  in its destructor.
             */
            void commit() {
                this->storage.commit();
                this->gotta_fire = false;
            }
            
            /**
             *  Call `ROLLBACK` explicitly. After this call
             *  guard will not call `COMMIT` or `ROLLBACK`
             *  in its destructor.
             */
            void rollback() {
                this->storage.rollback();
                this->gotta_fire = false;
            }
            
        protected:
            storage_type &storage;
            bool gotta_fire = true;
        };
    }
}


namespace sqlite_orm {
    
    namespace internal {
        
        /**
         *  Storage class itself. Create an instanse to use it as an interfacto to sqlite db by calling `make_storage` function.
         */
        template<class ...Ts>
        struct storage_t {
            using storage_type = storage_t<Ts...>;
            using impl_type = storage_impl<Ts...>;
            
            template<class T, class ...Args>
            struct view_t {
                using mapped_type = T;
                
                storage_t &storage;
                std::shared_ptr<internal::database_connection> connection;
                
                const std::string query;
                
                view_t(storage_t &stor, decltype(connection) conn, Args&& ...args):
                storage(stor),
                connection(conn),
                query([&args..., &stor]{
                    std::string q;
                    stor.template generate_select_asterisk<T>(&q, args...);
                    return q;
                }()){}
                
                struct iterator_t {
                protected:
                    std::shared_ptr<sqlite3_stmt *> stmt;
                    view_t<T, Args...> &view;
                    std::shared_ptr<T> temp;
                    
                    void extract_value(decltype(temp) &temp) {
                        temp = std::make_shared<T>();
                        auto &storage = this->view.storage;
                        auto &impl = storage.template get_impl<T>();
                        auto index = 0;
                        impl.table.for_each_column([&index, &temp, this] (auto &c) {
                            using field_type = typename std::remove_reference<decltype(c)>::type::field_type;
                            auto value = row_extractor<field_type>().extract(*this->stmt, index++);
                            if(c.member_pointer){
                                auto member_pointer = c.member_pointer;
                                (*temp).*member_pointer = value;
                            }else{
                                ((*temp).*(c.setter))(std::move(value));
                            }
                        });
                    }
                    
                public:
                    using value_type = T;
                    using difference_type = std::ptrdiff_t;
                    using pointer = value_type *;
                    using reference = value_type &;
                    using iterator_category = std::input_iterator_tag;
                    
                    iterator_t(sqlite3_stmt * stmt_, view_t<T, Args...> &view_): stmt(std::make_shared<sqlite3_stmt *>(stmt_)), view(view_) {
                        this->operator++();
                    }
                    
                    ~iterator_t() {
                        if(this->stmt){
                            statement_finalizer f{*this->stmt};
                        }
                    }
                    
                    T& operator*() {
                        if(!this->stmt) {
                            throw std::system_error(std::make_error_code(orm_error_code::trying_to_dereference_null_iterator));
                        }
                        if(!this->temp){
                            this->extract_value(this->temp);
                        }
                        return *this->temp;
                    }
                    
                    T* operator->() {
                        if(!this->stmt) {
                            throw std::system_error(std::make_error_code(orm_error_code::trying_to_dereference_null_iterator));
                        }
                        if(!this->temp){
                            this->extract_value(this->temp);
                        }
                        return &*this->temp;
                    }
                    
                    void operator++() {
                        if(this->stmt && *this->stmt){
                            auto ret = sqlite3_step(*this->stmt);
                            switch(ret){
                                case SQLITE_ROW:
                                    this->temp = nullptr;
                                    break;
                                case SQLITE_DONE:{
                                    statement_finalizer f{*this->stmt};
                                    *this->stmt = nullptr;
                                }break;
                                default:{
                                    throw std::system_error(std::error_code(sqlite3_errcode(this->view.connection->get_db()), get_sqlite_error_category()));
                                }
                            }
                        }
                    }
                    
                    void operator++(int) {
                        this->operator++();
                    }
                    
                    bool operator==(const iterator_t &other) const {
                        if(this->stmt && other.stmt){
                            return *this->stmt == *other.stmt;
                        }else{
                            if(!this->stmt && !other.stmt){
                                return true;
                            }else{
                                return false;
                            }
                        }
                    }
                    
                    bool operator!=(const iterator_t &other) const {
                        return !(*this == other);
                    }
                };
                
                size_t size() {
                    return this->storage.template count<T>();
                }
                
                bool empty() {
                    return !this->size();
                }
                
                iterator_t end() {
                    return {nullptr, *this};
                }
                
                iterator_t begin() {
                    sqlite3_stmt *stmt = nullptr;
                    auto db = this->connection->get_db();
                    auto ret = sqlite3_prepare_v2(db, this->query.c_str(), -1, &stmt, nullptr);
                    if(ret == SQLITE_OK){
                        return {stmt, *this};
                    }else{
                        throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                    }
                }
            };
            
            std::function<void(sqlite3*)> on_open;
            
            transaction_guard_t<storage_type> transaction_guard() {
                this->begin_transaction();
                return {*this};
            }
            
            struct pragma_t {
                
                pragma_t(storage_type &storage_): storage(storage_) {}
                
                int synchronous() {
                    return this->get_pragma<int>("synchronous");
                }
                
                void synchronous(int value) {
                    this->_synchronous = -1;
                    this->set_pragma("synchronous", value);
                    this->_synchronous = value;
                }
                
                int user_version() {
                    return this->get_pragma<int>("user_version");
                }
                
                void user_version(int value) {
                    this->set_pragma("user_version", value);
                }
                
                int auto_vacuum() {
                    return this->get_pragma<int>("auto_vacuum");
                }
                
                void auto_vacuum(int value) {
                    this->set_pragma("auto_vacuum", value);
                }
                
                friend struct storage_t<Ts...>;
                
            protected:
                storage_type &storage;
                int _synchronous = -1;
                
                template<class T>
                T get_pragma(const std::string &name) {
                    auto connection = this->storage.get_or_create_connection();
                    std::string query = "PRAGMA " + name;
                    int res = -1;
                    auto rc = sqlite3_exec(connection->get_db(),
                                           query.c_str(),
                                           [](void *data, int argc, char **argv, char **) -> int {
                                               auto &res = *(T*)data;
                                               if(argc){
                                                   res = row_extractor<T>().extract(argv[0]);
                                               }
                                               return 0;
                                           }, &res, nullptr);
                    if(rc != SQLITE_OK) {
                        throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                    }
                    return res;
                }
                
                template<class T>
                void set_pragma(const std::string &name, const T &value) {
                    auto connection = this->storage.get_or_create_connection();
                    std::stringstream ss;
                    ss << "PRAGMA " << name << " = " << this->storage.string_from_expression(value);
                    auto query = ss.str();
                    auto rc = sqlite3_exec(connection->get_db(), query.c_str(), nullptr, nullptr, nullptr);
                    if(rc != SQLITE_OK) {
                        throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                    }
                }
            };
            
            struct limit_accesor {
                
                int length() {
                    return this->get(SQLITE_LIMIT_LENGTH);
                }
                
                void length(int newValue) {
                    this->set(SQLITE_LIMIT_LENGTH, newValue);
                }
                
                int sql_length() {
                    return this->get(SQLITE_LIMIT_SQL_LENGTH);
                }
                
                void sql_length(int newValue) {
                    this->set(SQLITE_LIMIT_SQL_LENGTH, newValue);
                }
                
                int column() {
                    return this->get(SQLITE_LIMIT_COLUMN);
                }
                
                void column(int newValue) {
                    this->set(SQLITE_LIMIT_COLUMN, newValue);
                }
                
                int expr_depth() {
                    return this->get(SQLITE_LIMIT_EXPR_DEPTH);
                }
                
                void expr_depth(int newValue) {
                    this->set(SQLITE_LIMIT_EXPR_DEPTH, newValue);
                }
                
                int compound_select() {
                    return this->get(SQLITE_LIMIT_COMPOUND_SELECT);
                }
                
                void compound_select(int newValue) {
                    this->set(SQLITE_LIMIT_COMPOUND_SELECT, newValue);
                }
                
                int vdbe_op() {
                    return this->get(SQLITE_LIMIT_VDBE_OP);
                }
                
                void vdbe_op(int newValue) {
                    this->set(SQLITE_LIMIT_VDBE_OP, newValue);
                }
                
                int function_arg() {
                    return this->get(SQLITE_LIMIT_FUNCTION_ARG);
                }
                
                void function_arg(int newValue) {
                    this->set(SQLITE_LIMIT_FUNCTION_ARG, newValue);
                }
                
                int attached() {
                    return this->get(SQLITE_LIMIT_ATTACHED);
                }
                
                void attached(int newValue) {
                    this->set(SQLITE_LIMIT_ATTACHED, newValue);
                }
                
                int like_pattern_length() {
                    return this->get(SQLITE_LIMIT_LIKE_PATTERN_LENGTH);
                }
                
                void like_pattern_length(int newValue) {
                    this->set(SQLITE_LIMIT_LIKE_PATTERN_LENGTH, newValue);
                }
                
                int variable_number() {
                    return this->get(SQLITE_LIMIT_VARIABLE_NUMBER);
                }
                
                void variable_number(int newValue) {
                    this->set(SQLITE_LIMIT_VARIABLE_NUMBER, newValue);
                }
                
                int trigger_depth() {
                    return this->get(SQLITE_LIMIT_TRIGGER_DEPTH);
                }
                
                void trigger_depth(int newValue) {
                    this->set(SQLITE_LIMIT_TRIGGER_DEPTH, newValue);
                }
                
                int worker_threads() {
                    return this->get(SQLITE_LIMIT_WORKER_THREADS);
                }
                
                void worker_threads(int newValue) {
                    this->set(SQLITE_LIMIT_WORKER_THREADS, newValue);
                }
                
            protected:
                storage_type &storage;
                
                /**
                 *  Stores limit set between connections.
                 */
                std::map<int, int> limits;
                
                friend struct storage_t<Ts...>;
                
                limit_accesor(decltype(storage) storage_): storage(storage_) {}
                
                int get(int id) {
                    auto connection = this->storage.get_or_create_connection();
                    return sqlite3_limit(connection->get_db(), id, -1);
                }
                
                void set(int id, int newValue) {
                    this->limits[id] = newValue;
                    auto connection = this->storage.get_or_create_connection();
                    sqlite3_limit(connection->get_db(), id, newValue);
                }
            };
            
            /**
             *  @param filename_ database filename.
             */
            storage_t(const std::string &filename_, impl_type impl_):
            filename(filename_),
            impl(impl_),
            inMemory(filename_.empty() || filename_ == ":memory:"),
            pragma(*this),
            limit(*this){
                if(inMemory){
                    this->currentTransaction = std::make_shared<internal::database_connection>(this->filename);
                    this->on_open_internal(this->currentTransaction->get_db());
                }
            }
            
            storage_t(const storage_t &other):
            filename(other.filename),
            impl(other.impl),
            inMemory(other.inMemory),
            pragma(*this),
            limit(*this),
            collatingFunctions(other.collatingFunctions),
            currentTransaction(other.currentTransaction)
            {}
            
        protected:
            using collating_function = std::function<int(int, const void*, int, const void*)>;
            
            std::string filename;
            impl_type impl;
            std::shared_ptr<internal::database_connection> currentTransaction;
            const bool inMemory;
            bool isOpenedForever = false;
            std::map<std::string, collating_function> collatingFunctions;
            
            using collating_function_pair = typename decltype(collatingFunctions)::value_type;
            
            /**
             *  Check whether connection exists and returns it if yes or creates a new one
             *  and returns it.
             */
            std::shared_ptr<internal::database_connection> get_or_create_connection() {
                decltype(this->currentTransaction) connection;
                if(!this->currentTransaction){
                    connection = std::make_shared<internal::database_connection>(this->filename);
                    this->on_open_internal(connection->get_db());
                }else{
                    connection = this->currentTransaction;
                }
                return connection;
            }
            
            template<class O, class T, class G, class S, class ...Op>
            std::string serialize_column_schema(internal::column_t<O, T, G, S, Op...> c) {
                std::stringstream ss;
                ss << "'" << c.name << "' ";
                using field_type = typename decltype(c)::field_type;
                using constraints_type = typename decltype(c)::constraints_type;
                ss << type_printer<field_type>().print() << " ";
                tuple_helper::iterator<std::tuple_size<constraints_type>::value - 1, Op...>()(c.constraints, [&ss](auto &v){
                    ss << static_cast<std::string>(v) << ' ';
                });
                if(c.not_null()){
                    ss << "NOT NULL ";
                }
                return ss.str();
            }
            
            template<class ...Cs>
            std::string serialize_column_schema(constraints::primary_key_t<Cs...> fk) {
                std::stringstream ss;
                ss << static_cast<std::string>(fk) << " (";
                std::vector<std::string> columnNames;
                columnNames.reserve(std::tuple_size<decltype(fk.columns)>::value);
                tuple_helper::iterator<std::tuple_size<decltype(fk.columns)>::value - 1, Cs...>()(fk.columns, [&columnNames, this](auto &c){
                    columnNames.push_back(this->impl.column_name(c));
                });
                for(size_t i = 0; i < columnNames.size(); ++i) {
                    ss << columnNames[i];
                    if(i < columnNames.size() - 1) {
                        ss << ", ";
                    }
                }
                ss << ") ";
                return ss.str();
            }
            
#if SQLITE_VERSION_NUMBER >= 3006019
            
            template<class C, class R>
            std::string serialize_column_schema(constraints::foreign_key_t<C, R> &fk) {
                std::stringstream ss;
                using ref_type = typename internal::table_type<decltype(fk.r)>::type;
                auto refTableName = this->impl.template find_table_name<ref_type>();
                auto refColumnName = this->impl.column_name(fk.r);
                ss << "FOREIGN KEY(" << this->impl.column_name(fk.m) << ") REFERENCES ";
                ss << refTableName << "(" << refColumnName << ") ";
                return ss.str();
            }
#endif
            
            template<class I>
            void create_table(sqlite3 *db, const std::string &tableName, I *impl) {
                std::stringstream ss;
                ss << "CREATE TABLE '" << tableName << "' ( ";
                auto columnsCount = impl->table.columns_count();
                auto index = 0;
                impl->table.for_each_column_with_constraints([columnsCount, &index, &ss, this] (auto c) {
                    ss << this->serialize_column_schema(c);
                    if(index < columnsCount - 1) {
                        ss << ", ";
                    }
                    index++;
                });
                ss << ") ";
                if(impl->table._without_rowid) {
                    ss << "WITHOUT ROWID ";
                }
                auto query = ss.str();
                sqlite3_stmt *stmt;
                if (sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
                    statement_finalizer finalizer{stmt};
                    if (sqlite3_step(stmt) == SQLITE_DONE) {
                        //  done..
                    }else{
                        throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                    }
                }else {
                    throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                }
            }
            
            template<class I>
            void backup_table(sqlite3 *db, I *impl) {
                
                //  here we copy source table to another with a name with '_backup' suffix, but in case table with such
                //  a name already exists we append suffix 1, then 2, etc until we find a free name..
                auto backupTableName = impl->table.name + "_backup";
                if(impl->table_exists(backupTableName, db)){
                    int suffix = 1;
                    do{
                        std::stringstream stream;
                        stream << suffix;
                        auto anotherBackupTableName = backupTableName + stream.str();
                        if(!impl->table_exists(anotherBackupTableName, db)){
                            backupTableName = anotherBackupTableName;
                            break;
                        }
                        ++suffix;
                    }while(true);
                }
                
                this->create_table(db, backupTableName, impl);
                
                impl->copy_table(db, backupTableName);
                
                this->drop_table_internal(impl->table.name, db);
                
                impl->rename_table(db, backupTableName, impl->table.name);
            }
            
            template<class O>
            void assert_mapped_type() {
                using mapped_types_tuples = std::tuple<typename Ts::object_type...>;
                static_assert(tuple_helper::has_type<O, mapped_types_tuples>::value, "type is not mapped to a storage");
            }
            
            template<class O>
            auto& get_impl() {
                return this->impl.template get_impl<O>();
            }
            
            std::string escape(std::string text) {
                for(size_t i = 0; i < text.length(); ) {
                    if(text[i] == '\''){
                        text.insert(text.begin() + i, '\'');
                        i += 2;
                    }
                    else
                        ++i;
                }
                return text;
            }
            
            template<class T>
            std::string string_from_expression(T t, bool /*noTableName*/ = false, bool escape = false) {
                auto isNullable = type_is_nullable<T>::value;
                if(isNullable && !type_is_nullable<T>()(t)){
                    return "NULL";
                }else{
                    auto needQuotes = std::is_base_of<text_printer, type_printer<T>>::value;
                    std::stringstream ss;
                    if(needQuotes){
                        ss << "'";
                    }
                    std::string text = field_printer<T>()(t);
                    if(escape){
                        text = this->escape(text);
                    }
                    ss << text;
                    if(needQuotes){
                        ss << "'";
                    }
                    return ss.str();
                }
            }
            
            template<class T, class C>
            std::string string_from_expression(const alias_column_t<T, C> &als, bool noTableName = false, bool /*escape*/ = false) {
                std::stringstream ss;
                if(!noTableName){
                    ss << "'" << T::get() << "'.";
                }
                ss << this->string_from_expression(als.column, true);
                return ss.str();
            }
            
            std::string string_from_expression(const std::string &t, bool /*noTableName*/ = false, bool escape = false) {
                std::stringstream ss;
                std::string text = t;
                if(escape){
                    text = this->escape(text);
                }
                ss << "'" << text << "'";
                return ss.str();
            }
            
            std::string string_from_expression(const char *t, bool /*noTableName*/ = false, bool escape = false) {
                std::stringstream ss;
                std::string text = t;
                if(escape){
                    text = this->escape(text);
                }
                ss << "'" << text << "'";
                return ss.str();
            }
            
            template<class F, class O>
            std::string string_from_expression(F O::*m, bool noTableName = false, bool /*escape*/ = false) {
                std::stringstream ss;
                if(!noTableName){
                    ss << "'" << this->impl.template find_table_name<O>() << "'.";
                }
                ss << "\"" << this->impl.column_name(m) << "\"";
                return ss.str();
            }
            
            std::string string_from_expression(const rowid_t &rid, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                return static_cast<std::string>(rid);
            }
            
            std::string string_from_expression(const oid_t &rid, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                return static_cast<std::string>(rid);
            }
            
            std::string string_from_expression(const _rowid_t &rid, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                return static_cast<std::string>(rid);
            }
            
            template<class O>
            std::string string_from_expression(const table_rowid_t<O> &rid, bool noTableName = false, bool /*escape*/ = false) {
                std::stringstream ss;
                if(!noTableName){
                    ss << "'" << this->impl.template find_table_name<O>() << "'.";
                }
                ss << static_cast<std::string>(rid);
                return ss.str();
            }
            
            template<class O>
            std::string string_from_expression(const table_oid_t<O> &rid, bool noTableName = false, bool /*escape*/ = false) {
                std::stringstream ss;
                if(!noTableName){
                    ss << "'" << this->impl.template find_table_name<O>() << "'.";
                }
                ss << static_cast<std::string>(rid);
                return ss.str();
            }
            
            template<class O>
            std::string string_from_expression(const table__rowid_t<O> &rid, bool noTableName = false, bool /*escape*/ = false) {
                std::stringstream ss;
                if(!noTableName){
                    ss << "'" << this->impl.template find_table_name<O>() << "'.";
                }
                ss << static_cast<std::string>(rid);
                return ss.str();
            }
            
            template<class T>
            std::string string_from_expression(const aggregate_functions::group_concat_double_t<T> &f, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                auto expr = this->string_from_expression(f.t);
                auto expr2 = this->string_from_expression(f.y);
                ss << static_cast<std::string>(f) << "(" << expr << ", " << expr2 << ") ";
                return ss.str();
            }
            
            template<class T>
            std::string string_from_expression(const aggregate_functions::group_concat_single_t<T> &f, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                auto expr = this->string_from_expression(f.t);
                ss << static_cast<std::string>(f) << "(" << expr << ") ";
                return ss.str();
            }
            
            template<class L, class R>
            std::string string_from_expression(const conc_t<L, R> &f, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                auto lhs = this->string_from_expression(f.l);
                auto rhs = this->string_from_expression(f.r);
                ss << "(" << lhs << " || " << rhs << ") ";
                return ss.str();
            }
            
            template<class L, class R>
            std::string string_from_expression(const add_t<L, R> &f, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                auto lhs = this->string_from_expression(f.l);
                auto rhs = this->string_from_expression(f.r);
                ss << "(" << lhs << " + " << rhs << ") ";
                return ss.str();
            }
            
            template<class L, class R>
            std::string string_from_expression(const sub_t<L, R> &f, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                auto lhs = this->string_from_expression(f.l);
                auto rhs = this->string_from_expression(f.r);
                ss << "(" << lhs << " - " << rhs << ") ";
                return ss.str();
            }
            
            template<class L, class R>
            std::string string_from_expression(const mul_t<L, R> &f, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                auto lhs = this->string_from_expression(f.l);
                auto rhs = this->string_from_expression(f.r);
                ss << "(" << lhs << " * " << rhs << ") ";
                return ss.str();
            }
            
            template<class L, class R>
            std::string string_from_expression(const div_t<L, R> &f, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                auto lhs = this->string_from_expression(f.l);
                auto rhs = this->string_from_expression(f.r);
                ss << "(" << lhs << " / " << rhs << ") ";
                return ss.str();
            }
            
            template<class L, class R>
            std::string string_from_expression(const mod_t<L, R> &f, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                auto lhs = this->string_from_expression(f.l);
                auto rhs = this->string_from_expression(f.r);
                ss << "(" << lhs << " % " << rhs << ") ";
                return ss.str();
            }
            
            template<class T>
            std::string string_from_expression(const aggregate_functions::min_t<T> &f, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                auto expr = this->string_from_expression(f.t);
                ss << static_cast<std::string>(f) << "(" << expr << ") ";
                return ss.str();
            }
            
            template<class T>
            std::string string_from_expression(const aggregate_functions::max_t<T> &f, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                auto expr = this->string_from_expression(f.t);
                ss << static_cast<std::string>(f) << "(" << expr << ") ";
                return ss.str();
            }
            
            template<class T>
            std::string string_from_expression(const aggregate_functions::total_t<T> &f, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                auto expr = this->string_from_expression(f.t);
                ss << static_cast<std::string>(f) << "(" << expr << ") ";
                return ss.str();
            }
            
            template<class T>
            std::string string_from_expression(const aggregate_functions::sum_t<T> &f, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                auto expr = this->string_from_expression(f.t);
                ss << static_cast<std::string>(f) << "(" << expr << ") ";
                return ss.str();
            }
            
            std::string string_from_expression(const aggregate_functions::count_asterisk_t &f, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                ss << static_cast<std::string>(f) << "(*) ";
                return ss.str();
            }
            
            template<class T>
            std::string string_from_expression(const aggregate_functions::count_t<T> &f, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                auto expr = this->string_from_expression(f.t);
                ss << static_cast<std::string>(f) << "(" << expr << ") ";
                return ss.str();
            }
            
            template<class T>
            std::string string_from_expression(const aggregate_functions::avg_t<T> &a, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                auto expr = this->string_from_expression(a.t);
                ss << static_cast<std::string>(a) << "(" << expr << ") ";
                return ss.str();
            }
            
            template<class T>
            std::string string_from_expression(const distinct_t<T> &f, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                auto expr = this->string_from_expression(f.t);
                ss << static_cast<std::string>(f) << "(" << expr << ") ";
                return ss.str();
            }
            
            template<class T>
            std::string string_from_expression(const all_t<T> &f, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                auto expr = this->string_from_expression(f.t);
                ss << static_cast<std::string>(f) << "(" << expr << ") ";
                return ss.str();
            }
            
            template<class X, class Y>
            std::string string_from_expression(const core_functions::rtrim_double_t<X, Y> &f, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                auto expr = this->string_from_expression(f.x);
                auto expr2 = this->string_from_expression(f.y);
                ss << static_cast<std::string>(f) << "(" << expr << ", " << expr2 << ") ";
                return ss.str();
            }
            
            template<class X>
            std::string string_from_expression(const core_functions::rtrim_single_t<X> &f, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                auto expr = this->string_from_expression(f.x);
                ss << static_cast<std::string>(f) << "(" << expr << ") ";
                return ss.str();
            }
            
            template<class X, class Y>
            std::string string_from_expression(const core_functions::ltrim_double_t<X, Y> &f, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                auto expr = this->string_from_expression(f.x);
                auto expr2 = this->string_from_expression(f.y);
                ss << static_cast<std::string>(f) << "(" << expr << ", " << expr2 << ") ";
                return ss.str();
            }
            
            template<class X>
            std::string string_from_expression(const core_functions::ltrim_single_t<X> &f, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                auto expr = this->string_from_expression(f.x);
                ss << static_cast<std::string>(f) << "(" << expr << ") ";
                return ss.str();
            }
            
            template<class X, class Y>
            std::string string_from_expression(const core_functions::trim_double_t<X, Y> &f, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                auto expr = this->string_from_expression(f.x);
                auto expr2 = this->string_from_expression(f.y);
                ss << static_cast<std::string>(f) << "(" << expr << ", " << expr2 << ") ";
                return ss.str();
            }
            
            template<class X>
            std::string string_from_expression(const core_functions::trim_single_t<X> &f, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                auto expr = this->string_from_expression(f.x);
                ss << static_cast<std::string>(f) << "(" << expr << ") ";
                return ss.str();
            }
            
            std::string string_from_expression(const core_functions::changes_t &ch, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                ss << static_cast<std::string>(ch) << "() ";
                return ss.str();
            }
            
            template<class T>
            std::string string_from_expression(const core_functions::length_t<T> &len, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                auto expr = this->string_from_expression(len.t);
                ss << static_cast<std::string>(len) << "(" << expr << ") ";
                return ss.str();
            }
            
            template<class T, class ...Args>
            std::string string_from_expression(const core_functions::datetime_t<T, Args...> &f, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                ss << static_cast<std::string>(f) << "(" << this->string_from_expression(f.timestring);
                using tuple_t = std::tuple<Args...>;
                tuple_helper::iterator<std::tuple_size<tuple_t>::value - 1, Args...>()(f.modifiers, [&ss, this](auto &v){
                    ss << ", " << this->string_from_expression(v);
                });
                ss << ") ";
                return ss.str();
            }
            
            template<class T, class ...Args>
            std::string string_from_expression(const core_functions::date_t<T, Args...> &f, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                ss << static_cast<std::string>(f) << "(" << this->string_from_expression(f.timestring);
                using tuple_t = std::tuple<Args...>;
                tuple_helper::iterator<std::tuple_size<tuple_t>::value - 1, Args...>()(f.modifiers, [&ss, this](auto &v){
                    ss << ", " << this->string_from_expression(v);
                });
                ss << ") ";
                return ss.str();
            }
            
            std::string string_from_expression(const core_functions::random_t &f, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                ss << static_cast<std::string>(f) << "() ";
                return ss.str();
            }
            
#if SQLITE_VERSION_NUMBER >= 3007016
            
            template<class ...Args>
            std::string string_from_expression(const core_functions::char_t_<Args...> &f, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                using tuple_t = decltype(f.args);
                std::vector<std::string> args;
                args.reserve(std::tuple_size<tuple_t>::value);
                tuple_helper::tuple_for_each(f.args, [&args, this](auto &v){
                    auto expression = this->string_from_expression(v);
                    args.emplace_back(std::move(expression));
                });
                ss << static_cast<std::string>(f) << "(";
                auto lim = int(args.size());
                for(auto i = 0; i < lim; ++i) {
                    ss << args[i];
                    if(i < lim - 1) {
                        ss << ", ";
                    }else{
                        ss << " ";
                    }
                }
                ss << ") ";
                return ss.str();
            }
#endif
            
            template<class T>
            std::string string_from_expression(const core_functions::upper_t<T> &a, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                auto expr = this->string_from_expression(a.t);
                ss << static_cast<std::string>(a) << "(" << expr << ") ";
                return ss.str();
            }
            
            template<class T>
            std::string string_from_expression(const core_functions::lower_t<T> &a, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                auto expr = this->string_from_expression(a.t);
                ss << static_cast<std::string>(a) << "(" << expr << ") ";
                return ss.str();
            }
            
            template<class T>
            std::string string_from_expression(const core_functions::abs_t<T> &a, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                auto expr = this->string_from_expression(a.t);
                ss << static_cast<std::string>(a) << "(" << expr << ") ";
                return ss.str();
            }
            
            template<class T, class F>
            std::string string_from_expression(const column_pointer<T, F> &c, bool noTableName = false, bool escape = false) {
                std::stringstream ss;
                if(!noTableName){
                    ss << "'" << this->impl.template find_table_name<T>() << "'.";
                }
                auto &impl = this->get_impl<T>();
                ss << "\"" << impl.column_name_simple(c.field) << "\"";
                return ss.str();
            }
            
            template<class T>
            std::vector<std::string> get_column_names(const T &t) {
                auto columnName = this->string_from_expression(t);
                if(columnName.length()){
                    return {columnName};
                }else{
                    throw std::system_error(std::make_error_code(orm_error_code::column_not_found));
                }
            }
            
            template<class ...Args>
            std::vector<std::string> get_column_names(const internal::columns_t<Args...> &cols) {
                std::vector<std::string> columnNames;
                columnNames.reserve(cols.count());
                cols.for_each([&columnNames, this](auto &m) {
                    auto columnName = this->string_from_expression(m);
                    if(columnName.length()){
                        columnNames.push_back(columnName);
                    }else{
                        throw std::system_error(std::make_error_code(orm_error_code::column_not_found));
                    }
                });
                return columnNames;
            }
            
            /**
             *  Takes select_t object and returns SELECT query string
             */
            template<class T, class ...Args>
            std::string string_from_expression(const internal::select_t<T, Args...> &sel, bool /*noTableName*/ = false, bool /*escape*/ = false) {
                std::stringstream ss;
                ss << "SELECT ";
                if(get_distinct(sel.col)) {
                    ss << static_cast<std::string>(distinct(0)) << " ";
                }
                auto columnNames = this->get_column_names(sel.col);
                for(size_t i = 0; i < columnNames.size(); ++i) {
                    ss << columnNames[i];
                    if(i < columnNames.size() - 1) {
                        ss << ",";
                    }
                    ss << " ";
                }
                auto tableNamesSet = this->parse_table_names(sel.col);
                internal::join_iterator<Args...>()([&tableNamesSet, this](auto c){
                    using original_join_type = typename decltype(c)::type;
                    using cross_join_type = typename internal::mapped_type_proxy<original_join_type>::type;
                    auto crossJoinedTableName = this->impl.template find_table_name<cross_join_type>();
                    auto tableAliasString = alias_exractor<original_join_type>::get();
                    if(!tableAliasString.length()){
                        tableNamesSet.erase(crossJoinedTableName);
                    }
                });
                if(tableNamesSet.size()){
                    ss << "FROM ";
                    std::vector<std::string> tableNames(tableNamesSet.begin(), tableNamesSet.end());
                    for(size_t i = 0; i < tableNames.size(); ++i) {
                        ss << " '" << tableNames[i] << "' ";
                        if(int(i) < int(tableNames.size()) - 1) {
                            ss << ",";
                        }
                        ss << " ";
                    }
                }
                using tuple_t = typename std::decay<decltype(sel)>::type::conditions_type;
                tuple_helper::iterator<std::tuple_size<tuple_t>::value - 1, Args...>()(sel.conditions, [&ss, this](auto &v){
                    this->process_single_condition(ss, v);
                }, false);
                return ss.str();
            }
             
            template<class T>
            std::string process_where(const conditions::is_null_t<T> &c) {
                std::stringstream ss;
                ss << this->string_from_expression(c.t) << " " << static_cast<std::string>(c) << " ";
                return ss.str();
            }
            
            template<class T>
            std::string process_where(const conditions::is_not_null_t<T> &c) {
                std::stringstream ss;
                ss << this->string_from_expression(c.t) << " " << static_cast<std::string>(c) << " ";
                return ss.str();
            }
            
            template<class C>
            std::string process_where(const conditions::negated_condition_t<C> &c) {
                std::stringstream ss;
                ss << " " << static_cast<std::string>(c) << " ";
                auto cString = this->process_where(c.c);
                ss << " (" << cString << " ) ";
                return ss.str();
            }
            
            template<class L, class R>
            std::string process_where(const conditions::and_condition_t<L, R> &c) {
                std::stringstream ss;
                ss << " (" << this->process_where(c.l) << ") " << static_cast<std::string>(c) << " (" << this->process_where(c.r) << ") ";
                return ss.str();
            }
            
            template<class L, class R>
            std::string process_where(const conditions::or_condition_t<L, R> &c) {
                std::stringstream ss;
                ss << " (" << this->process_where(c.l) << ") " << static_cast<std::string>(c) << " (" << this->process_where(c.r) << ") ";
                return ss.str();
            }
            
            /**
             *  Common case. Is used to process binary conditions like is_equal, not_equal
             */
            template<class C>
            std::string process_where(const C &c) {
                auto leftString = this->string_from_expression(c.l, false, true);
                auto rightString = this->string_from_expression(c.r, false, true);
                std::stringstream ss;
                ss << leftString << " " << static_cast<std::string>(c) << " " << rightString;
                return ss.str();
            }
            
            template<class T>
            std::string process_where(const conditions::named_collate<T> &col) {
                auto res = this->process_where(col.expr);
                return res + " " + static_cast<std::string>(col);
            }
            
            template<class T>
            std::string process_where(const conditions::collate_t<T> &col) {
                auto res = this->process_where(col.expr);
                return res + " " + static_cast<std::string>(col);
            }
            
            template<class L, class E>
            std::string process_where(const conditions::in_t<L, E> &inCondition) {
                std::stringstream ss;
                auto leftString = this->string_from_expression(inCondition.l);
                ss << leftString << " " << static_cast<std::string>(inCondition) << " (";
                for(size_t index = 0; index < inCondition.values.size(); ++index) {
                    auto &value = inCondition.values[index];
                    ss << " " << this->string_from_expression(value);
                    if(index < inCondition.values.size() - 1) {
                        ss << ", ";
                    }
                }
                ss << " )";
                return ss.str();
            }
            
            template<class A, class T>
            std::string process_where(const conditions::like_t<A, T> &l) {
                std::stringstream ss;
                ss << this->string_from_expression(l.a) << " " << static_cast<std::string>(l) << " " << this->string_from_expression(l.t) << " ";
                return ss.str();
            }
            
            template<class A, class T>
            std::string process_where(const conditions::between_t<A, T> &bw) {
                std::stringstream ss;
                auto expr = this->string_from_expression(bw.expr);
                ss << expr << " " << static_cast<std::string>(bw) << " " << this->string_from_expression(bw.b1) << " AND " << this->string_from_expression(bw.b2) << " ";
                return ss.str();
            }
            
            template<class O>
            std::string process_order_by(const conditions::order_by_t<O> &orderBy) {
                std::stringstream ss;
                auto columnName = this->string_from_expression(orderBy.o);
                ss << columnName << " ";
                if(orderBy._collate_argument.length()){
                    ss << "COLLATE " << orderBy._collate_argument << " ";
                }
                switch(orderBy.asc_desc){
                    case 1:
                        ss << "ASC ";
                        break;
                    case -1:
                        ss << "DESC ";
                        break;
                }
                return ss.str();
            }
            
            template<class T>
            void process_join_constraint(std::stringstream &ss, const conditions::on_t<T> &t) {
                ss << static_cast<std::string>(t) << " " << this->process_where(t.t) << " ";
            }
            
            template<class F, class O>
            void process_join_constraint(std::stringstream &ss, const conditions::using_t<F, O> &u) {
                ss << static_cast<std::string>(u) << " (" << this->string_from_expression(u.column, true) << " ) ";
            }
            
            void process_single_condition(std::stringstream &ss, const conditions::limit_t &limt) {
                ss << static_cast<std::string>(limt) << " ";
                if(limt.has_offset) {
                    if(limt.offset_is_implicit){
                        ss << limt.off << ", " << limt.lim;
                    }else{
                        ss << limt.lim << " OFFSET " << limt.off;
                    }
                }else{
                    ss << limt.lim;
                }
                ss << " ";
            }
            
            template<class O>
            void process_single_condition(std::stringstream &ss, const conditions::cross_join_t<O> &c) {
                ss << static_cast<std::string>(c) << " ";
                ss << " '" << this->impl.template find_table_name<O>() << "' ";
            }
            
            template<class O>
            void process_single_condition(std::stringstream &ss, const conditions::natural_join_t<O> &c) {
                ss << static_cast<std::string>(c) << " ";
                ss << " '" << this->impl.template find_table_name<O>() << "' ";
            }
            
            template<class T, class O>
            void process_single_condition(std::stringstream &ss, const conditions::inner_join_t<T, O> &l) {
                ss << static_cast<std::string>(l) << " ";
                auto aliasString = alias_exractor<T>::get();
                ss << " '" << this->impl.template find_table_name<typename mapped_type_proxy<T>::type>() << "' ";
                if(aliasString.length()){
                    ss << "'" << aliasString << "' ";
                }
                this->process_join_constraint(ss, l.constraint);
            }
            
            template<class T, class O>
            void process_single_condition(std::stringstream &ss, const conditions::left_outer_join_t<T, O> &l) {
                ss << static_cast<std::string>(l) << " ";
                ss << " '" << this->impl.template find_table_name<T>() << "' ";
                this->process_join_constraint(ss, l.constraint);
            }
            
            template<class T, class O>
            void process_single_condition(std::stringstream &ss, const conditions::left_join_t<T, O> &l) {
                ss << static_cast<std::string>(l) << " ";
                ss << " '" << this->impl.template find_table_name<T>() << "' ";
                this->process_join_constraint(ss, l.constraint);
            }
            
            template<class T, class O>
            void process_single_condition(std::stringstream &ss, const conditions::join_t<T, O> &l) {
                ss << static_cast<std::string>(l) << " ";
                ss << " '" << this->impl.template find_table_name<T>() << "' ";
                this->process_join_constraint(ss, l.constraint);
            }
            
            template<class C>
            void process_single_condition(std::stringstream &ss, const conditions::where_t<C> &w) {
                ss << static_cast<std::string>(w) << " ";
                auto whereString = this->process_where(w.c);
                ss << "( " << whereString << ") ";
            }
            
            template<class O>
            void process_single_condition(std::stringstream &ss, const conditions::order_by_t<O> &orderBy) {
                ss << static_cast<std::string>(orderBy) << " ";
                auto orderByString = this->process_order_by(orderBy);
                ss << orderByString << " ";
            }
            
            template<class ...Args>
            void process_single_condition(std::stringstream &ss, const conditions::multi_order_by_t<Args...> &orderBy) {
                std::vector<std::string> expressions;
                using tuple_t = std::tuple<Args...>;
                tuple_helper::iterator<std::tuple_size<tuple_t>::value - 1, Args...>()(orderBy.args, [&expressions, this](auto &v){
                    auto expression = this->process_order_by(v);
                    expressions.insert(expressions.begin(), expression);
                });
                ss << static_cast<std::string>(orderBy) << " ";
                for(size_t i = 0; i < expressions.size(); ++i) {
                    ss << expressions[i];
                    if(i < expressions.size() - 1) {
                        ss << ", ";
                    }
                }
                ss << " ";
            }
            
            template<class ...Args>
            void process_single_condition(std::stringstream &ss, const conditions::group_by_t<Args...> &groupBy) {
                std::vector<std::string> expressions;
                using tuple_t = std::tuple<Args...>;
                tuple_helper::iterator<std::tuple_size<tuple_t>::value - 1, Args...>()(groupBy.args, [&expressions, this](auto &v){
                    auto expression = this->string_from_expression(v);
                    expressions.push_back(expression);
                });
                ss << static_cast<std::string>(groupBy) << " ";
                for(size_t i = 0; i < expressions.size(); ++i) {
                    ss << expressions[i];
                    if(i < expressions.size() - 1) {
                        ss << ", ";
                    }
                }
                ss << " ";
            }
            
            /**
             *  Recursion end.
             */
            template<class ...Args>
            void process_conditions(std::stringstream &, Args .../*args*/) {
                //..
            }
            
            template<class C, class ...Args>
            void process_conditions(std::stringstream &ss, C c, Args&& ...args) {
                this->process_single_condition(ss, c);
                this->process_conditions(ss, std::forward<Args>(args)...);
            }
            
            void on_open_internal(sqlite3 *db) {
                
#if SQLITE_VERSION_NUMBER >= 3006019
                if(this->foreign_keys_count()){
                    this->foreign_keys(db, true);
                }
#endif
                if(this->pragma._synchronous != -1) {
                    this->pragma.synchronous(this->pragma._synchronous);
                }
                
                for(auto &p : this->collatingFunctions){
                    if(sqlite3_create_collation(db,
                                                p.first.c_str(),
                                                SQLITE_UTF8,
                                                &p.second,
                                                collate_callback) != SQLITE_OK)
                    {
                        throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                    }
                }
                
                for(auto &p : this->limit.limits) {
                    sqlite3_limit(db, p.first, p.second);
                }
                
                if(this->on_open){
                    this->on_open(db);
                }
                
            }
            
#if SQLITE_VERSION_NUMBER >= 3006019
            
            //  returns foreign keys count in storage definition
            int foreign_keys_count() {
                auto res = 0;
                this->impl.for_each([&res](auto impl){
                    res += impl->foreign_keys_count();
                });
                return res;
            }
#endif
            static int collate_callback(void *arg, int leftLen, const void *lhs, int rightLen, const void *rhs) {
                auto &f = *(collating_function*)arg;
                return f(leftLen, lhs, rightLen, rhs);
            }
            
        public:
            
            template<class T, class ...Args>
            view_t<T, Args...> iterate(Args&& ...args) {
                this->assert_mapped_type<T>();
                
                auto connection = this->get_or_create_connection();
                return {*this, connection, std::forward<Args>(args)...};
            }
            
            void create_collation(const std::string &name, collating_function f) {
                collating_function *functionPointer = nullptr;
                if(f){
                    functionPointer = &(collatingFunctions[name] = f);
                }else{
                    collatingFunctions.erase(name);
                }
                
                //  create collations if db is open
                if(this->currentTransaction){
                    auto db = this->currentTransaction->get_db();
                    if(sqlite3_create_collation(db,
                                                name.c_str(),
                                                SQLITE_UTF8,
                                                functionPointer,
                                                f ? collate_callback : nullptr) != SQLITE_OK)
                    {
                        throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                    }
                }
            }
            
            template<class O, class ...Args>
            void remove_all(Args&& ...args) {
                this->assert_mapped_type<O>();
                
                auto connection = this->get_or_create_connection();
                auto &impl = this->get_impl<O>();
                std::stringstream ss;
                ss << "DELETE FROM '" << impl.table.name << "' ";
                this->process_conditions(ss, std::forward<Args>(args)...);
                auto query = ss.str();
                sqlite3_stmt *stmt;
                if (sqlite3_prepare_v2(connection->get_db(), query.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
                    statement_finalizer finalizer{stmt};
                    if (sqlite3_step(stmt) == SQLITE_DONE) {
                        //  done..
                    }else{
                        throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                    }
                }else {
                    throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                }
            }
            
            /**
             *  Delete routine.
             *  O is an object's type. Must be specified explicitly.
             *  @param id id of object to be removed.
             */
            template<class O, class I>
            void remove(I id) {
                this->assert_mapped_type<O>();
                
                auto connection = this->get_or_create_connection();
                auto &impl = this->get_impl<O>();
                std::stringstream ss;
                ss << "DELETE FROM '" << impl.table.name << "' ";
                ss << "WHERE ";
                auto primaryKeyColumnNames = impl.table.primary_key_column_names();
                for(size_t i = 0; i < primaryKeyColumnNames.size(); ++i) {
                    ss << "\"" << primaryKeyColumnNames[i] << "\"" << " =  ?";
                    if(i < primaryKeyColumnNames.size() - 1) {
                        ss << " AND ";
                    }else{
                        ss << " ";
                    }
                }
                auto query = ss.str();
                sqlite3_stmt *stmt;
                if (sqlite3_prepare_v2(connection->get_db(), query.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
                    statement_finalizer finalizer{stmt};
                    auto index = 1;
                    statement_binder<I>().bind(stmt, index++, id);
                    if (sqlite3_step(stmt) == SQLITE_DONE) {
                        //  done..
                    }else{
                        throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                    }
                }else{
                    throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                }
            }
            
            /**
             *  Update routine. Sets all non primary key fields where primary key is equal.
             *  O is an object type. May be not specified explicitly cause it can be deduced by
             *      compiler from first parameter.
             *  @param o object to be updated.
             */
            template<class O>
            void update(const O &o) {
                this->assert_mapped_type<O>();
                
                auto connection = this->get_or_create_connection();
                auto &impl = this->get_impl<O>();
                std::stringstream ss;
                ss << "UPDATE '" << impl.table.name << "' SET ";
                std::vector<std::string> setColumnNames;
                impl.table.for_each_column([&setColumnNames](auto c) {
                    if(!c.template has<constraints::primary_key_t<>>()) {
                        setColumnNames.emplace_back(c.name);
                    }
                });
                for(size_t i = 0; i < setColumnNames.size(); ++i) {
                    ss << "\"" << setColumnNames[i] << "\"" << " = ?";
                    if(i < setColumnNames.size() - 1) {
                        ss << ", ";
                    }else{
                        ss << " ";
                    }
                }
                ss << "WHERE ";
                auto primaryKeyColumnNames = impl.table.primary_key_column_names();
                for(size_t i = 0; i < primaryKeyColumnNames.size(); ++i) {
                    ss << "\"" << primaryKeyColumnNames[i] << "\"" << " = ?";
                    if(i < primaryKeyColumnNames.size() - 1) {
                        ss << " AND ";
                    }else{
                        ss << " ";
                    }
                }
                auto query = ss.str();
                sqlite3_stmt *stmt;
                if (sqlite3_prepare_v2(connection->get_db(), query.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
                    statement_finalizer finalizer{stmt};
                    auto index = 1;
                    impl.table.for_each_column([&o, stmt, &index] (auto c) {
                        if(!c.template has<constraints::primary_key_t<>>()) {
                            using field_type = typename decltype(c)::field_type;
                            const field_type *value = nullptr;
                            if(c.member_pointer){
                                value = &(o.*c.member_pointer);
                            }else{
                                value = &((o).*(c.getter))();
                            }
                            statement_binder<field_type>().bind(stmt, index++, *value);
                        }
                    });
                    impl.table.for_each_column([&o, stmt, &index] (auto c) {
                        if(c.template has<constraints::primary_key_t<>>()) {
                            typedef typename decltype(c)::field_type field_type;
                            const field_type *value = nullptr;
                            if(c.member_pointer){
                                value = &(o.*c.member_pointer);
                            }else{
                                value = &((o).*(c.getter))();
                            }
                            statement_binder<field_type>().bind(stmt, index++, *value);
                        }
                    });
                    if (sqlite3_step(stmt) == SQLITE_DONE) {
                        //  done..
                    }else{
                        throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                    }
                }else {
                    throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                }
            }
            
            template<class ...Args, class ...Wargs>
            void update_all(internal::set_t<Args...> set, Wargs ...wh) {
                auto connection = this->get_or_create_connection();
                
                std::stringstream ss;
                ss << "UPDATE ";
                std::set<std::string> tableNamesSet;
                set.for_each([this, &tableNamesSet](auto &asgn) {
                    auto tableName = this->parse_table_name(asgn.l);
                    tableNamesSet.insert(tableName.begin(), tableName.end());
                });
                if(tableNamesSet.size()){
                    if(tableNamesSet.size() == 1){
                        ss << " '" << *tableNamesSet.begin() << "' ";
                        ss << static_cast<std::string>(set) << " ";
                        std::vector<std::string> setPairs;
                        set.for_each([this, &setPairs](auto &asgn){
                            std::stringstream sss;
                            sss << this->string_from_expression(asgn.l, true) << " = " << this->string_from_expression(asgn.r) << " ";
                            setPairs.push_back(sss.str());
                        });
                        auto setPairsCount = setPairs.size();
                        for(size_t i = 0; i < setPairsCount; ++i) {
                            ss << setPairs[i] << " ";
                            if(i < setPairsCount - 1) {
                                ss << ", ";
                            }
                        }
                        this->process_conditions(ss, wh...);
                        auto query = ss.str();
                        sqlite3_stmt *stmt;
                        if (sqlite3_prepare_v2(connection->get_db(), query.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
                            statement_finalizer finalizer{stmt};
                            if (sqlite3_step(stmt) == SQLITE_DONE) {
                                //  done..
                            }else{
                                throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                            }
                        }else{
                            throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                        }
                    }else{
                        throw std::system_error(std::make_error_code(orm_error_code::too_many_tables_specified));
                    }
                }else{
                    throw std::system_error(std::make_error_code(orm_error_code::incorrect_set_fields_specified));
                }
            }
            
        protected:
            
            /**
             *  O - mapped type
             *  Args - conditions
             *  @param query - result query string
             *  @return impl for O
             */
            template<class O, class ...Args>
            auto& generate_select_asterisk(std::string *query, Args&& ...args) {
                std::stringstream ss;
                ss << "SELECT ";
                auto &impl = this->get_impl<O>();
                auto columnNames = impl.table.column_names();
                for(size_t i = 0; i < columnNames.size(); ++i) {
                    ss
                    << "'" << impl.table.name << "'."
                    << "\""
                    << columnNames[i]
                    << "\""
                    ;
                    if(i < columnNames.size() - 1) {
                        ss << ", ";
                    }else{
                        ss << " ";
                    }
                }
                ss << "FROM '" << impl.table.name << "' ";
                this->process_conditions(ss, std::forward<Args>(args)...);
                if(query){
                    *query = ss.str();
                }
                return impl;
            }
            
            template<class T>
            std::set<std::string> parse_table_name(const T &) {
                return {};
            }
            
            template<class F, class O>
            std::set<std::string> parse_table_name(F O::*) {
                return {this->impl.template find_table_name<O>()};
            }
            
            template<class T>
            std::set<std::string> parse_table_name(const aggregate_functions::min_t<T> &f) {
                return this->parse_table_name(f.t);
            }
            
            template<class T>
            std::set<std::string> parse_table_name(const aggregate_functions::max_t<T> &f) {
                return this->parse_table_name(f.t);
            }
            
            template<class T>
            std::set<std::string> parse_table_name(const aggregate_functions::sum_t<T> &f) {
                return this->parse_table_name(f.t);
            }
            
            template<class T>
            std::set<std::string> parse_table_name(const aggregate_functions::total_t<T> &f) {
                return this->parse_table_name(f.t);
            }
            
            template<class T>
            std::set<std::string> parse_table_name(const aggregate_functions::group_concat_double_t<T> &f) {
                auto res = this->parse_table_name(f.t);
                auto secondSet = this->parse_table_name(f.y);
                res.insert(secondSet.begin(), secondSet.end());
                return res;
            }
            
            template<class T>
            std::set<std::string> parse_table_name(const aggregate_functions::group_concat_single_t<T> &f) {
                return this->parse_table_name(f.t);
            }
            
            template<class T>
            std::set<std::string> parse_table_name(const aggregate_functions::count_t<T> &f) {
                return this->parse_table_name(f.t);
            }
            
            template<class T>
            std::set<std::string> parse_table_name(const aggregate_functions::avg_t<T> &a) {
                return this->parse_table_name(a.t);
            }
            
            template<class T>
            std::set<std::string> parse_table_name(const core_functions::length_t<T> &len) {
                return this->parse_table_name(len.t);
            }
            
            template<class T, class ...Args>
            std::set<std::string> parse_table_name(const core_functions::date_t<T, Args...> &f) {
                auto res = this->parse_table_name(f.timestring);
                using tuple_t = decltype(f.modifiers);
                tuple_helper::iterator<std::tuple_size<tuple_t>::value - 1, Args...>()(f.modifiers, [&res, this](auto &v){
                    auto tableNames = this->parse_table_name(v);
                    res.insert(tableNames.begin(), tableNames.end());
                });
                return res;
            }
            
            template<class T, class ...Args>
            std::set<std::string> parse_table_name(const core_functions::datetime_t<T, Args...> &f) {
                auto res = this->parse_table_name(f.timestring);
                using tuple_t = decltype(f.modifiers);
                tuple_helper::iterator<std::tuple_size<tuple_t>::value - 1, Args...>()(f.modifiers, [&res, this](auto &v){
                    auto tableNames = this->parse_table_name(v);
                    res.insert(tableNames.begin(), tableNames.end());
                });
                return res;
            }
            
            template<class X>
            std::set<std::string> parse_table_name(const core_functions::trim_single_t<X> &f) {
                return this->parse_table_name(f.x);
            }
            
            template<class X, class Y>
            std::set<std::string> parse_table_name(const core_functions::trim_double_t<X, Y> &f) {
                auto res = this->parse_table_name(f.x);
                auto res2 = this->parse_table_name(f.y);
                res.insert(res2.begin(), res2.end());
                return res;
            }
            
            template<class X>
            std::set<std::string> parse_table_name(const core_functions::rtrim_single_t<X> &f) {
                return this->parse_table_name(f.x);
            }
            
            template<class X, class Y>
            std::set<std::string> parse_table_name(const core_functions::rtrim_double_t<X, Y> &f) {
                auto res = this->parse_table_name(f.x);
                auto res2 = this->parse_table_name(f.y);
                res.insert(res2.begin(), res2.end());
                return res;
            }
            
            template<class X>
            std::set<std::string> parse_table_name(const core_functions::ltrim_single_t<X> &f) {
                return this->parse_table_name(f.x);
            }
            
            template<class X, class Y>
            std::set<std::string> parse_table_name(const core_functions::ltrim_double_t<X, Y> &f) {
                auto res = this->parse_table_name(f.x);
                auto res2 = this->parse_table_name(f.y);
                res.insert(res2.begin(), res2.end());
                return res;
            }
            
#if SQLITE_VERSION_NUMBER >= 3007016
            
            template<class ...Args>
            std::set<std::string> parse_table_name(const core_functions::char_t_<Args...> &f) {
                std::set<std::string> res;
                using tuple_t = decltype(f.args);
                tuple_helper::iterator<std::tuple_size<tuple_t>::value - 1, Args...>()(f.args, [&res, this](auto &v){
                    auto tableNames = this->parse_table_name(v);
                    res.insert(tableNames.begin(), tableNames.end());
                });
                return res;
            }
            
#endif
            
            std::set<std::string> parse_table_name(const core_functions::random_t &) {
                return {};
            }
            
            template<class T>
            std::set<std::string> parse_table_name(const core_functions::upper_t<T> &a) {
                return this->parse_table_name(a.t);
            }
            
            template<class T>
            std::set<std::string> parse_table_name(const core_functions::lower_t<T> &a) {
                return this->parse_table_name(a.t);
            }
            
            template<class T>
            std::set<std::string> parse_table_name(const core_functions::abs_t<T> &a) {
                return this->parse_table_name(a.t);
            }
            
            template<class T>
            std::set<std::string> parse_table_name(const distinct_t<T> &f) {
                return this->parse_table_name(f.t);
            }
            
            template<class T>
            std::set<std::string> parse_table_name(const all_t<T> &f) {
                return this->parse_table_name(f.t);
            }
            
            template<class L, class R, class ...Args>
            std::set<std::string> parse_table_name(const conc_t<L, R> &f) {
                std::set<std::string> res;
                auto leftSet = this->parse_table_names(f.l);
                res.insert(leftSet.begin(), leftSet.end());
                auto rightSet = this->parse_table_names(f.r);
                res.insert(rightSet.begin(), rightSet.end());
                return res;
            }
            
            template<class L, class R, class ...Args>
            std::set<std::string> parse_table_name(const add_t<L, R> &f) {
                std::set<std::string> res;
                auto leftSet = this->parse_table_names(f.l);
                res.insert(leftSet.begin(), leftSet.end());
                auto rightSet = this->parse_table_names(f.r);
                res.insert(rightSet.begin(), rightSet.end());
                return res;
            }
            
            template<class L, class R, class ...Args>
            std::set<std::string> parse_table_name(const sub_t<L, R> &f) {
                std::set<std::string> res;
                auto leftSet = this->parse_table_names(f.l);
                res.insert(leftSet.begin(), leftSet.end());
                auto rightSet = this->parse_table_names(f.r);
                res.insert(rightSet.begin(), rightSet.end());
                return res;
            }
            
            template<class L, class R, class ...Args>
            std::set<std::string> parse_table_name(const mul_t<L, R> &f) {
                std::set<std::string> res;
                auto leftSet = this->parse_table_names(f.l);
                res.insert(leftSet.begin(), leftSet.end());
                auto rightSet = this->parse_table_names(f.r);
                res.insert(rightSet.begin(), rightSet.end());
                return res;
            }
            
            template<class L, class R, class ...Args>
            std::set<std::string> parse_table_name(const div_t<L, R> &f) {
                std::set<std::string> res;
                auto leftSet = this->parse_table_names(f.l);
                res.insert(leftSet.begin(), leftSet.end());
                auto rightSet = this->parse_table_names(f.r);
                res.insert(rightSet.begin(), rightSet.end());
                return res;
            }
            
            template<class L, class R, class ...Args>
            std::set<std::string> parse_table_name(const mod_t<L, R> &f) {
                std::set<std::string> res;
                auto leftSet = this->parse_table_names(f.l);
                res.insert(leftSet.begin(), leftSet.end());
                auto rightSet = this->parse_table_names(f.r);
                res.insert(rightSet.begin(), rightSet.end());
                return res;
            }
            
            template<class T, class F>
            std::set<std::string> parse_table_name(const column_pointer<T, F> &c) {
                std::set<std::string> res;
                res.insert(this->impl.template find_table_name<T>());
                return res;
            }
            
            template<class ...Args>
            std::set<std::string> parse_table_names(Args...) {
                return {};
            }
            
            template<class H, class ...Args>
            std::set<std::string> parse_table_names(H h, Args&& ...args) {
                auto res = this->parse_table_names(std::forward<Args>(args)...);
                auto tableName = this->parse_table_name(h);
                res.insert(tableName.begin(), tableName.end());
                return res;
            }
            
            template<class ...Args>
            std::set<std::string> parse_table_names(const internal::columns_t<Args...> &cols) {
                std::set<std::string> res;
                cols.for_each([&res, this](auto &m){
                    auto tableName = this->parse_table_name(m);
                    res.insert(tableName.begin(), tableName.end());
                });
                return res;
            }
            
            template<class F, class O, class ...Args>
            std::string group_concat_internal(F O::*m, std::shared_ptr<const std::string> y, Args&& ...args) {
                this->assert_mapped_type<O>();
                
                auto connection = this->get_or_create_connection();
                auto &impl = this->get_impl<O>();
                std::string res;
                std::stringstream ss;
                ss << "SELECT " << static_cast<std::string>(sqlite_orm::group_concat(0)) << "(";
                auto columnName = this->string_from_expression(m);
                if(columnName.length()){
                    ss << columnName;
                    if(y){
                        ss << ",\"" << *y << "\"";
                    }
                    ss << ") FROM '"<< impl.table.name << "' ";
                    this->process_conditions(ss, std::forward<Args>(args)...);
                    auto query = ss.str();
                    auto rc = sqlite3_exec(connection->get_db(),
                                           query.c_str(),
                                           [](void *data, int argc, char **argv,char **) -> int {
                                               auto &res = *(std::string*)data;
                                               if(argc){
                                                   res = row_extractor<std::string>().extract(argv[0]);
                                               }
                                               return 0;
                                           }, &res, nullptr);
                    if(rc != SQLITE_OK) {
                        throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                    }
                }else{
                    throw std::system_error(std::make_error_code(orm_error_code::column_not_found));
                }
                return res;
            }
            
        public:
            
            /**
             *  Select * with no conditions routine.
             *  O is an object type to be extracted. Must be specified explicitly.
             *  @return All objects of type O stored in database at the moment.
             */
            template<class O, class C = std::vector<O>, class ...Args>
            C get_all(Args&& ...args) {
                this->assert_mapped_type<O>();
                
                auto connection = this->get_or_create_connection();
                C res;
                std::string query;
                auto &impl = this->generate_select_asterisk<O>(&query, std::forward<Args>(args)...);
                sqlite3_stmt *stmt;
                if (sqlite3_prepare_v2(connection->get_db(), query.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
                    statement_finalizer finalizer{stmt};
                    int stepRes;
                    do{
                        stepRes = sqlite3_step(stmt);
                        switch(stepRes){
                            case SQLITE_ROW:{
                                O obj;
                                auto index = 0;
                                impl.table.for_each_column([&index, &obj, stmt] (auto c) {
                                    using field_type = typename decltype(c)::field_type;
                                    auto value = row_extractor<field_type>().extract(stmt, index++);
                                    if(c.member_pointer){
                                        obj.*c.member_pointer = value;
                                    }else{
                                        ((obj).*(c.setter))(std::move(value));
                                    }
                                });
                                res.push_back(std::move(obj));
                            }break;
                            case SQLITE_DONE: break;
                            default:{
                                throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                            }
                        }
                    }while(stepRes != SQLITE_DONE);
                    return res;
                }else{
                    throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                }
            }
            
            /**
             *  Select * by id routine.
             *  throws std::system_error(orm_error_code::not_found, orm_error_category) if object not found with given id.
             *  throws std::system_error with orm_error_category in case of db error.
             *  O is an object type to be extracted. Must be specified explicitly.
             *  @return Object of type O where id is equal parameter passed or throws `std::system_error(orm_error_code::not_found, orm_error_category)`
             *  if there is no object with such id.
             */
            template<class O, class ...Ids>
            O get(Ids ...ids) {
                this->assert_mapped_type<O>();
                
                auto connection = this->get_or_create_connection();
                auto &impl = this->get_impl<O>();
                std::shared_ptr<O> res;
                std::stringstream ss;
                ss << "SELECT ";
                auto columnNames = impl.table.column_names();
                for(size_t i = 0; i < columnNames.size(); ++i) {
                    ss << "\"" << columnNames[i] << "\"";
                    if(i < columnNames.size() - 1) {
                        ss << ", ";
                    }else{
                        ss << " ";
                    }
                }
                ss << "FROM '" << impl.table.name << "' WHERE ";
                auto primaryKeyColumnNames = impl.table.primary_key_column_names();
                if(primaryKeyColumnNames.size()){
                    for(size_t i = 0; i < primaryKeyColumnNames.size(); ++i) {
                        ss << "\"" << primaryKeyColumnNames[i] << "\"" << " = ? ";
                        if(i < primaryKeyColumnNames.size() - 1) {
                            ss << "AND ";
                        }
                        ss << ' ';
                    }
                    auto query = ss.str();
                    sqlite3_stmt *stmt;
                    if (sqlite3_prepare_v2(connection->get_db(), query.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
                        statement_finalizer finalizer{stmt};
                        auto index = 1;
                        auto idsTuple = std::make_tuple(std::forward<Ids>(ids)...);
                        constexpr const auto idsCount = std::tuple_size<decltype(idsTuple)>::value;
                        tuple_helper::iterator<idsCount - 1, Ids...>()(idsTuple, [stmt, &index](auto &v){
                            using field_type = typename std::decay<decltype(v)>::type;
                            statement_binder<field_type>().bind(stmt, index++, v);
                        });
                        auto stepRes = sqlite3_step(stmt);
                        switch(stepRes){
                            case SQLITE_ROW:{
                                O res;
                                index = 0;
                                impl.table.for_each_column([&index, &res, stmt] (auto c) {
                                    using field_type = typename decltype(c)::field_type;
                                    auto value = row_extractor<field_type>().extract(stmt, index++);
                                    if(c.member_pointer){
                                        res.*c.member_pointer = value;
                                    }else{
                                        ((res).*(c.setter))(std::move(value));
                                    }
                                });
                                return res;
                            }break;
                            case SQLITE_DONE:{
                                throw std::system_error(std::make_error_code(sqlite_orm::orm_error_code::not_found));
                            }break;
                            default:{
                                throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                            }
                        }
                    }else{
                        throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                    }
                }else{
                    throw std::system_error(std::make_error_code(orm_error_code::table_has_no_primary_key_column));
                }
            }
            
            /**
             *  The same as `get` function but doesn't throw an exception if noting found but returns std::shared_ptr with null value.
             *  throws std::system_error in case of db error.
             */
            template<class O, class ...Ids>
            std::shared_ptr<O> get_no_throw(Ids ...ids) {
                this->assert_mapped_type<O>();
                
                auto connection = this->get_or_create_connection();
                auto &impl = this->get_impl<O>();
                std::shared_ptr<O> res;
                std::stringstream ss;
                ss << "SELECT ";
                auto columnNames = impl.table.column_names();
                for(size_t i = 0; i < columnNames.size(); ++i) {
                    ss << "\"" << columnNames[i] << "\"";
                    if(i < columnNames.size() - 1) {
                        ss << ", ";
                    }else{
                        ss << " ";
                    }
                }
                ss << "FROM '" << impl.table.name << "' WHERE ";
                auto primaryKeyColumnNames = impl.table.primary_key_column_names();
                if(primaryKeyColumnNames.size() && primaryKeyColumnNames.front().length()){
                    for(size_t i = 0; i < primaryKeyColumnNames.size(); ++i) {
                        ss << "\"" << primaryKeyColumnNames[i] << "\"" << " = ? ";
                        if(i < primaryKeyColumnNames.size() - 1) {
                            ss << "AND ";
                        }
                        ss << ' ';
                    }
                    auto query = ss.str();
                    sqlite3_stmt *stmt;
                    if (sqlite3_prepare_v2(connection->get_db(), query.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
                        statement_finalizer finalizer{stmt};
                        auto index = 1;
                        auto idsTuple = std::make_tuple(std::forward<Ids>(ids)...);
                        constexpr const auto idsCount = std::tuple_size<decltype(idsTuple)>::value;
                        tuple_helper::iterator<idsCount - 1, Ids...>()(idsTuple, [stmt, &index](auto &v){
                            using field_type = typename std::decay<decltype(v)>::type;
                            statement_binder<field_type>().bind(stmt, index++, v);
                        });
                        auto stepRes = sqlite3_step(stmt);
                        switch(stepRes){
                            case SQLITE_ROW:{
                                O res;
                                index = 0;
                                impl.table.for_each_column([&index, &res, stmt] (auto c) {
                                    using field_type = typename decltype(c)::field_type;
                                    auto value = row_extractor<field_type>().extract(stmt, index++);
                                    if(c.member_pointer){
                                        res.*c.member_pointer = value;
                                    }else{
                                        ((res).*(c.setter))(std::move(value));
                                    }
                                });
                                return std::make_shared<O>(std::move(res));
                            }break;
                            case SQLITE_DONE:{
                                return {};
                            }break;
                            default:{
                                throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                            }
                        }
                    }else{
                        throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                    }
                }else{
                    throw std::system_error(std::make_error_code(orm_error_code::table_has_no_primary_key_column));
                }
            }
            
            /**
             *  SELECT COUNT(*) with no conditions routine. https://www.sqlite.org/lang_aggfunc.html#count
             *  @return Number of O object in table.
             */
            template<class O, class ...Args, class R = typename mapped_type_proxy<O>::type>
            int count(Args&& ...args) {
                this->assert_mapped_type<R>();
                auto tableAliasString = alias_exractor<O>::get();
                
                auto connection = this->get_or_create_connection();
                auto &impl = this->get_impl<R>();
                int res = 0;
                std::stringstream ss;
                ss << "SELECT " << static_cast<std::string>(sqlite_orm::count()) << "(*) FROM '" << impl.table.name << "' ";
                if(tableAliasString.length()) {
                    ss << "'" << tableAliasString << "' ";
                }
                this->process_conditions(ss, args...);
                auto query = ss.str();
                auto rc = sqlite3_exec(connection->get_db(),
                                       query.c_str(),
                                       [](void *data, int argc, char **argv, char **) -> int {
                                           auto &res = *(int*)data;
                                           if(argc){
                                               res = row_extractor<int>().extract(argv[0]);
                                           }
                                           return 0;
                                       }, &res, nullptr);
                if(rc != SQLITE_OK) {
                    throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                }
                return res;
            }
            
            /**
             *  SELECT COUNT(X) https://www.sqlite.org/lang_aggfunc.html#count
             *  @param m member pointer to class mapped to the storage.
             */
            template<class F, class O, class ...Args>
            int count(F O::*m, Args&& ...args) {
                this->assert_mapped_type<O>();
                
                auto connection = this->get_or_create_connection();
                auto &impl = this->get_impl<O>();
                int res = 0;
                std::stringstream ss;
                ss << "SELECT " << static_cast<std::string>(sqlite_orm::count(0)) << "(";
                auto columnName = this->string_from_expression(m);
                if(columnName.length()){
                    ss << columnName << ") FROM '"<< impl.table.name << "' ";
                    this->process_conditions(ss, std::forward<Args>(args)...);
                    auto query = ss.str();
                    auto rc = sqlite3_exec(connection->get_db(),
                                           query.c_str(),
                                           [](void *data, int argc, char **argv,char **) -> int {
                                               auto &res = *(int*)data;
                                               if(argc){
                                                   res = row_extractor<int>().extract(argv[0]);
                                               }
                                               return 0;
                                           }, &res, nullptr);
                    if(rc != SQLITE_OK) {
                        throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                    }
                }else{
                    throw std::system_error(std::make_error_code(orm_error_code::column_not_found));
                }
                return res;
            }
            
            /**
             *  AVG(X) query.   https://www.sqlite.org/lang_aggfunc.html#avg
             *  @param m is a class member pointer (the same you passed into make_column).
             *  @return average value from db.
             */
            template<class F, class O, class ...Args>
            double avg(F O::*m, Args&& ...args) {
                this->assert_mapped_type<O>();
                
                auto connection = this->get_or_create_connection();
                auto &impl = this->get_impl<O>();
                double res = 0;
                std::stringstream ss;
                ss << "SELECT " << static_cast<std::string>(sqlite_orm::avg(0)) << "(";
                auto columnName = this->string_from_expression(m);
                if(columnName.length()){
                    ss << columnName << ") FROM '"<< impl.table.name << "' ";
                    this->process_conditions(ss, std::forward<Args>(args)...);
                    auto query = ss.str();
                    auto rc = sqlite3_exec(connection->get_db(),
                                           query.c_str(),
                                           [](void *data, int argc, char **argv,char **)->int{
                                               auto &res = *(double*)data;
                                               if(argc){
                                                   res = row_extractor<double>().extract(argv[0]);
                                               }
                                               return 0;
                                           }, &res, nullptr);
                    if(rc != SQLITE_OK) {
                        throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                    }
                }else{
                    throw std::system_error(std::make_error_code(orm_error_code::column_not_found));
                }
                return res;
            }
            
            template<class F, class O>
            std::string group_concat(F O::*m) {
                return this->group_concat_internal(m, {});
            }
            
            /**
             *  GROUP_CONCAT(X) query.  https://www.sqlite.org/lang_aggfunc.html#groupconcat
             *  @param m is a class member pointer (the same you passed into make_column).
             *  @return group_concat query result.
             */
            template<class F, class O, class ...Args,
            class Tuple = std::tuple<Args...>,
            typename sfinae = typename std::enable_if<std::tuple_size<std::tuple<Args...>>::value >= 1>::type
            >
            std::string group_concat(F O::*m, Args&& ...args) {
                return this->group_concat_internal(m, {}, std::forward<Args>(args)...);
            }
            
            /**
             *  GROUP_CONCAT(X, Y) query.   https://www.sqlite.org/lang_aggfunc.html#groupconcat
             *  @param m is a class member pointer (the same you passed into make_column).
             *  @return group_concat query result.
             */
            template<class F, class O, class ...Args>
            std::string group_concat(F O::*m, const std::string &y, Args&& ...args) {
                return this->group_concat_internal(m, std::make_shared<std::string>(y), std::forward<Args>(args)...);
            }
            
            template<class F, class O, class ...Args>
            std::string group_concat(F O::*m, const char *y, Args&& ...args) {
                return this->group_concat_internal(m, std::make_shared<std::string>(y), std::forward<Args>(args)...);
            }
            
            /**
             *  MAX(x) query.
             *  @param m is a class member pointer (the same you passed into make_column).
             *  @return std::shared_ptr with max value or null if sqlite engine returned null.
             */
            template<class F, class O, class ...Args, class Ret = typename column_result_t<F O::*>::type>
            std::shared_ptr<Ret> max(F O::*m, Args&& ...args) {
                this->assert_mapped_type<O>();
                
                auto connection = this->get_or_create_connection();
                auto &impl = this->get_impl<O>();
                std::shared_ptr<Ret> res;
                std::stringstream ss;
                ss << "SELECT " << static_cast<std::string>(sqlite_orm::max(0)) << "(";
                auto columnName = this->string_from_expression(m);
                if(columnName.length()){
                    ss << columnName << ") FROM '" << impl.table.name << "' ";
                    this->process_conditions(ss, std::forward<Args>(args)...);
                    auto query = ss.str();
                    auto rc = sqlite3_exec(connection->get_db(),
                                           query.c_str(),
                                           [](void *data, int argc, char **argv,char **)->int{
                                               auto &res = *(std::shared_ptr<Ret>*)data;
                                               if(argc){
                                                   if(argv[0]){
                                                       res = std::make_shared<Ret>(row_extractor<Ret>().extract(argv[0]));
                                                   }
                                               }
                                               return 0;
                                           }, &res, nullptr);
                    if(rc != SQLITE_OK) {
                        throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                    }
                }else{
                    throw std::system_error(std::make_error_code(orm_error_code::column_not_found));
                }
                return res;
            }
            
            /**
             *  MIN(x) query.
             *  @param m is a class member pointer (the same you passed into make_column).
             *  @return std::shared_ptr with min value or null if sqlite engine returned null.
             */
            template<class F, class O, class ...Args, class Ret = typename column_result_t<F O::*>::type>
            std::shared_ptr<Ret> min(F O::*m, Args&& ...args) {
                this->assert_mapped_type<O>();
                
                auto connection = this->get_or_create_connection();
                auto &impl = this->get_impl<O>();
                std::shared_ptr<Ret> res;
                std::stringstream ss;
                ss << "SELECT " << static_cast<std::string>(sqlite_orm::min(0)) << "(";
                auto columnName = this->string_from_expression(m);
                if(columnName.length()){
                    ss << columnName << ") FROM '" << impl.table.name << "' ";
                    this->process_conditions(ss, std::forward<Args>(args)...);
                    auto query = ss.str();
                    auto rc = sqlite3_exec(connection->get_db(),
                                           query.c_str(),
                                           [](void *data, int argc, char **argv,char **)->int{
                                               auto &res = *(std::shared_ptr<Ret>*)data;
                                               if(argc){
                                                   if(argv[0]){
                                                       res = std::make_shared<Ret>(row_extractor<Ret>().extract(argv[0]));
                                                   }
                                               }
                                               return 0;
                                           }, &res, nullptr);
                    if(rc != SQLITE_OK) {
                        throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                    }
                }else{
                    throw std::system_error(std::make_error_code(orm_error_code::column_not_found));
                }
                return res;
            }
            
            /**
             *  SUM(x) query.
             *  @param m is a class member pointer (the same you passed into make_column).
             *  @return std::shared_ptr with sum value or null if sqlite engine returned null.
             */
            template<class F, class O, class ...Args, class Ret = typename column_result_t<F O::*>::type>
            std::shared_ptr<Ret> sum(F O::*m, Args&& ...args) {
                this->assert_mapped_type<O>();
                
                auto connection = this->get_or_create_connection();
                auto &impl = this->get_impl<O>();
                std::shared_ptr<Ret> res;
                std::stringstream ss;
                ss << "SELECT " << static_cast<std::string>(sqlite_orm::sum(0)) << "(";
                auto columnName = this->string_from_expression(m);
                if(columnName.length()){
                    ss << columnName << ") FROM '"<< impl.table.name << "' ";
                    this->process_conditions(ss, std::forward<Args>(args)...);
                    auto query = ss.str();
                    auto rc = sqlite3_exec(connection->get_db(),
                                           query.c_str(),
                                           [](void *data, int argc, char **argv, char **)->int{
                                               auto &res = *(std::shared_ptr<Ret>*)data;
                                               if(argc){
                                                   res = std::make_shared<Ret>(row_extractor<Ret>().extract(argv[0]));
                                               }
                                               return 0;
                                           }, &res, nullptr);
                    if(rc != SQLITE_OK) {
                        throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                    }
                }else{
                    throw std::system_error(std::make_error_code(orm_error_code::column_not_found));
                }
                return res;
            }
            
            /**
             *  TOTAL(x) query.
             *  @param m is a class member pointer (the same you passed into make_column).
             *  @return total value (the same as SUM but not nullable. More details here https://www.sqlite.org/lang_aggfunc.html)
             */
            template<class F, class O, class ...Args>
            double total(F O::*m, Args&& ...args) {
                this->assert_mapped_type<O>();
                
                auto connection = this->get_or_create_connection();
                double res;
                std::stringstream ss;
                ss << "SELECT " << static_cast<std::string>(sqlite_orm::total(0)) << "(";
                auto columnName = this->string_from_expression(m);
                if(columnName.length()){
                    ss << columnName << ") ";
                    auto tableNamesSet = this->parse_table_names(m);
                    if(tableNamesSet.size()){
                        ss << "FROM " ;
                        std::vector<std::string> tableNames(tableNamesSet.begin(), tableNamesSet.end());
                        for(size_t i = 0; i < tableNames.size(); ++i) {
                            ss << " '" << tableNames[i] << "' ";
                            if(i < tableNames.size() - 1) {
                                ss << ",";
                            }
                            ss << " ";
                        }
                    }
                    this->process_conditions(ss, std::forward<Args>(args)...);
                    auto query = ss.str();
                    auto rc = sqlite3_exec(connection->get_db(),
                                           query.c_str(),
                                           [](void *data, int argc, char **argv, char **)->int{
                                               auto &res = *(double*)data;
                                               if(argc){
                                                   res = row_extractor<double>().extract(argv[0]);
                                               }
                                               return 0;
                                           }, &res, nullptr);
                    if(rc != SQLITE_OK) {
                        throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                    }
                }else{
                    throw std::system_error(std::make_error_code(orm_error_code::column_not_found));
                }
                return res;
            }
            
            /**
             *  Select a single column into std::vector<T> or multiple columns into std::vector<std::tuple<...>>.
             *  For a single column use `auto rows = storage.select(&User::id, where(...));
             *  For multicolumns user `auto rows = storage.select(columns(&User::id, &User::name), where(...));
             */
            template<
            class T,
            class ...Args,
            class R = typename internal::column_result_t<T>::type>
            std::vector<R> select(T m, Args ...args) {
                using select_type = select_t<T, Args...>;
                auto query = this->string_from_expression(select_type{std::move(m), std::make_tuple<Args...>(std::forward<Args>(args)...)});
                auto connection = this->get_or_create_connection();
                sqlite3_stmt *stmt;
                if (sqlite3_prepare_v2(connection->get_db(), query.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
                    statement_finalizer finalizer{stmt};
                    std::vector<R> res;
                    int stepRes;
                    do{
                        stepRes = sqlite3_step(stmt);
                        switch(stepRes){
                            case SQLITE_ROW:{
                                res.push_back(row_extractor<R>().extract(stmt, 0));
                            }break;
                            case SQLITE_DONE: break;
                            default:{
                                throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                            }
                        }
                    }while(stepRes != SQLITE_DONE);
                    return res;
                }else{
                    throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                }
            }
            
            template<
            class L,
            class R,
            class ...Args,
            class Ret = typename internal::column_result_t<union_t<L, R>>::type>
            std::vector<Ret> select(union_t<L, R> op, Args ...args) {
                std::stringstream ss;
                ss << this->string_from_expression(op.left) << " ";
                ss << static_cast<std::string>(op) << " ";
                ss << this->string_from_expression(op.right) << " ";
                auto query = ss.str();
                auto connection = this->get_or_create_connection();
                sqlite3_stmt *stmt;
                if (sqlite3_prepare_v2(connection->get_db(), query.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
                    statement_finalizer finalizer{stmt};
                    std::vector<Ret> res;
                    int stepRes;
                    do{
                        stepRes = sqlite3_step(stmt);
                        switch(stepRes){
                            case SQLITE_ROW:{
                                res.push_back(row_extractor<Ret>().extract(stmt, 0));
                            }break;
                            case SQLITE_DONE: break;
                            default:{
                                throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                            }
                        }
                    }while(stepRes != SQLITE_DONE);
                    return res;
                }else{
                    throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                }
            }
            
            /**
             *  Returns a string representation of object of a class mapped to the storage.
             *  Type of string has json-like style.
             */
            template<class O>
            std::string dump(const O &o) {
                this->assert_mapped_type<O>();
                return this->impl.dump(o);
            }
            
            /**
             *  This is REPLACE (INSERT OR REPLACE) function.
             *  Also if you need to insert value with knows id you should
             *  also you this function instead of insert cause inserts ignores
             *  id and creates own one.
             */
            template<class O>
            void replace(const O &o) {
                this->assert_mapped_type<O>();
                
                auto connection = this->get_or_create_connection();
                auto &impl = get_impl<O>();
                std::stringstream ss;
                ss << "REPLACE INTO '" << impl.table.name << "' (";
                auto columnNames = impl.table.column_names();
                auto columnNamesCount = columnNames.size();
                for(size_t i = 0; i < columnNamesCount; ++i) {
                    ss << "\"" << columnNames[i] << "\"";
                    if(i < columnNamesCount - 1) {
                        ss << ", ";
                    }else{
                        ss << ") ";
                    }
                }
                ss << "VALUES(";
                for(size_t i = 0; i < columnNamesCount; ++i) {
                    ss << "?";
                    if(i < columnNamesCount - 1) {
                        ss << ", ";
                    }else{
                        ss << ")";
                    }
                }
                auto query = ss.str();
                sqlite3_stmt *stmt;
                if (sqlite3_prepare_v2(connection->get_db(), query.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
                    statement_finalizer finalizer{stmt};
                    auto index = 1;
                    impl.table.for_each_column([&o, &index, &stmt] (auto c) {
                        using field_type = typename decltype(c)::field_type;
                        const field_type *value = nullptr;
                        if(c.member_pointer){
                            value = &(o.*c.member_pointer);
                        }else{
                            value = &((o).*(c.getter))();
                        }
                        statement_binder<field_type>().bind(stmt, index++, *value);
                    });
                    if (sqlite3_step(stmt) == SQLITE_DONE) {
                        //..
                    }else{
                        throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                    }
                }else {
                    throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                }
            }
            
            template<class It>
            void replace_range(It from, It to) {
                using O = typename std::iterator_traits<It>::value_type;
                this->assert_mapped_type<O>();
                if(from == to) {
                    return;
                }
                
                auto connection = this->get_or_create_connection();
                auto &impl = get_impl<O>();
                std::stringstream ss;
                ss << "REPLACE INTO '" << impl.table.name << "' (";
                auto columnNames = impl.table.column_names();
                auto columnNamesCount = columnNames.size();
                for(size_t i = 0; i < columnNamesCount; ++i) {
                    ss << "\"" << columnNames[i] << "\"";
                    if(i < columnNamesCount - 1) {
                        ss << ", ";
                    }else{
                        ss << ") ";
                    }
                }
                ss << "VALUES ";
                auto valuesString = [columnNamesCount]{
                    std::stringstream ss;
                    ss << "(";
                    for(size_t i = 0; i < columnNamesCount; ++i) {
                        ss << "?";
                        if(i < columnNamesCount - 1) {
                            ss << ", ";
                        }else{
                            ss << ")";
                        }
                    }
                    return ss.str();
                }();
                auto valuesCount = static_cast<int>(std::distance(from, to));
                for(auto i = 0; i < valuesCount; ++i) {
                    ss << valuesString;
                    if(i < valuesCount - 1) {
                        ss << ",";
                    }
                    ss << " ";
                }
                auto query = ss.str();
                sqlite3_stmt *stmt;
                if (sqlite3_prepare_v2(connection->get_db(), query.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
                    statement_finalizer finalizer{stmt};
                    auto index = 1;
                    for(auto it = from; it != to; ++it) {
                        auto &o = *it;
                        impl.table.for_each_column([&o, &index, &stmt] (auto c) {
                            using field_type = typename decltype(c)::field_type;
                            const field_type *value = nullptr;
                            if(c.member_pointer){
                                value = &(o.*c.member_pointer);
                            }else{
                                value = &((o).*(c.getter))();
                            }
                            statement_binder<field_type>().bind(stmt, index++, *value);
                        });
                    }
                    if (sqlite3_step(stmt) == SQLITE_DONE) {
                        //..
                    }else{
                        throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                    }
                }else {
                    throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                }
            }
            
            template<class O, class ...Cols>
            int insert(const O &o, columns_t<Cols...> cols) {
                constexpr const size_t colsCount = std::tuple_size<std::tuple<Cols...>>::value;
                static_assert(colsCount > 0, "Use insert or replace with 1 argument instead");
                this->assert_mapped_type<O>();
                auto connection = this->get_or_create_connection();
                auto &impl = get_impl<O>();
                std::stringstream ss;
                ss << "INSERT INTO '" << impl.table.name << "' ";
                std::vector<std::string> columnNames;
                columnNames.reserve(colsCount);
                cols.for_each([&columnNames, this](auto &m) {
                    auto columnName = this->string_from_expression(m, true);
                    if(columnName.length()){
                        columnNames.push_back(columnName);
                    }else{
                        throw std::system_error(std::make_error_code(orm_error_code::column_not_found));
                    }
                });
                ss << "(";
                for(size_t i = 0; i < columnNames.size(); ++i){
                    ss << columnNames[i];
                    if(i < columnNames.size() - 1){
                        ss << ",";
                    }else{
                        ss << ")";
                    }
                    ss << " ";
                }
                ss << "VALUES (";
                for(size_t i = 0; i < columnNames.size(); ++i){
                    ss << "?";
                    if(i < columnNames.size() - 1){
                        ss << ",";
                    }else{
                        ss << ")";
                    }
                    ss << " ";
                }
                auto query = ss.str();
                sqlite3_stmt *stmt;
                if (sqlite3_prepare_v2(connection->get_db(), query.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
                    statement_finalizer finalizer{stmt};
                    auto index = 1;
                    cols.for_each([&o, &index, &stmt, &impl] (auto &m) {
                        using column_type = typename std::decay<decltype(m)>::type;
                        using field_type = typename column_result_t<column_type>::type;
                        const field_type *value = impl.table.template get_object_field_pointer<field_type>(o, m);
                        statement_binder<field_type>().bind(stmt, index++, *value);
                    });
                    if (sqlite3_step(stmt) == SQLITE_DONE) {
                        return int(sqlite3_last_insert_rowid(connection->get_db()));
                    }else{
                        throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                    }
                }else {
                    throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                }
            }
            
            /**
             *  Insert routine. Inserts object with all non primary key fields in passed object. Id of passed
             *  object doesn't matter.
             *  @return id of just created object.
             */
            template<class O>
            int insert(const O &o) {
                this->assert_mapped_type<O>();
                
                auto connection = this->get_or_create_connection();
                auto &impl = get_impl<O>();
                int res = 0;
                std::stringstream ss;
                ss << "INSERT INTO '" << impl.table.name << "' ";
                std::vector<std::string> columnNames;
                auto compositeKeyColumnNames = impl.table.composite_key_columns_names();
                
                impl.table.for_each_column([&impl, &columnNames, &compositeKeyColumnNames] (auto c) {
                    if(impl.table._without_rowid || !c.template has<constraints::primary_key_t<>>()) {
                        auto it = std::find(compositeKeyColumnNames.begin(),
                                            compositeKeyColumnNames.end(),
                                            c.name);
                        if(it == compositeKeyColumnNames.end()){
                            columnNames.emplace_back(c.name);
                        }
                    }
                });
                
                auto columnNamesCount = columnNames.size();
                if(columnNamesCount){
                    ss << "( ";
                    for(size_t i = 0; i < columnNamesCount; ++i) {
                        ss << "\"" << columnNames[i] << "\"";
                        if(i < columnNamesCount - 1) {
                            ss << ", ";
                        }else{
                            ss << ") ";
                        }
                    }
                }else{
                    ss << "DEFAULT ";
                }
                ss << "VALUES ";
                if(columnNamesCount){
                    ss << "( ";
                    for(size_t i = 0; i < columnNamesCount; ++i) {
                        ss << "?";
                        if(i < columnNamesCount - 1) {
                            ss << ", ";
                        }else{
                            ss << ")";
                        }
                    }
                }
                auto query = ss.str();
                sqlite3_stmt *stmt;
                if (sqlite3_prepare_v2(connection->get_db(), query.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
                    statement_finalizer finalizer{stmt};
                    auto index = 1;
                    impl.table.for_each_column([&o, &index, &stmt, &impl, &compositeKeyColumnNames] (auto c) {
                        if(impl.table._without_rowid || !c.template has<constraints::primary_key_t<>>()){
                            auto it = std::find(compositeKeyColumnNames.begin(),
                                                compositeKeyColumnNames.end(),
                                                c.name);
                            if(it == compositeKeyColumnNames.end()){
                                using field_type = typename decltype(c)::field_type;
                                const field_type *value = nullptr;
                                if(c.member_pointer){
                                    value = &(o.*c.member_pointer);
                                }else{
                                    value = &((o).*(c.getter))();
                                }
                                statement_binder<field_type>().bind(stmt, index++, *value);
                            }
                        }
                    });
                    if (sqlite3_step(stmt) == SQLITE_DONE) {
                        res = int(sqlite3_last_insert_rowid(connection->get_db()));
                    }else{
                        throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                    }
                }else {
                    throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                }
                return res;
            }
            
            template<class It>
            void insert_range(It from, It to) {
                using O = typename std::iterator_traits<It>::value_type;
                this->assert_mapped_type<O>();
                if(from == to) {
                    return;
                }
                
                auto connection = this->get_or_create_connection();
                auto &impl = get_impl<O>();
                
                std::stringstream ss;
                ss << "INSERT INTO '" << impl.table.name << "' (";
                std::vector<std::string> columnNames;
                impl.table.for_each_column([&columnNames] (auto c) {
                    if(!c.template has<constraints::primary_key_t<>>()) {
                        columnNames.emplace_back(c.name);
                    }
                });
                
                auto columnNamesCount = columnNames.size();
                for(size_t i = 0; i < columnNamesCount; ++i) {
                    ss << "\"" << columnNames[i] << "\"";
                    if(i < columnNamesCount - 1) {
                        ss << ", ";
                    }else{
                        ss << ") ";
                    }
                }
                ss << "VALUES ";
                auto valuesString = [columnNamesCount]{
                    std::stringstream ss;
                    ss << "(";
                    for(size_t i = 0; i < columnNamesCount; ++i) {
                        ss << "?";
                        if(i < columnNamesCount - 1) {
                            ss << ", ";
                        }else{
                            ss << ")";
                        }
                    }
                    return ss.str();
                }();
                auto valuesCount = static_cast<int>(std::distance(from, to));
                for(auto i = 0; i < valuesCount; ++i) {
                    ss << valuesString;
                    if(i < valuesCount - 1) {
                        ss << ",";
                    }
                    ss << " ";
                }
                auto query = ss.str();
                sqlite3_stmt *stmt;
                if (sqlite3_prepare_v2(connection->get_db(), query.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
                    statement_finalizer finalizer{stmt};
                    auto index = 1;
                    for(auto it = from; it != to; ++it) {
                        auto &o = *it;
                        impl.table.for_each_column([&o, &index, &stmt] (auto c) {
                            if(!c.template has<constraints::primary_key_t<>>()){
                                typedef typename decltype(c)::field_type field_type;
                                const field_type *value = nullptr;
                                if(c.member_pointer){
                                    value = &(o.*c.member_pointer);
                                }else{
                                    value = &((o).*(c.getter))();
                                }
                                statement_binder<field_type>().bind(stmt, index++, *value);
                            }
                        });
                    }
                    if (sqlite3_step(stmt) == SQLITE_DONE) {
                        //..
                    }else{
                        throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                    }
                }else {
                    throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                }
            }
            
            void drop_index(const std::string &indexName) {
                auto connection = this->get_or_create_connection();
                std::stringstream ss;
                ss << "DROP INDEX '" << indexName + "'";
                auto query = ss.str();
                sqlite3_stmt *stmt;
                if (sqlite3_prepare_v2(connection->get_db(), query.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
                    statement_finalizer finalizer{stmt};
                    if (sqlite3_step(stmt) == SQLITE_DONE) {
                        //  done..
                    }else{
                        throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                    }
                }else {
                    throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                }
            }
            
            void vacuum() {
                auto connection = this->get_or_create_connection();
                std::string query = "VACUUM";
                sqlite3_stmt *stmt;
                if (sqlite3_prepare_v2(connection->get_db(), query.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
                    statement_finalizer finalizer{stmt};
                    if (sqlite3_step(stmt) == SQLITE_DONE) {
                        //  done..
                    }else{
                        throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                    }
                }else {
                    throw std::system_error(std::error_code(sqlite3_errcode(connection->get_db()), get_sqlite_error_category()));
                }
            }
            
        protected:
            
            void drop_table_internal(const std::string &tableName, sqlite3 *db) {
                std::stringstream ss;
                ss << "DROP TABLE '" << tableName + "'";
                auto query = ss.str();
                sqlite3_stmt *stmt;
                if (sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
                    statement_finalizer finalizer{stmt};
                    if (sqlite3_step(stmt) == SQLITE_DONE) {
                        //  done..
                    }else{
                        throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                    }
                }else {
                    throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                }
            }
            
        public:
            
            /**
             *  Drops table with given name.
             */
            void drop_table(const std::string &tableName) {
                auto connection = this->get_or_create_connection();
                this->drop_table_internal(tableName, connection->get_db());
            }
            
            /**
             *  sqlite3_changes function.
             */
            int changes() {
                auto connection = this->get_or_create_connection();
                return sqlite3_changes(connection->get_db());
            }
            
            /**
             *  sqlite3_total_changes function.
             */
            int total_changes() {
                auto connection = this->get_or_create_connection();
                return sqlite3_total_changes(connection->get_db());
            }
            
            int64 last_insert_rowid() {
                auto connection = this->get_or_create_connection();
                return sqlite3_last_insert_rowid(connection->get_db());
            }
            
            int busy_timeout(int ms) {
                auto connection = this->get_or_create_connection();
                return sqlite3_busy_timeout(connection->get_db(), ms);
            }
            
            /**
             *  Returns libsqltie3 lib version, not sqlite_orm
             */
            std::string libversion() {
                return sqlite3_libversion();
            }
            
        protected:
            
            template<class ...Tss, class ...Cols>
            sync_schema_result sync_table(storage_impl<internal::index_t<Cols...>, Tss...> *impl, sqlite3 *db, bool) {
                auto res = sync_schema_result::already_in_sync;
                std::stringstream ss;
                ss << "CREATE ";
                if(impl->table.unique){
                    ss << "UNIQUE ";
                }
                using columns_type = typename decltype(impl->table)::columns_type;
                using head_t = typename std::tuple_element<0, columns_type>::type;
                using indexed_type = typename internal::table_type<head_t>::type;
                ss << "INDEX IF NOT EXISTS " << impl->table.name << " ON '" << this->impl.template find_table_name<indexed_type>() << "' ( ";
                std::vector<std::string> columnNames;
                tuple_helper::iterator<std::tuple_size<columns_type>::value - 1, Cols...>()(impl->table.columns, [&columnNames, this](auto &v){
                    columnNames.push_back(this->impl.column_name(v));
                });
                for(size_t i = 0; i < columnNames.size(); ++i) {
                    ss << columnNames[i];
                    if(i < columnNames.size() - 1) {
                        ss << ",";
                    }
                    ss << " ";
                }
                ss << ") ";
                auto query = ss.str();
                auto rc = sqlite3_exec(db, query.c_str(), nullptr, nullptr, nullptr);
                if(rc != SQLITE_OK) {
                    throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                }
                return res;
            }
            
            template<class ...Tss, class ...Cs>
            sync_schema_result sync_table(storage_impl<table_t<Cs...>, Tss...> *impl, sqlite3 *db, bool preserve) {
                auto res = sync_schema_result::already_in_sync;
                
                auto schema_stat = impl->schema_status(db, preserve);
                if(schema_stat != decltype(schema_stat)::already_in_sync) {
                    if(schema_stat == decltype(schema_stat)::new_table_created) {
                        this->create_table(db, impl->table.name, impl);
                        res = decltype(res)::new_table_created;
                    }else{
                        if(schema_stat == sync_schema_result::old_columns_removed
                           || schema_stat == sync_schema_result::new_columns_added
                           || schema_stat == sync_schema_result::new_columns_added_and_old_columns_removed)
                        {
                            
                            //  get table info provided in `make_table` call..
                            auto storageTableInfo = impl->table.get_table_info();
                            
                            //  now get current table info from db using `PRAGMA table_info` query..
                            auto dbTableInfo = impl->get_table_info(impl->table.name, db);
                            
                            //  this vector will contain pointers to columns that gotta be added..
                            std::vector<table_info*> columnsToAdd;
                            
                            impl->get_remove_add_columns(columnsToAdd, storageTableInfo, dbTableInfo);
                            
                            if(schema_stat == sync_schema_result::old_columns_removed) {
                                
                                //  extra table columns than storage columns
                                this->backup_table(db, impl);
                                res = decltype(res)::old_columns_removed;
                            }
                            
                            if(schema_stat == sync_schema_result::new_columns_added) {
                                for(auto columnPointer : columnsToAdd) {
                                    impl->add_column(*columnPointer, db);
                                }
                                res = decltype(res)::new_columns_added;
                            }
                            
                            if(schema_stat == sync_schema_result::new_columns_added_and_old_columns_removed) {
                                
                                //remove extra columns
                                this->backup_table(db, impl);
                                for(auto columnPointer : columnsToAdd) {
                                    impl->add_column(*columnPointer, db);
                                }
                                res = decltype(res)::new_columns_added_and_old_columns_removed;
                            }
                        } else if(schema_stat == sync_schema_result::dropped_and_recreated) {
                            this->drop_table_internal(impl->table.name, db);
                            this->create_table(db, impl->table.name, impl);
                            res = decltype(res)::dropped_and_recreated;
                        }
                    }
                }
                return res;
            }
            
        public:
            
            /**
             *  This is a cute function used to replace migration up/down functionality.
             *  It performs check storage schema with actual db schema and:
             *  * if there are excess tables exist in db they are ignored (not dropped)
             *  * every table from storage is compared with it's db analog and
             *      * if table doesn't exist it is being created
             *      * if table exists its colums are being compared with table_info from db and
             *          * if there are columns in db that do not exist in storage (excess) table will be dropped and recreated
             *          * if there are columns in storage that do not exist in db they will be added using `ALTER TABLE ... ADD COLUMN ...' command
             *          * if there is any column existing in both db and storage but differs by any of properties/constraints (type, pk, notnull, dflt_value) table will be dropped and recreated
             *  Be aware that `sync_schema` doesn't guarantee that data will not be dropped. It guarantees only that it will make db schema the same
             *  as you specified in `make_storage` function call. A good point is that if you have no db file at all it will be created and
             *  all tables also will be created with exact tables and columns you specified in `make_storage`, `make_table` and `make_column` call.
             *  The best practice is to call this function right after storage creation.
             *  @param preserve affects on function behaviour in case it is needed to remove a column. If it is `false` so table will be dropped
             *  if there is column to remove, if `true` -  table is being copied into another table, dropped and copied table is renamed with source table name.
             *  Warning: sync_schema doesn't check foreign keys cause it is unable to do so in sqlite3. If you know how to get foreign key info
             *  please submit an issue https://github.com/fnc12/sqlite_orm/issues
             *  @return std::map with std::string key equal table name and `sync_schema_result` as value. `sync_schema_result` is a enum value that stores
             *  table state after syncing a schema. `sync_schema_result` can be printed out on std::ostream with `operator<<`.
             */
            std::map<std::string, sync_schema_result> sync_schema(bool preserve = false) {
                auto connection = this->get_or_create_connection();
                std::map<std::string, sync_schema_result> result;
                auto db = connection->get_db();
                this->impl.for_each([&result, db, preserve, this](auto impl){
                    auto res = this->sync_table(impl, db, preserve);
                    result.insert({impl->table.name, res});
                });
                return result;
            }
            
            /**
             *  This function returns the same map that `sync_schema` returns but it
             *  doesn't perform `sync_schema` actually - just simulates it in case you want to know
             *  what will happen if you sync your schema.
             */
            std::map<std::string, sync_schema_result> sync_schema_simulate(bool preserve = false) {
                auto connection = this->get_or_create_connection();
                std::map<std::string, sync_schema_result> result;
                auto db = connection->get_db();
                this->impl.for_each([&result, db, preserve](auto impl){
                    result.insert({impl->table.name, impl->schema_status(db, preserve)});
                });
                return result;
            }
            
            bool transaction(std::function<bool()> f) {
                this->begin_transaction();
                auto db = this->currentTransaction->get_db();
                auto shouldCommit = f();
                if(shouldCommit){
                    this->impl.commit(db);
                }else{
                    this->impl.rollback(db);
                }
                if(!this->inMemory && !this->isOpenedForever){
                    this->currentTransaction = nullptr;
                }
                return shouldCommit;
            }
            
            void begin_transaction() {
                if(!this->inMemory){
                    if(!this->isOpenedForever){
                        if(this->currentTransaction) throw std::system_error(std::make_error_code(orm_error_code::cannot_start_a_transaction_within_a_transaction));
                        this->currentTransaction = std::make_shared<internal::database_connection>(this->filename);
                        this->on_open_internal(this->currentTransaction->get_db());
                    }
                }
                auto db = this->currentTransaction->get_db();
                this->impl.begin_transaction(db);
            }
            
            void commit() {
                if(!this->inMemory){
                    if(!this->currentTransaction) throw std::system_error(std::make_error_code(orm_error_code::no_active_transaction));
                }
                auto db = this->currentTransaction->get_db();
                this->impl.commit(db);
                if(!this->inMemory && !this->isOpenedForever){
                    this->currentTransaction = nullptr;
                }
            }
            
            void rollback() {
                if(!this->inMemory){
                    if(!this->currentTransaction) throw std::system_error(std::make_error_code(orm_error_code::no_active_transaction));
                }
                auto db = this->currentTransaction->get_db();
                this->impl.rollback(db);
                if(!this->inMemory && !this->isOpenedForever){
                    this->currentTransaction = nullptr;
                }
            }
            
            std::string current_timestamp() {
                auto connection = this->get_or_create_connection();
                return this->impl.current_timestamp(connection->get_db());
            }
            
        protected:
            
#if SQLITE_VERSION_NUMBER >= 3006019
            
            void foreign_keys(sqlite3 *db, bool value) {
                std::stringstream ss;
                ss << "PRAGMA foreign_keys = " << value;
                auto query = ss.str();
                auto rc = sqlite3_exec(db, query.c_str(), nullptr, nullptr, nullptr);
                if(rc != SQLITE_OK) {
                    throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                }
            }
            
            bool foreign_keys(sqlite3 *db) {
                std::string query = "PRAGMA foreign_keys";
                auto res = false;
                auto rc = sqlite3_exec(db,
                                       query.c_str(),
                                       [](void *data, int argc, char **argv,char **) -> int {
                                           auto &res = *(bool*)data;
                                           if(argc){
                                               res = row_extractor<bool>().extract(argv[0]);
                                           }
                                           return 0;
                                       }, &res, nullptr);
                if(rc != SQLITE_OK) {
                    throw std::system_error(std::error_code(sqlite3_errcode(db), get_sqlite_error_category()));
                }
                return res;
            }
            
#endif
            
        public:
            
#if SQLITE_VERSION_NUMBER >= 3007010
            /**
             * \fn db_release_memory
             * \brief Releases freeable memory of database. It is function can/should be called periodically by application,
             * if application has less memory usage constraint.
             * \note sqlite3_db_release_memory added in 3.7.10 https://sqlite.org/changes.html
             */
            int db_release_memory() {
                auto connection = this->get_or_create_connection();
                return sqlite3_db_release_memory(connection->get_db());
            }
#endif
            
            /**
             *  Checks whether table exists in db. Doesn't check storage itself - works only with actual database.
             *  Note: table can be not mapped to a storage
             *  @return true if table with a given name exists in db, false otherwise.
             */
            bool table_exists(const std::string &tableName) {
                auto connection = this->get_or_create_connection();
                return this->impl.table_exists(tableName, connection->get_db());
            }
            
            /**
             *  Returns existing permanent table names in database. Doesn't check storage itself - works only with actual database.
             *  @return Returns list of tables in database.
             */
            std::vector<std::string> table_names() {
                auto connection = this->get_or_create_connection();
                std::vector<std::string> tableNames;
                std::string sql = "SELECT name FROM sqlite_master WHERE type='table'";
                using Data = std::vector<std::string>;
                int res = sqlite3_exec(connection->get_db(), sql.c_str(),
                                       [] (void *data, int argc, char **argv, char ** /*columnName*/) -> int {
                                           auto& tableNames = *(Data*)data;
                                           for(int i = 0; i < argc; i++) {
                                               if(argv[i]){
                                                   tableNames.push_back(argv[i]);
                                               }
                                           }
                                           return 0;
                                       }, &tableNames,nullptr);
                
                if(res != SQLITE_OK) {
                    throw std::system_error(std::error_code(sqlite3_errcode(connection->db()), get_sqlite_error_category()));
                }
                return tableNames;
            }
            
            void open_forever() {
                this->isOpenedForever = true;
                if(!this->currentTransaction){
                    this->currentTransaction = std::make_shared<internal::database_connection>(this->filename);
                    this->on_open_internal(this->currentTransaction->get_db());
                }
            }
            
        public:
            pragma_t pragma;
            limit_accesor limit;
        };
    }
    
    template<class ...Ts>
    internal::storage_t<Ts...> make_storage(const std::string &filename, Ts ...tables) {
        return {filename, internal::storage_impl<Ts...>(tables...)};
    }
}
#pragma once

#if defined(_MSC_VER)
# if defined(__RESTORE_MIN__)
__pragma(pop_macro("min"))
# undef __RESTORE_MIN__
# endif
# if defined(__RESTORE_MAX__)
__pragma(pop_macro("max"))
# undef __RESTORE_MAX__
# endif
#endif // defined(_MSC_VER)
