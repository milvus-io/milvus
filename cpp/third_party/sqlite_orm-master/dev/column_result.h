#pragma once

#include <type_traits>  //  std::enable_if, std::is_same, std::decay
#include <tuple>    //  std::tuple

#include "core_functions.h"
#include "aggregate_functions.h"
#include "select_constraints.h"
#include "operators.h"
#include "rowid.h"
#include "alias.h"
#include "column.h"
#include "storage_traits.h"

namespace sqlite_orm {
    
    namespace internal {
        
        /**
         *  This is a proxy class used to define what type must have result type depending on select
         *  arguments (member pointer, aggregate functions, etc). Below you can see specializations
         *  for different types. E.g. specialization for core_functions::length_t has `type` int cause
         *  LENGTH returns INTEGER in sqlite. Every column_result_t must have `type` type that equals
         *  c++ SELECT return type for T
         *  T - C++ type
         *  SFINAE - sfinae argument
         */
        template<class St, class T, class SFINAE = void>
        struct column_result_t;
        
        template<class St, class O, class F>
        struct column_result_t<St, F O::*, typename std::enable_if<std::is_member_pointer<F O::*>::value && !std::is_member_function_pointer<F O::*>::value>::type> {
            using type = F;
        };
        
        /**
         *  Common case for all getter types. Getter types are defined in column.h file
         */
        template<class St, class T>
        struct column_result_t<St, T, typename std::enable_if<is_getter<T>::value>::type> {
            using type = typename getter_traits<T>::field_type;
        };
        
        /**
         *  Common case for all setter types. Setter types are defined in column.h file
         */
        template<class St, class T>
        struct column_result_t<St, T, typename std::enable_if<is_setter<T>::value>::type> {
            using type = typename setter_traits<T>::field_type;
        };
        
        template<class St, class T>
        struct column_result_t<St, core_functions::length_t<T>, void> {
            using type = int;
        };
        
#if SQLITE_VERSION_NUMBER >= 3007016
        
        template<class St, class ...Args>
        struct column_result_t<St, core_functions::char_t_<Args...>, void> {
            using type = std::string;
        };
#endif
        
        template<class St>
        struct column_result_t<St, core_functions::random_t, void> {
            using type = int;
        };
        
        template<class St>
        struct column_result_t<St, core_functions::changes_t, void> {
            using type = int;
        };
        
        template<class St, class T>
        struct column_result_t<St, core_functions::abs_t<T>, void> {
            using type = std::unique_ptr<double>;
        };
        
        template<class St, class T>
        struct column_result_t<St, core_functions::lower_t<T>, void> {
            using type = std::string;
        };
        
        template<class St, class T>
        struct column_result_t<St, core_functions::upper_t<T>, void> {
            using type = std::string;
        };
        
        template<class St, class X>
        struct column_result_t<St, core_functions::trim_single_t<X>, void> {
            using type = std::string;
        };
        
        template<class St, class X, class Y>
        struct column_result_t<St, core_functions::trim_double_t<X, Y>, void> {
            using type = std::string;
        };
        
        template<class St, class X>
        struct column_result_t<St, core_functions::ltrim_single_t<X>, void> {
            using type = std::string;
        };
        
        template<class St, class X, class Y>
        struct column_result_t<St, core_functions::ltrim_double_t<X, Y>, void> {
            using type = std::string;
        };
        
        template<class St, class X>
        struct column_result_t<St, core_functions::rtrim_single_t<X>, void> {
            using type = std::string;
        };
        
        template<class St, class X, class Y>
        struct column_result_t<St, core_functions::rtrim_double_t<X, Y>, void> {
            using type = std::string;
        };
        
        template<class St, class T, class ...Args>
        struct column_result_t<St, core_functions::date_t<T, Args...>, void> {
            using type = std::string;
        };
        
        template<class St, class T, class ...Args>
        struct column_result_t<St, core_functions::julianday_t<T, Args...>, void> {
            using type = double;
        };
        
        template<class St, class T, class ...Args>
        struct column_result_t<St, core_functions::datetime_t<T, Args...>, void> {
            using type = std::string;
        };
        
        template<class St, class T>
        struct column_result_t<St, aggregate_functions::avg_t<T>, void> {
            using type = double;
        };
        
        template<class St, class T>
        struct column_result_t<St, aggregate_functions::count_t<T>, void> {
            using type = int;
        };
        
        template<class St, class T>
        struct column_result_t<St, aggregate_functions::count_asterisk_t<T>, void> {
            using type = int;
        };
        
        template<class St, class T>
        struct column_result_t<St, aggregate_functions::sum_t<T>, void> {
            using type = std::unique_ptr<double>;
        };
        
        template<class St, class T>
        struct column_result_t<St, aggregate_functions::total_t<T>, void> {
            using type = double;
        };
        
        template<class St, class T>
        struct column_result_t<St, aggregate_functions::group_concat_single_t<T>, void> {
            using type = std::string;
        };
        
        template<class St, class T>
        struct column_result_t<St, aggregate_functions::group_concat_double_t<T>, void> {
            using type = std::string;
        };
        
        template<class St, class T>
        struct column_result_t<St, aggregate_functions::max_t<T>, void> {
            using type = std::unique_ptr<typename column_result_t<St, T>::type>;
        };
        
        template<class St, class T>
        struct column_result_t<St, aggregate_functions::min_t<T>, void> {
            using type = std::unique_ptr<typename column_result_t<St, T>::type>;
        };
        
        template<class St>
        struct column_result_t<St, aggregate_functions::count_asterisk_without_type, void> {
            using type = int;
        };
        
        template<class St, class T>
        struct column_result_t<St, distinct_t<T>, void> {
            using type = typename column_result_t<St, T>::type;
        };
        
        template<class St, class T>
        struct column_result_t<St, all_t<T>, void> {
            using type = typename column_result_t<St, T>::type;
        };
        
        template<class St, class L, class R>
        struct column_result_t<St, conc_t<L, R>, void> {
            using type = std::string;
        };
        
        template<class St, class L, class R>
        struct column_result_t<St, add_t<L, R>, void> {
            using type = double;
        };
        
        template<class St, class L, class R>
        struct column_result_t<St, sub_t<L, R>, void> {
            using type = double;
        };
        
        template<class St, class L, class R>
        struct column_result_t<St, mul_t<L, R>, void> {
            using type = double;
        };
        
        template<class St, class L, class R>
        struct column_result_t<St, internal::div_t<L, R>, void> {
            using type = double;
        };
        
        template<class St, class L, class R>
        struct column_result_t<St, mod_t<L, R>, void> {
            using type = double;
        };
        
        template<class St>
        struct column_result_t<St, rowid_t, void> {
            using type = int64;
        };
        
        template<class St>
        struct column_result_t<St, oid_t, void> {
            using type = int64;
        };
        
        template<class St>
        struct column_result_t<St, _rowid_t, void> {
            using type = int64;
        };
        
        template<class St, class T>
        struct column_result_t<St, table_rowid_t<T>, void> {
            using type = int64;
        };
        
        template<class St, class T>
        struct column_result_t<St, table_oid_t<T>, void> {
            using type = int64;
        };
        
        template<class St, class T>
        struct column_result_t<St, table__rowid_t<T>, void> {
            using type = int64;
        };
        
        template<class St, class T, class C>
        struct column_result_t<St, alias_column_t<T, C>, void> {
            using type = typename column_result_t<St, C>::type;
        };
        
        template<class St, class T, class F>
        struct column_result_t<St, column_pointer<T, F>> : column_result_t<St, F, void> {};
        
        template<class St, class ...Args>
        struct column_result_t<St, columns_t<Args...>, void> {
            using type = std::tuple<typename column_result_t<St, typename std::decay<Args>::type>::type...>;
        };
        
        template<class St, class T, class ...Args>
        struct column_result_t<St, select_t<T, Args...>> : column_result_t<St, T, void> {};
        
        template<class St, class T>
        struct column_result_t<St, T, typename std::enable_if<is_base_of_template<T, compound_operator>::value>::type> {
            using left_type = typename T::left_type;
            using right_type = typename T::right_type;
            using left_result = typename column_result_t<St, left_type>::type;
            using right_result = typename column_result_t<St, right_type>::type;
            static_assert(std::is_same<left_result, right_result>::value, "Compound subselect queries must return same types");
            using type = left_result;
        };
        
        /**
         *  Result for the most simple queries like `SELECT 1`
         */
        template<class St, class T>
        struct column_result_t<St, T, typename std::enable_if<std::is_arithmetic<T>::value>::type> {
            using type = T;
        };
        
        /**
         *  Result for the most simple queries like `SELECT 'ototo'`
         */
        template<class St>
        struct column_result_t<St, const char*, void> {
            using type = std::string;
        };
        
        template<class St, class T, class E>
        struct column_result_t<St, as_t<T, E>, void> : column_result_t<St, typename std::decay<E>::type, void> {};
        
        template<class St, class T>
        struct column_result_t<St, asterisk_t<T>, void> {
            using type = typename storage_traits::storage_mapped_columns<St, T>::type;
        };
        
        template<class St, class T, class E>
        struct column_result_t<St, conditions::cast_t<T, E>, void> {
            using type = T;
        };
    }
}
