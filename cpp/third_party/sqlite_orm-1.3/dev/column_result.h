#pragma once

#include <type_traits>  //  std::enable_if, std::is_same
#include <tuple>    //  std::tuple

#include "core_functions.h"
#include "aggregate_functions.h"
#include "select_constraints.h"
#include "operators.h"
#include "rowid.h"
#include "alias.h"
#include "column.h"

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
