// Copyright 2019 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_JSON_TYPE_TRAITS_MACROS_HPP
#define JSONCONS_JSON_TYPE_TRAITS_MACROS_HPP

#include <array>
#include <string>
#include <vector>
#include <valarray>
#include <exception>
#include <cstring>
#include <utility>
#include <algorithm> // std::swap
#include <limits> // std::numeric_limits
#include <type_traits> // std::enable_if
#include <iterator> // std::iterator_traits, std::input_iterator_tag
#include <jsoncons/bignum.hpp>
#include <jsoncons/json_content_handler.hpp>
#include <jsoncons/detail/type_traits.hpp>
#include <string>
#include <tuple>
#include <map>
#include <functional>
#include <memory>

// This follows https://github.com/Loki-Astari/ThorsSerializer/blob/master/src/Serialize/Traits.h

#define JSONCONS_EXPAND(X) X    

#define JSONCONS_NARGS(...) JSONCONS_NARG_(__VA_ARGS__, 50, 49, 48, 47, 46, 45, 44, 43, 42, 41, 40, 39, 38, 37, 36, 35, 34, 33, 32, 31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
#define JSONCONS_NARG_(...) JSONCONS_EXPAND( JSONCONS_ARG_N(__VA_ARGS__) )
#define JSONCONS_ARG_N(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, e20, e21, e22, e23, e24, e25, e26, e27, e28, e29, e30, e31, e32, e33, e34, e35, e36, e37, e38, e39, e40, e41, e42, e43, e44, e45, e46, e47, e48, e49, e50, N, ...) N

#define JSONCONS_QUOTE(A) JSONCONS_EXPAND(#A)

#define JSONCONS_GENERATE(Call, TC, JVal, TVal, P2)          Call(TC, JVal, TVal, P2)

#define JSONCONS_REP_N(Call, TC, JVal, TVal, ...)            JSONCONS_REP_OF_N(Call, TC, JVal, TVal, JSONCONS_NARGS(__VA_ARGS__), __VA_ARGS__)
#define JSONCONS_REP_OF_N(Call, TC, JVal, TVal, Count, ...)  JSONCONS_REP_OF_N_(Call, TC, JVal, TVal, Count, __VA_ARGS__)
#define JSONCONS_REP_OF_N_(Call, TC, JVal, TVal, Count, ...) JSONCONS_EXPAND(JSONCONS_REP_OF_ ## Count(Call, TC, JVal, TVal, __VA_ARGS__))

#define JSONCONS_REP_OF_50(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_49(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_49(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_48(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_48(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_47(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_47(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_46(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_46(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_45(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_45(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_44(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_44(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_43(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_43(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_42(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_42(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_41(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_41(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_40(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_40(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_39(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_39(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_38(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_38(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_37(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_37(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_36(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_36(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_35(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_35(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_34(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_34(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_33(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_33(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_32(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_32(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_31(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_31(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_30(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_30(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_29(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_29(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_28(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_28(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_27(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_27(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_26(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_26(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_25(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_25(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_24(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_24(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_23(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_23(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_22(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_22(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_21(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_21(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_20(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_20(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_19(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_19(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_18(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_18(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_17(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_17(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_16(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_16(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_15(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_15(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_14(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_14(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_13(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_13(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_12(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_12(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_11(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_11(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_10(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_10(Call, TC, JVal, TVal, P2, ...)    JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_9(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_9(Call, TC, JVal, TVal, P2, ...)     JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_8(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_8(Call, TC, JVal, TVal, P2, ...)     JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_7(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_7(Call, TC, JVal, TVal, P2, ...)     JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_6(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_6(Call, TC, JVal, TVal, P2, ...)     JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_5(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_5(Call, TC, JVal, TVal, P2, ...)     JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_4(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_4(Call, TC, JVal, TVal, P2, ...)     JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_3(Call, TC, JVal, TVal, __VA_ARGS__))
#define JSONCONS_REP_OF_3(Call, TC, JVal, TVal, P2, ...)     JSONCONS_GENERATE(Call, TC, JVal, TVal ,P2) JSONCONS_EXPAND(JSONCONS_REP_OF_2(Call, TC, JVal, TVal, __VA_ARGS__)) 
#define JSONCONS_REP_OF_2(Call, TC, JVal, TVal, P2, ...)     JSONCONS_GENERATE(Call, TC, JVal, TVal, P2) JSONCONS_EXPAND(JSONCONS_REP_OF_1(Call, TC, JVal, TVal, __VA_ARGS__)) 
#define JSONCONS_REP_OF_1(Call, TC, JVal, TVal, P2)          JSONCONS_EXPAND(Call ## _LAST(TC, JVal, TVal, P2))

#define JSONCONS_IS(TC, JVal, TVal, Member) if (!(JVal).contains(JSONCONS_QUOTE(Member))) return false;
#define JSONCONS_IS_LAST(TC, JVal, TVal, Member) if (!(JVal).contains(JSONCONS_QUOTE(Member))) return false;

#define JSONCONS_TO_JSON(TC, JVal, TVal, Member) (JVal).try_emplace(JSONCONS_QUOTE(Member), TVal.Member);
#define JSONCONS_TO_JSON_LAST(TC, JVal, TVal, Member) (JVal).try_emplace(JSONCONS_QUOTE(Member), TVal.Member);

#define JSONCONS_AS(TC, JVal, TVal, Member) if ((JVal).contains(JSONCONS_QUOTE(Member))) {val.Member = (JVal).at(JSONCONS_QUOTE(Member)).template as<decltype(TVal.Member)>();}
#define JSONCONS_AS_LAST(TC, JVal, TVal, Member) if ((JVal).contains(JSONCONS_QUOTE(Member))) {val.Member = (JVal).at(JSONCONS_QUOTE(Member)).template as<decltype(TVal.Member)>();}

#define JSONCONS_MEMBER_TRAITS_DECL(ValueType, ...)  \
namespace jsoncons \
{ \
    template<class Json> \
    struct json_type_traits<Json, ValueType> \
    { \
        typedef ValueType value_type; \
        typedef typename Json::allocator_type allocator_type; \
        static bool is(const Json& j) noexcept \
        { \
            if (!j.is_object()) return false; \
            JSONCONS_REP_N(JSONCONS_IS, 0, j, void(), __VA_ARGS__)\
            return true; \
        } \
        static ValueType as(const Json& j) \
        { \
            ValueType val{}; \
            JSONCONS_REP_N(JSONCONS_AS, 0, j, val, __VA_ARGS__) \
            return val; \
        } \
        static Json to_json(const ValueType& val, allocator_type allocator=allocator_type()) \
        { \
            Json j(allocator); \
            JSONCONS_REP_N(JSONCONS_TO_JSON, 0, j, val, __VA_ARGS__) \
            return j; \
        } \
    }; \
} \
  /**/
 
#define JSONCONS_IS2(TC, JVal, TVal, Member) if (!(JVal).contains(JSONCONS_QUOTE(Member))) return false;
#define JSONCONS_IS2_LAST(TC, JVal, TVal, Member) if (!(JVal).contains(JSONCONS_QUOTE(Member))) return false;

#define JSONCONS_TO_JSON2(TC, JVal, TVal, Member) (JVal).try_emplace(JSONCONS_QUOTE(Member), TVal.Member() );
#define JSONCONS_TO_JSON2_LAST(TC, JVal, TVal, Member) (JVal).try_emplace(JSONCONS_QUOTE(Member), TVal.Member() );

#define JSONCONS_AS2(TC, JVal, TVal, Member) ((JVal).at(JSONCONS_QUOTE(Member))).template as<typename std::decay<decltype(((value_type*)nullptr)->Member())>::type>(),
#define JSONCONS_AS2_LAST(TC, JVal, TVal, Member) ((JVal).at(JSONCONS_QUOTE(Member))).template as<typename std::decay<decltype(((value_type*)nullptr)->Member())>::type>()
 
#define JSONCONS_GETTER_CTOR_TRAITS_DECL(ValueType, ...)  \
namespace jsoncons \
{ \
    template<class Json> \
    struct json_type_traits<Json, ValueType> \
    { \
        typedef ValueType value_type; \
        typedef typename Json::allocator_type allocator_type; \
        static bool is(const Json& j) noexcept \
        { \
            if (!j.is_object()) return false; \
            JSONCONS_REP_N(JSONCONS_IS2, 0, j, void(), __VA_ARGS__)\
            return true; \
        } \
        static value_type as(const Json& j) \
        { \
            return value_type ( JSONCONS_REP_N(JSONCONS_AS2, 0, j, void(), __VA_ARGS__) ); \
        } \
        static Json to_json(const value_type& val, allocator_type allocator=allocator_type()) \
        { \
            Json j(allocator); \
            JSONCONS_REP_N(JSONCONS_TO_JSON2, 0, j, val, __VA_ARGS__) \
            return j; \
        } \
    }; \
} \
  /**/

#define JSONCONS_TYPE_TRAITS_FRIEND \
    template <class JSON,class T,class Enable> \
    friend struct jsoncons::json_type_traits

#if !defined(JSONCONS_NO_DEPRECATED)
#define JSONCONS_TYPE_TRAITS_DECL JSONCONS_MEMBER_TRAITS_DECL
#endif

#endif
