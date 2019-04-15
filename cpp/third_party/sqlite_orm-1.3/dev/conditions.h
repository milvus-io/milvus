#pragma once

#include <string>   //  std::string

#include "collate_argument.h"
#include "constraints.h"

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
