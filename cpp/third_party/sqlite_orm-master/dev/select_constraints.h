#pragma once

#include <string>   //  std::string

namespace sqlite_orm {
    
    namespace internal {

/*
 * This is because of bug in MSVC, for more information, please visit
 * https://stackoverflow.com/questions/34672441/stdis-base-of-for-template-classes/34672753#34672753
 */
#if defined(_MSC_VER)
       template <template <typename...> class Base, typename Derived>
       struct is_base_of_template_impl {
               template<typename... Ts>
               static constexpr std::true_type test(const Base<Ts...>*);

               static constexpr std::false_type test(...);

               using type = decltype(test(std::declval<Derived*>()));
       };

       template <typename Derived, template <typename...> class Base>
       using is_base_of_template = typename is_base_of_template_impl<Base, Derived>::type;

#else
        template <template <typename...> class C, typename...Ts>
        std::true_type is_base_of_template_impl(const C<Ts...>*);
        
        template <template <typename...> class C>
        std::false_type is_base_of_template_impl(...);
        
        template <typename T, template <typename...> class C>
        using is_base_of_template = decltype(is_base_of_template_impl<C>(std::declval<T*>()));

#endif
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
            bool highest_level = false;
        };
        
        /**
         *  Base for UNION, UNION ALL, EXCEPT and INTERSECT
         */
        template<class L, class R>
        struct compound_operator {
            using left_type = L;
            using right_type = R;
            
            left_type left;
            right_type right;
            
            compound_operator(left_type l, right_type r): left(std::move(l)), right(std::move(r)) {
                this->left.highest_level = true;
                this->right.highest_level = true;
            }
        };
        
        /**
         *  UNION object type.
         */
        template<class L, class R>
        struct union_t : public compound_operator<L, R> {
            using super = compound_operator<L, R>;
            using left_type = typename super::left_type;
            using right_type = typename super::right_type;
            
            bool all = false;
            
            union_t(left_type l, right_type r, decltype(all) all_): super(std::move(l), std::move(r)), all(all_) {}
            
            union_t(left_type l, right_type r): union_t(std::move(l), std::move(r), false) {}

            operator std::string() const {
                if(!this->all){
                    return "UNION";
                }else{
                    return "UNION ALL";
                }
            }
        };
        
        /**
         *  EXCEPT object type.
         */
        template<class L, class R>
        struct except_t : public compound_operator<L, R> {
            using super = compound_operator<L, R>;
            using left_type = typename super::left_type;
            using right_type = typename super::right_type;
            
            using super::super;
            
            operator std::string() const {
                return "EXCEPT";
            }
        };
        
        /**
         *  INTERSECT object type.
         */
        template<class L, class R>
        struct intersect_t : public compound_operator<L, R> {
            using super = compound_operator<L, R>;
            using left_type = typename super::left_type;
            using right_type = typename super::right_type;
            
            using super::super;
            
            operator std::string() const {
                return "INTERSECT";
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
        
        template<class T>
        struct asterisk_t {
            using type = T;
        };
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
     *  Public function for EXCEPT operator.
     *  lhs and rhs are subselect objects.
     *  Look through example in examples/except.cpp
     */
    template<class L, class R>
    internal::except_t<L, R> except(L lhs, R rhs) {
        return {std::move(lhs), std::move(rhs)};
    }
    
    template<class L, class R>
    internal::intersect_t<L, R> intersect(L lhs, R rhs) {
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
    
    template<class T>
    internal::asterisk_t<T> asterisk() {
        return {};
    }
}
