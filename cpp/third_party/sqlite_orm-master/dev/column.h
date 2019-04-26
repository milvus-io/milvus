#pragma once

#include <tuple>    //  std::tuple
#include <string>   //  std::string
#include <memory>   //  std::unique_ptr
#include <type_traits>  //  std::true_type, std::false_type, std::is_same, std::enable_if

#include "type_is_nullable.h"
#include "tuple_helper.h"
#include "default_value_extractor.h"
#include "constraints.h"

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
            std::unique_ptr<std::string> default_value() {
                std::unique_ptr<std::string> res;
                tuple_helper::iterator<std::tuple_size<constraints_type>::value - 1, Op...>()(constraints, [&res](auto &v){
                    auto dft = internal::default_value_extractor()(v);
                    if(dft){
                        res = std::move(dft);
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
            
            static constexpr const bool returns_lvalue = false;
        };
        
        template<class O, class T>
        struct getter_traits<getter_by_value<O, T>> {
            using object_type = O;
            using field_type = T;
            
            static constexpr const bool returns_lvalue = false;
        };
        
        template<class O, class T>
        struct getter_traits<getter_by_ref_const<O, T>> {
            using object_type = O;
            using field_type = T;
            
            static constexpr const bool returns_lvalue = true;
        };
        
        template<class O, class T>
        struct getter_traits<getter_by_ref<O, T>> {
            using object_type = O;
            using field_type = T;
            
            static constexpr const bool returns_lvalue = true;
        };
        
        template<class O, class T>
        struct getter_traits<getter_by_const_ref_const<O, T>> {
            using object_type = O;
            using field_type = T;
            
            static constexpr const bool returns_lvalue = true;
        };
        
        template<class O, class T>
        struct getter_traits<getter_by_const_ref<O, T>> {
            using object_type = O;
            using field_type = T;
            
            static constexpr const bool returns_lvalue = true;
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
