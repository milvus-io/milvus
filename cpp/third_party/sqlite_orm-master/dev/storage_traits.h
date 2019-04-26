#include <type_traits>  //  std::is_same, std::enable_if, std::true_type, std::false_type, std::integral_constant
#include <tuple>    //  std::tuple

namespace sqlite_orm {
    
    namespace internal {
        
        template<class ...Ts>
        struct storage_impl;
        
        template<typename... Args>
        struct table_impl;
        
        namespace storage_traits {
            
            /**
             *  S - storage_impl type
             *  T - mapped or not mapped data type
             */
            template<class S, class T, class SFINAE = void>
            struct type_is_mapped_impl;
            
            /**
             *  S - storage
             *  T - mapped or not mapped data type
             */
            template<class S, class T>
            struct type_is_mapped : type_is_mapped_impl<typename S::impl_type, T> {};
            
            /**
             *  Final specialisation
             */
            template<class T>
            struct type_is_mapped_impl<storage_impl<>, T, void> : std::false_type {};
            
            template<class S, class T>
            struct type_is_mapped_impl<S, T, typename std::enable_if<std::is_same<T, typename S::table_type::object_type>::value>::type> : std::true_type {};
            
            template<class S, class T>
            struct type_is_mapped_impl<S, T, typename std::enable_if<!std::is_same<T, typename S::table_type::object_type>::value>::type>
            : type_is_mapped_impl<typename S::super, T> {};
            
            
            /**
             *  S - storage_impl type
             *  T - mapped or not mapped data type
             */
            template<class S, class T, class SFINAE = void>
            struct storage_columns_count_impl;
            
            /**
             *  S - storage
             *  T - mapped or not mapped data type
             */
            template<class S, class T>
            struct storage_columns_count : storage_columns_count_impl<typename S::impl_type, T> {};
            
            /**
             *  Final specialisation
             */
            template<class T>
            struct storage_columns_count_impl<storage_impl<>, T, void> : std::integral_constant<int, 0> {};
            
            template<class S, class T>
            struct storage_columns_count_impl<S, T,  typename std::enable_if<std::is_same<T, typename S::table_type::object_type>::value>::type> : std::integral_constant<int, S::table_type::columns_count> {};
            
            template<class S, class T>
            struct storage_columns_count_impl<S, T,  typename std::enable_if<!std::is_same<T, typename S::table_type::object_type>::value>::type> : storage_columns_count_impl<typename S::super, T> {};
            
            
            /**
             *  T - table_impl type.
             */
            template<class T>
            struct table_impl_types;
            
            /**
             *  type is std::tuple of field types of mapped colums.
             */
            template<typename... Args>
            struct table_impl_types<table_impl<Args...>> {
                using type = std::tuple<typename Args::field_type...>;
            };
            
            
            /**
             *  S - storage_impl type
             *  T - mapped or not mapped data type
             */
            template<class S, class T, class SFINAE = void>
            struct storage_mapped_columns_impl;
            
            /**
             *  S - storage
             *  T - mapped or not mapped data type
             */
            template<class S, class T>
            struct storage_mapped_columns : storage_mapped_columns_impl<typename S::impl_type, T> {};
            
            /**
             *  Final specialisation
             */
            template<class T>
            struct storage_mapped_columns_impl<storage_impl<>, T, void> {
                using type = std::tuple<>;
            };
            
            template<class S, class T>
            struct storage_mapped_columns_impl<S, T, typename std::enable_if<std::is_same<T, typename S::table_type::object_type>::value>::type> {
                using table_type = typename S::table_type;
                using table_impl_type = typename table_type::impl_type;
                using type = typename table_impl_types<table_impl_type>::type;
            };
            
            template<class S, class T>
            struct storage_mapped_columns_impl<S, T, typename std::enable_if<!std::is_same<T, typename S::table_type::object_type>::value>::type> : storage_mapped_columns_impl<typename S::super, T> {};
            
        }
    }
}
