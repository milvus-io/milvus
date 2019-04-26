#pragma once

#include <vector>   //  std::vector
#include <string>   //  std::string
#include <tuple>    //  std::tuple
#include <type_traits>  //  std::is_same, std::integral_constant, std::true_type, std::false_type

#include "column.h"
#include "tuple_helper.h"
#include "constraints.h"

namespace sqlite_orm {
    
    namespace internal {
        
        /**
         *  Common case for table_impl class.
         */
        template<typename... Args>
        struct table_impl;
        
        /**
         *  Final superclass for table_impl.
         */
        template<>
        struct table_impl<>{
            
            static constexpr const int columns_count = 0;
            
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
            
        };
        
        /**
         *  Regular table_impl class.
         */
        template<typename H, typename... T>
        struct table_impl<H, T...> : private table_impl<T...> {
            using column_type = H;
            using tail_types = std::tuple<T...>;
            using super = table_impl<T...>;
            
            table_impl(H h, T ...t) : super(t...), col(h) {}
            
            column_type col;
            
            static constexpr const int columns_count = 1 + super::columns_count;
            
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
        };
    }
}
