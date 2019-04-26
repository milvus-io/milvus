#pragma once

#include "conditions.h"

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
