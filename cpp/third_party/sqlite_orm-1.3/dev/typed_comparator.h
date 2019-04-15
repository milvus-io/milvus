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
