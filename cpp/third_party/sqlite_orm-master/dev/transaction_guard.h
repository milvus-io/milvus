#pragma once

namespace sqlite_orm {
    
    namespace internal {
        
        /**
         *  Class used as a guard for a transaction. Calls `ROLLBACK` in destructor.
         *  Has explicit `commit()` and `rollback()` functions. After explicit function is fired
         *  guard won't do anything in d-tor. Also you can set `commit_on_destroy` to true to
         *  make it call `COMMIT` on destroy.
         *  S - storage type
         */
        template<class S>
        struct transaction_guard_t {
            using storage_type = S;
            
            /**
             *  This is a public lever to tell a guard what it must do in its destructor
             *  if `gotta_fire` is true
             */
            bool commit_on_destroy = false;
            
            transaction_guard_t(storage_type &s): storage(s) {}
            
            ~transaction_guard_t() {
                if(this->gotta_fire){
                    if(!this->commit_on_destroy){
                        this->storage.rollback();
                    }else{
                        this->storage.commit();
                    }
                }
            }
            
            /**
             *  Call `COMMIT` explicitly. After this call
             *  guard will not call `COMMIT` or `ROLLBACK`
             *  in its destructor.
             */
            void commit() {
                this->storage.commit();
                this->gotta_fire = false;
            }
            
            /**
             *  Call `ROLLBACK` explicitly. After this call
             *  guard will not call `COMMIT` or `ROLLBACK`
             *  in its destructor.
             */
            void rollback() {
                this->storage.rollback();
                this->gotta_fire = false;
            }
            
        protected:
            storage_type &storage;
            bool gotta_fire = true;
        };
    }
}
