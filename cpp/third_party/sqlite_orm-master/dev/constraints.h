#pragma once

#include <string>   //  std::string
#include <tuple>    //  std::tuple, std::make_tuple
#include <sstream>  //  std::stringstream
#include <type_traits>  //  std::is_base_of, std::false_type, std::true_type
#include <ostream>  //  std::ostream

namespace sqlite_orm {
    
    namespace constraints {
        
        /**
         *  AUTOINCREMENT constraint class.
         */
        struct autoincrement_t {
            
            operator std::string() const {
                return "AUTOINCREMENT";
            }
        };
        
        /**
         *  PRIMARY KEY constraint class.
         *  Cs is parameter pack which contains columns (member pointer and/or function pointers). Can be empty when used withen `make_column` function.
         */
        template<class ...Cs>
        struct primary_key_t {
            std::tuple<Cs...> columns;
            enum class order_by {
                unspecified,
                ascending,
                descending,
            };
            order_by asc_option = order_by::unspecified;
            
            primary_key_t(decltype(columns) c):columns(std::move(c)){}
            
            using field_type = void;    //  for column iteration. Better be deleted
            using constraints_type = std::tuple<>;
            
            operator std::string() const {
                std::string res = "PRIMARY KEY";
                switch(this->asc_option){
                    case order_by::ascending:
                        res += " ASC";
                        break;
                    case order_by::descending:
                        res += " DESC";
                        break;
                    default:
                        break;
                }
                return res;
            }
            
            primary_key_t<Cs...> asc() const {
                auto res = *this;
                res.asc_option = order_by::ascending;
                return res;
            }
            
            primary_key_t<Cs...> desc() const {
                auto res = *this;
                res.asc_option = order_by::descending;
                return res;
            }
        };
        
        /**
         *  UNIQUE constraint class.
         */
        struct unique_t {
            
            operator std::string() const {
                return "UNIQUE";
            }
        };
        
        /**
         *  DEFAULT constraint class.
         *  T is a value type.
         */
        template<class T>
        struct default_t {
            using value_type = T;
            
            value_type value;
            
            operator std::string() const {
                std::stringstream ss;
                ss << "DEFAULT ";
                auto needQuotes = std::is_base_of<text_printer, type_printer<T>>::value;
                if(needQuotes){
                    ss << "'";
                }
                ss << this->value;
                if(needQuotes){
                    ss << "'";
                }
                return ss.str();
            }
        };
        
#if SQLITE_VERSION_NUMBER >= 3006019
        
        /**
         *  FOREIGN KEY constraint class.
         *  Cs are columns which has foreign key
         *  Rs are column which C references to
         *  Available in SQLite 3.6.19 or higher
         */
        
        template<class A, class B>
        struct foreign_key_t;
        
        enum class foreign_key_action {
            none,   //  not specified
            no_action,
            restrict_,
            set_null,
            set_default,
            cascade,
        };
        
        inline std::ostream &operator<<(std::ostream &os, foreign_key_action action) {
            switch(action){
                case decltype(action)::no_action:
                    os << "NO ACTION";
                    break;
                case decltype(action)::restrict_:
                    os << "RESTRICT";
                    break;
                case decltype(action)::set_null:
                    os << "SET NULL";
                    break;
                case decltype(action)::set_default:
                    os << "SET DEFAULT";
                    break;
                case decltype(action)::cascade:
                    os << "CASCADE";
                    break;
                case decltype(action)::none:
                    break;
            }
            return os;
        }
        
        /**
         *  F - foreign key class
         */
        template<class F>
        struct on_update_delete_t {
            using foreign_key_type = F;
            
            const foreign_key_type &fk;
            const bool update;  //  true if update and false if delete
            
            on_update_delete_t(decltype(fk) fk_, decltype(update) update_, foreign_key_action action_) : fk(fk_), update(update_), _action(action_) {}
            
            foreign_key_action _action = foreign_key_action::none;
            
            foreign_key_type no_action() const {
                auto res = this->fk;
                if(update){
                    res.on_update._action = foreign_key_action::no_action;
                }else{
                    res.on_delete._action = foreign_key_action::no_action;
                }
                return res;
            }
            
            foreign_key_type restrict_() const {
                auto res = this->fk;
                if(update){
                    res.on_update._action = foreign_key_action::restrict_;
                }else{
                    res.on_delete._action = foreign_key_action::restrict_;
                }
                return res;
            }
            
            foreign_key_type set_null() const {
                auto res = this->fk;
                if(update){
                    res.on_update._action = foreign_key_action::set_null;
                }else{
                    res.on_delete._action = foreign_key_action::set_null;
                }
                return res;
            }
            
            foreign_key_type set_default() const {
                auto res = this->fk;
                if(update){
                    res.on_update._action = foreign_key_action::set_default;
                }else{
                    res.on_delete._action = foreign_key_action::set_default;
                }
                return res;
            }
            
            foreign_key_type cascade() const {
                auto res = this->fk;
                if(update){
                    res.on_update._action = foreign_key_action::cascade;
                }else{
                    res.on_delete._action = foreign_key_action::cascade;
                }
                return res;
            }
            
            operator bool() const {
                return this->_action != decltype(this->_action)::none;
            }
            
            operator std::string() const {
                if(this->update){
                    return "ON UPDATE";
                }else{
                    return "ON DELETE";
                }
            }
        };
        
        template<class ...Cs, class ...Rs>
        struct foreign_key_t<std::tuple<Cs...>, std::tuple<Rs...>> {
            using columns_type = std::tuple<Cs...>;
            using references_type = std::tuple<Rs...>;
            using self = foreign_key_t<columns_type, references_type>;
            
            columns_type columns;
            references_type references;
            
            on_update_delete_t<self> on_update;
            on_update_delete_t<self> on_delete;
            
            static_assert(std::tuple_size<columns_type>::value == std::tuple_size<references_type>::value, "Columns size must be equal to references tuple");
            
            foreign_key_t(columns_type columns_, references_type references_):
            columns(std::move(columns_)),
            references(std::move(references_)),
            on_update(*this, true, foreign_key_action::none),
            on_delete(*this, false, foreign_key_action::none)
            {}
            
            foreign_key_t(const self &other):
            columns(other.columns),
            references(other.references),
            on_update(*this, true, other.on_update._action),
            on_delete(*this, false, other.on_delete._action)
            {}
            
            self &operator=(const self &other) {
                this->columns = other.columns;
                this->references = other.references;
                this->on_update = {*this, true, other.on_update._action};
                this->on_delete = {*this, false, other.on_delete._action};
                return *this;
            }
            
            using field_type = void;    //  for column iteration. Better be deleted
            using constraints_type = std::tuple<>;
            
            template<class L>
            void for_each_column(L) {}
            
            template<class ...Opts>
            constexpr bool has_every() const  {
                return false;
            }
        };
        
        /**
         *  Cs can be a class member pointer, a getter function member pointer or setter
         *  func member pointer
         *  Available in SQLite 3.6.19 or higher
         */
        template<class ...Cs>
        struct foreign_key_intermediate_t {
            using tuple_type = std::tuple<Cs...>;
            
            tuple_type columns;
            
            foreign_key_intermediate_t(tuple_type columns_): columns(std::move(columns_)) {}
            
            template<class ...Rs>
            foreign_key_t<std::tuple<Cs...>, std::tuple<Rs...>> references(Rs ...references) {
                using ret_type = foreign_key_t<std::tuple<Cs...>, std::tuple<Rs...>>;
                return ret_type(std::move(this->columns), std::make_tuple(std::forward<Rs>(references)...));
            }
        };
#endif
        
        struct collate_t {
            internal::collate_argument argument;
            
            collate_t(internal::collate_argument argument_): argument(argument_) {}
            
            operator std::string() const {
                std::string res = "COLLATE " + string_from_collate_argument(this->argument);
                return res;
            }
            
            static std::string string_from_collate_argument(internal::collate_argument argument){
                switch(argument){
                    case decltype(argument)::binary: return "BINARY";
                    case decltype(argument)::nocase: return "NOCASE";
                    case decltype(argument)::rtrim: return "RTRIM";
                }
            }
        };
        
        template<class T>
        struct is_constraint : std::false_type {};
        
        template<>
        struct is_constraint<autoincrement_t> : std::true_type {};
        
        template<class ...Cs>
        struct is_constraint<primary_key_t<Cs...>> : std::true_type {};
        
        template<>
        struct is_constraint<unique_t> : std::true_type {};
        
        template<class T>
        struct is_constraint<default_t<T>> : std::true_type {};
        
        template<class C, class R>
        struct is_constraint<foreign_key_t<C, R>> : std::true_type {};
        
        template<>
        struct is_constraint<collate_t> : std::true_type {};
        
        template<class ...Args>
        struct constraints_size;
        
        template<>
        struct constraints_size<> {
            static constexpr const int value = 0;
        };
        
        template<class H, class ...Args>
        struct constraints_size<H, Args...> {
            static constexpr const int value = is_constraint<H>::value + constraints_size<Args...>::value;
        };
    }
    
#if SQLITE_VERSION_NUMBER >= 3006019
    
    /**
     *  FOREIGN KEY constraint construction function that takes member pointer as argument
     *  Available in SQLite 3.6.19 or higher
     */
    template<class ...Cs>
    constraints::foreign_key_intermediate_t<Cs...> foreign_key(Cs ...columns) {
        return {std::make_tuple(std::forward<Cs>(columns)...)};
    }
#endif
    
    /**
     *  UNIQUE constraint builder function.
     */
    inline constraints::unique_t unique() {
        return {};
    }
    
    inline constraints::autoincrement_t autoincrement() {
        return {};
    }
    
    template<class ...Cs>
    inline constraints::primary_key_t<Cs...> primary_key(Cs ...cs) {
        using ret_type = constraints::primary_key_t<Cs...>;
        return ret_type(std::make_tuple(cs...));
    }
    
    template<class T>
    constraints::default_t<T> default_value(T t) {
        return {t};
    }
    
    inline constraints::collate_t collate_nocase() {
        return {internal::collate_argument::nocase};
    }
    
    inline constraints::collate_t collate_binary() {
        return {internal::collate_argument::binary};
    }
    
    inline constraints::collate_t collate_rtrim() {
        return {internal::collate_argument::rtrim};
    }
    
    namespace internal {
        
        /**
         *  FOREIGN KEY traits. Common case
         */
        template<class T>
        struct is_foreign_key : std::false_type {};
        
        /**
         *  FOREIGN KEY traits. Specialized case
         */
        template<class C, class R>
        struct is_foreign_key<constraints::foreign_key_t<C, R>> : std::true_type {};
        
        /**
         *  PRIMARY KEY traits. Common case
         */
        template<class T>
        struct is_primary_key : public std::false_type {};
        
        /**
         *  PRIMARY KEY traits. Specialized case
         */
        template<class ...Cs>
        struct is_primary_key<constraints::primary_key_t<Cs...>> : public std::true_type {};
    }
    
}
