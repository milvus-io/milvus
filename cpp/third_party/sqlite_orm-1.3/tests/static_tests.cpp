
#include <sqlite_orm/sqlite_orm.h>
#include <tuple>
#include <type_traits>

struct User {
    int id;
    
    const int& getIdByRefConst() const {
        return this->id;
    }
    
    const int& getIdByRef() {
        return this->id;
    }
    
    int getIdByValConst() const {
        return this->id;
    }
    
    void setIdByVal(int id) {
        this->id = id;
    }
    
    void setIdByConstRef(const int &id) {
        this->id = id;
    }
    
    void setIdByRef(int &id) {
        this->id = id;
    }
};

struct Object {
    int id;
};

struct Token : Object {
    
};

int main() {
    using namespace sqlite_orm;
    
    {
        using column_type = decltype(make_column("id", &User::id));
        static_assert(std::tuple_size<column_type::constraints_type>::value == 0, "Incorrect constraints_type size");
        static_assert(std::is_same<column_type::object_type, User>::value, "Incorrect object_type");
        static_assert(std::is_same<column_type::field_type, int>::value, "Incorrect field_type");
        static_assert(std::is_same<column_type::member_pointer_t, int User::*>::value, "Incorrect member pointer type");
        static_assert(std::is_same<column_type::getter_type, const int&(User::*)() const>::value, "Incorrect getter_type");
        static_assert(std::is_same<column_type::setter_type, void(User::*)(int)>::value, "Incorrect setter_type");
    }
    {
        using column_type = decltype(make_column("id", &User::getIdByRefConst, &User::setIdByVal));
        static_assert(std::tuple_size<column_type::constraints_type>::value == 0, "Incorrect constraints_type size");
        static_assert(std::is_same<column_type::object_type, User>::value, "Incorrect object_type");
        static_assert(std::is_same<column_type::field_type, int>::value, "Incorrect field_type");
        static_assert(std::is_same<column_type::member_pointer_t, int User::*>::value, "Incorrect member pointer type");
        static_assert(std::is_same<column_type::getter_type, const int&(User::*)() const>::value, "Incorrect getter_type");
        static_assert(std::is_same<column_type::setter_type, void(User::*)(int)>::value, "Incorrect setter_type");
    }
    {
        using column_type = decltype(make_column("id", &User::setIdByVal, &User::getIdByRefConst));
        static_assert(std::tuple_size<column_type::constraints_type>::value == 0, "Incorrect constraints_type size");
        static_assert(std::is_same<column_type::object_type, User>::value, "Incorrect object_type");
        static_assert(std::is_same<column_type::field_type, int>::value, "Incorrect field_type");
        static_assert(std::is_same<column_type::member_pointer_t, int User::*>::value, "Incorrect member pointer type");
        static_assert(std::is_same<column_type::getter_type, const int&(User::*)() const>::value, "Incorrect getter_type");
        static_assert(std::is_same<column_type::setter_type, void(User::*)(int)>::value, "Incorrect setter_type");
    }
    {
        using column_type = decltype(make_column("id", &User::getIdByRef, &User::setIdByConstRef));
        static_assert(std::tuple_size<column_type::constraints_type>::value == 0, "Incorrect constraints_type size");
        static_assert(std::is_same<column_type::object_type, User>::value, "Incorrect object_type");
        static_assert(std::is_same<column_type::field_type, int>::value, "Incorrect field_type");
        static_assert(std::is_same<column_type::member_pointer_t, int User::*>::value, "Incorrect member pointer type");
        static_assert(std::is_same<column_type::getter_type, const int&(User::*)()>::value, "Incorrect getter_type");
        static_assert(std::is_same<column_type::setter_type, void(User::*)(const int&)>::value, "Incorrect setter_type");
    }
    {
        using column_type = decltype(make_column("id", &User::setIdByConstRef, &User::getIdByRef));
        static_assert(std::tuple_size<column_type::constraints_type>::value == 0, "Incorrect constraints_type size");
        static_assert(std::is_same<column_type::object_type, User>::value, "Incorrect object_type");
        static_assert(std::is_same<column_type::field_type, int>::value, "Incorrect field_type");
        static_assert(std::is_same<column_type::member_pointer_t, int User::*>::value, "Incorrect member pointer type");
        static_assert(std::is_same<column_type::getter_type, const int&(User::*)()>::value, "Incorrect getter_type");
        static_assert(std::is_same<column_type::setter_type, void(User::*)(const int&)>::value, "Incorrect setter_type");
    }
    {
        using column_type = decltype(make_column("id", &User::getIdByValConst, &User::setIdByRef));
        static_assert(std::tuple_size<column_type::constraints_type>::value == 0, "Incorrect constraints_type size");
        static_assert(std::is_same<column_type::object_type, User>::value, "Incorrect object_type");
        static_assert(std::is_same<column_type::field_type, int>::value, "Incorrect field_type");
        static_assert(std::is_same<column_type::member_pointer_t, int User::*>::value, "Incorrect member pointer type");
        static_assert(std::is_same<column_type::getter_type, int(User::*)() const>::value, "Incorrect getter_type");
        static_assert(std::is_same<column_type::setter_type, void(User::*)(int&)>::value, "Incorrect setter_type");
    }
    {
        using column_type = decltype(make_column("id", &User::setIdByRef, &User::getIdByValConst));
        static_assert(std::tuple_size<column_type::constraints_type>::value == 0, "Incorrect constraints_type size");
        static_assert(std::is_same<column_type::object_type, User>::value, "Incorrect object_type");
        static_assert(std::is_same<column_type::field_type, int>::value, "Incorrect field_type");
        static_assert(std::is_same<column_type::member_pointer_t, int User::*>::value, "Incorrect member pointer type");
        static_assert(std::is_same<column_type::getter_type, int(User::*)() const>::value, "Incorrect getter_type");
        static_assert(std::is_same<column_type::setter_type, void(User::*)(int&)>::value, "Incorrect setter_type");
    }
    {
        using column_type = decltype(column<Token>(&Token::id));
        static_assert(std::is_same<column_type::type, Token>::value, "Incorrect column type");
        using field_type = column_type::field_type;
        static_assert(std::is_same<field_type, decltype(&Object::id)>::value, "Incorrect field type");
        static_assert(std::is_same<internal::table_type<field_type>::type, Object>::value, "Incorrect mapped type");
        static_assert(std::is_same<internal::column_result_t<field_type>::type, int>::value, "Incorrect field type");
        static_assert(std::is_member_pointer<field_type>::value, "Field type is not a member pointer");
        static_assert(!std::is_member_function_pointer<field_type>::value, "Field type is not a member pointer");
    }
    {
        struct User {
            int id;
            std::string name;
            
            int getIdByValConst() const {
                return this->id;
            }
            
            void setIdByVal(int id) {
                this->id = id;
            }
            
            std::string getNameByVal() {
                return this->name;
            }
            
            void setNameByConstRef(const std::string &name) {
                this->name = name;
            }
            
            const int& getConstIdByRefConst() const {
                return this->id;
            }
            
            void setIdByRef(int &id) {
                this->id = id;
            }
            
            const std::string& getConstNameByRefConst() const {
                return this->name;
            }
            
            void setNameByRef(std::string &name) {
                this->name = std::move(name);
            }
        };
        const std::string filename = "static_tests.sqlite";
        auto storage0 = make_storage(filename,
                                     make_table("users",
                                                make_column("id", &User::id, primary_key()),
                                                make_column("name", &User::name)));
        auto storage1 = make_storage(filename,
                                     make_table("users",
                                                make_column("id", &User::getIdByValConst, &User::setIdByVal, primary_key()),
                                                make_column("name", &User::setNameByConstRef, &User::getNameByVal)));
        auto storage2 = make_storage(filename,
                                     make_table("users",
                                                make_column("id", &User::getConstIdByRefConst, &User::setIdByRef, primary_key()),
                                                make_column("name", &User::getConstNameByRefConst, &User::setNameByRef)));
        static_assert(std::is_same<decltype(storage0.max(&User::id))::element_type, int>::value, "Incorrect max value");
        static_assert(std::is_same<decltype(storage1.max(&User::getIdByValConst))::element_type, int>::value, "Incorrect max value");
        static_assert(std::is_same<decltype(storage1.max(&User::setIdByVal))::element_type, int>::value, "Incorrect max value");
        static_assert(std::is_same<decltype(storage2.max(&User::getConstIdByRefConst))::element_type, int>::value, "Incorrect max value");
        static_assert(std::is_same<decltype(storage2.max(&User::setIdByRef))::element_type, int>::value, "Incorrect max value");
        
        static_assert(std::is_same<decltype(storage0.max(&User::id, where(lesser_than(&User::id, 10))))::element_type, int>::value,
                      "Incorrect max value");
        static_assert(std::is_same<decltype(storage1.max(&User::getIdByValConst, where(lesser_than(&User::id, 10))))::element_type, int>::value,
                      "Incorrect max value");
        static_assert(std::is_same<decltype(storage1.max(&User::setIdByVal, where(lesser_than(&User::id, 10))))::element_type, int>::value,
                      "Incorrect max value");
        static_assert(std::is_same<decltype(storage2.max(&User::getConstIdByRefConst, where(lesser_than(&User::id, 10))))::element_type, int>::value,
                      "Incorrect max value");
        static_assert(std::is_same<decltype(storage2.max(&User::setIdByRef, where(lesser_than(&User::id, 10))))::element_type, int>::value,
                      "Incorrect max value");
        
        static_assert(std::is_same<decltype(storage0.min(&User::id))::element_type, int>::value, "Incorrect min value");
        static_assert(std::is_same<decltype(storage1.min(&User::getIdByValConst))::element_type, int>::value, "Incorrect min value");
        static_assert(std::is_same<decltype(storage1.min(&User::setIdByVal))::element_type, int>::value, "Incorrect min value");
        static_assert(std::is_same<decltype(storage2.min(&User::getConstIdByRefConst))::element_type, int>::value, "Incorrect min value");
        static_assert(std::is_same<decltype(storage2.min(&User::setIdByRef))::element_type, int>::value, "Incorrect min value");
        
        static_assert(std::is_same<decltype(storage0.min(&User::id, where(lesser_than(&User::id, 10))))::element_type, int>::value,
                      "Incorrect min value");
        static_assert(std::is_same<decltype(storage1.min(&User::getIdByValConst, where(lesser_than(&User::id, 10))))::element_type, int>::value,
                      "Incorrect min value");
        static_assert(std::is_same<decltype(storage1.min(&User::setIdByVal, where(lesser_than(&User::id, 10))))::element_type, int>::value,
                      "Incorrect min value");
        static_assert(std::is_same<decltype(storage2.min(&User::getConstIdByRefConst, where(lesser_than(&User::id, 10))))::element_type, int>::value,
                      "Incorrect min value");
        static_assert(std::is_same<decltype(storage2.min(&User::setIdByRef, where(lesser_than(&User::id, 10))))::element_type, int>::value,
                      "Incorrect min value");
        
        static_assert(std::is_same<decltype(storage0.sum(&User::id))::element_type, int>::value, "Incorrect sum value");
        static_assert(std::is_same<decltype(storage1.sum(&User::getIdByValConst))::element_type, int>::value, "Incorrect sum value");
        static_assert(std::is_same<decltype(storage1.sum(&User::setIdByVal))::element_type, int>::value, "Incorrect sum value");
        static_assert(std::is_same<decltype(storage2.sum(&User::getConstIdByRefConst))::element_type, int>::value, "Incorrect sum value");
        static_assert(std::is_same<decltype(storage2.sum(&User::setIdByRef))::element_type, int>::value, "Incorrect sum value");
        
        static_assert(std::is_same<decltype(storage0.sum(&User::id, where(lesser_than(&User::id, 10))))::element_type, int>::value,
                      "Incorrect sum value");
        static_assert(std::is_same<decltype(storage1.sum(&User::getIdByValConst, where(lesser_than(&User::id, 10))))::element_type, int>::value,
                      "Incorrect sum value");
        static_assert(std::is_same<decltype(storage1.sum(&User::setIdByVal, where(lesser_than(&User::id, 10))))::element_type, int>::value,
                      "Incorrect sum value");
        static_assert(std::is_same<decltype(storage2.sum(&User::getConstIdByRefConst, where(lesser_than(&User::id, 10))))::element_type, int>::value,
                      "Incorrect sum value");
        static_assert(std::is_same<decltype(storage2.sum(&User::setIdByRef, where(lesser_than(&User::id, 10))))::element_type, int>::value,
                      "Incorrect sum value");
        
    }
    
    auto storage = make_storage("",
                                make_table("users",
                                           make_column("id", &User::id)));
    //  this call is important - it tests compilation in inner storage_t::serialize_column_schema function
    storage.sync_schema();
    
    
    return 0;
}
