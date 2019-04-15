
#include <sqlite_orm/sqlite_orm.h>
#include <iostream>
#include <memory>
#include <string>

using std::cout;
using std::endl;

/**
 *  Gender is gonna me stored as a nullable TEXT.
 *  None is gonna be NULL,
 *  Male - 'male' string,
 *  Female - 'female' string
 */
enum class Gender {
    None,
    Male,
    Female,
};

std::shared_ptr<std::string> GenderToString(Gender gender) {
    switch(gender){
        case Gender::Female:return std::make_shared<std::string>("female");
        case Gender::Male:return std::make_shared<std::string>("male");
        case Gender::None:return {};
    }
}

std::shared_ptr<Gender> GenderFromString(const std::string &s) {
    if(s == "female") {
        return std::make_shared<Gender>(Gender::Female);
    }else if(s == "male") {
        return std::make_shared<Gender>(Gender::Male);
    }
    return nullptr;
}

struct User {
    int id;
    std::string name;
    Gender gender;
};

namespace sqlite_orm {

    template<>
    struct type_printer<Gender> : public text_printer {};

    template<>
    struct statement_binder<Gender> {

        int bind(sqlite3_stmt *stmt, int index, const Gender &value) {
            if(auto str = GenderToString(value)){
                return statement_binder<std::string>().bind(stmt, index, *str);
            }else{
                return statement_binder<std::nullptr_t>().bind(stmt, index, nullptr);
            }
        }
    };

    template<>
    struct field_printer<Gender> {
        std::string operator()(const Gender &t) const {
            if(auto res = GenderToString(t)){
                return *res;
            }else{
                return "None";
            }
        }
    };

    template<>
    struct row_extractor<Gender> {
        Gender extract(const char *row_value) {
            if(row_value){
                if(auto gender = GenderFromString(row_value)){
                    return *gender;
                }else{
                    throw std::runtime_error("incorrect gender string (" + std::string(row_value) + ")");
                }
            }else{
                return Gender::None;
            }
        }

        Gender extract(sqlite3_stmt *stmt, int columnIndex) {
            auto str = sqlite3_column_text(stmt, columnIndex);
            return this->extract((const char*)str);
        }
    };

    /**
     *  This is where sqlite_orm lib understands that your type is nullable - by
     *  specializing type_is_nullable<T> and deriving from std::true_type.
     */
    template<>
    struct type_is_nullable<Gender> : public std::true_type {

        //  this function must return whether value null or not (false is null). Don't forget to implement it
        bool operator()(const Gender &g) const {
            return g != Gender::None;
        }
    };
}

int main(int argc, char **argv) {
    using namespace sqlite_orm;
    auto storage = make_storage("nullable_enum.sqlite",
                                make_table("users",
                                           make_column("id",
                                                       &User::id,
                                                       primary_key()),
                                           make_column("name",
                                                       &User::name),
                                           make_column("gender",
                                                       &User::gender)));
    storage.sync_schema();
    storage.remove_all<User>();

    storage.insert(User{
        -1,
        "Creeper",
        Gender::Male,
    });
    storage.insert(User{
        -1,
        "Witch",
        Gender::Female,
    });
    storage.insert(User{
        -1,
        "Enderman",
        Gender::None,
    });

    cout << "All users :" << endl;
    for(auto &user : storage.iterate<User>()) {
        cout << storage.dump(user) << endl;
    }

    auto allWithNoneGender = storage.get_all<User>(where(is_null(&User::gender)));
    cout << "allWithNoneGender = " << allWithNoneGender.size() << endl;
    for(auto &user : allWithNoneGender) {
        cout << storage.dump(user) << endl;
    }

    auto allWithGender = storage.get_all<User>(where(is_not_null(&User::gender)));
    cout << "allWithGender = " << allWithGender.size() << endl;
    for(auto &user : allWithGender) {
        cout << storage.dump(user) << endl;
    }

    return 0;
}
