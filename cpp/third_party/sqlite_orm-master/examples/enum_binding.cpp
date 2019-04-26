
#include <sqlite_orm/sqlite_orm.h>
#include <iostream>
#include <memory>

using std::cout;
using std::endl;

/**
 *  This is a enum we want to map to our sqlite db.
 *  First we have to decide what db type enum must have.
 *  Let's make it `TEXT`: Gender::Male will store as 'male' string
 *  and Gender::Female as 'female' string.
 */
enum class Gender {
    Male,
    Female,
};

//  This is a class with field `gender` with type `Gender`.
struct SuperHero {
    int id;
    std::string name;
    Gender gender;
};

//  also we need transform functions to make string from enum..
std::string GenderToString(Gender gender) {
    switch(gender){
        case Gender::Female:return "female";
        case Gender::Male:return "male";
    }
}

/**
 *  and enum from string. This function has nullable result cause
 *  string can be neither `male` nor `female`. Of course we
 *  won't allow this to happed but as developers we must have
 *  a scenario for this case.
 *  These functions are just helpers. They will be called from several places
 *  that's why I placed it separatedly. You can use any transformation type/form
 *  (for example BETTER_ENUM https://github.com/aantron/better-enums)
 */
std::unique_ptr<Gender> GenderFromString(const std::string &s) {
    if(s == "female") {
        return std::make_unique<Gender>(Gender::Female);
    }else if(s == "male") {
        return std::make_unique<Gender>(Gender::Male);
    }
    return nullptr;
}

/**
 *  This is where magic happens. To tell sqlite_orm how to act
 *  with Gender enum we have to create a few service classes
 *  specializations (traits) in sqlite_orm namespace.
 */
namespace sqlite_orm {

    /**
     *  First of all is a type_printer template class.
     *  It is responsible for sqlite type string representation.
     *  We want Gender to be `TEXT` so let's just derive from
     *  text_printer. Also there are other printers: real_printer and
     *  integer_printer. We must use them if we want to map our type to `REAL` (double/float)
     *  or `INTEGER` (int/long/short etc) respectively.
     */
    template<>
    struct type_printer<Gender> : public text_printer {};

    /**
     *  This is a binder class. It is used to bind c++ values to sqlite queries.
     *  Here we have to create gender string representation and bind it as string.
     *  Any statement_binder specialization must have `int bind(sqlite3_stmt*, int, const T&)` function
     *  which returns bind result. Also you can call any of `sqlite3_bind_*` functions directly.
     *  More here https://www.sqlite.org/c3ref/bind_blob.html
     */
    template<>
    struct statement_binder<Gender> {

        int bind(sqlite3_stmt *stmt, int index, const Gender &value) {
            return statement_binder<std::string>().bind(stmt, index, GenderToString(value));
            //  or return sqlite3_bind_text(stmt, index++, GenderToString(value).c_str(), -1, SQLITE_TRANSIENT);
        }
    };

    /**
     *  field_printer is used in `dump` and `where` functions. Here we have to create
     *  a string from mapped object.
     */
    template<>
    struct field_printer<Gender> {
        std::string operator()(const Gender &t) const {
            return GenderToString(t);
        }
    };

    /**
     *  This is a reverse operation: here we have to specify a way to transform string received from
     *  database to our Gender object. Here we call `GenderFromString` and throw `std::runtime_error` if it returns
     *  nullptr. Every `row_extractor` specialization must have `extract(const char*)` and `extract(sqlite3_stmt *stmt, int columnIndex)`
     *  functions which return a mapped type value.
     */
    template<>
    struct row_extractor<Gender> {
        Gender extract(const char *row_value) {
            if(auto gender = GenderFromString(row_value)){
                return *gender;
            }else{
                throw std::runtime_error("incorrect gender string (" + std::string(row_value) + ")");
            }
        }

        Gender extract(sqlite3_stmt *stmt, int columnIndex) {
            auto str = sqlite3_column_text(stmt, columnIndex);
            return this->extract((const char*)str);
        }
    };
}

int main(int/* argc*/, char **/*argv*/) {
    using namespace sqlite_orm;
    auto storage = make_storage("",
                                make_table("superheros",
                                           make_column("id", &SuperHero::id, primary_key()),
                                           make_column("name", &SuperHero::name),
                                           make_column("gender", &SuperHero::gender)));
    storage.sync_schema();
    storage.remove_all<SuperHero>();

    //  insert Batman (male)
    storage.insert(SuperHero{ -1, "Batman", Gender::Male });

    //  get Batman by name
    auto batman = storage.get_all<SuperHero>(where(c(&SuperHero::name) == "Batman")).front();

    //  print Batman
    cout << "batman = " << storage.dump(batman) << endl;

    //  insert Wonder woman
    storage.insert(SuperHero{ -1, "Wonder woman", Gender::Female });

    //  get all superheros
    auto allSuperHeros = storage.get_all<SuperHero>();

    //  print all superheros
    cout << "allSuperHeros = " << allSuperHeros.size() << endl;
    for(auto &superHero : allSuperHeros) {
        cout << storage.dump(superHero) << endl;
    }

    //  insert a second male (Superman)
    storage.insert(SuperHero{ -1, "Superman", Gender::Male});

    //  get all male superheros (2 expected)
    auto males = storage.get_all<SuperHero>(where(c(&SuperHero::gender) == Gender::Male));
    cout << "males = " << males.size() << endl;
    for(auto &superHero : males) {
        cout << storage.dump(superHero) << endl;
    }

    //  get all female superheros (1 expected)
    auto females = storage.get_all<SuperHero>(where(c(&SuperHero::gender) == Gender::Female));
    cout << "females = " << females.size() << endl;
    for(auto &superHero : females) {
        cout << storage.dump(superHero) << endl;
    }

    return 0;
}
