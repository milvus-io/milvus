/**
 *  This example shows you how to create a storage with a tablw with a composite primary key
 *  and another table woth foreign key to first table's primary compisite key.
 */
#include <cstdio>
#include <ctime>

#include <string>
#include <iostream>

#include <sqlite_orm/sqlite_orm.h>

using std::cout;
using std::endl;

struct User {
    int id;
    std::string firstName;
    std::string lastName;
};

struct UserVisit {
    int userId;
    std::string userFirstName;
    time_t time;
};

int main() {
    using namespace sqlite_orm;
    
    auto storage = make_storage("",
                                make_table("users",
                                           make_column("id", &User::id),
                                           make_column("first_name", &User::firstName),
                                           make_column("last_name", &User::lastName),
                                           primary_key(&User::id, &User::firstName)),
                                make_table("visits",
                                           make_column("user_id", &UserVisit::userId),
                                           make_column("user_first_name", &UserVisit::userFirstName),
                                           make_column("time", &UserVisit::time),
                                           foreign_key(&UserVisit::userId, &UserVisit::userFirstName).references(&User::id, &User::firstName)));
    storage.sync_schema();
    
    storage.replace(User{
        1,
        "Bebe",
        "Rexha",
    });
    auto bebeRexha = storage.get<User>(1, "Bebe");
    cout << "bebeRexha = " << storage.dump(bebeRexha) << endl;
    auto bebeRexhaMaybe = storage.get_pointer<User>(1, "Bebe");
    try{
        //  2 and 'Drake' values will be ignored cause they are primary keys
        storage.insert(User{ 2, "Drake", "Singer" });
    }catch(std::system_error e){
        cout << "exception = " << e.what() << endl;
    }
    storage.replace(User{ 2, "The Weeknd", "Singer" });
    auto weeknd = storage.get<User>(2, "The Weeknd");
    cout << "weeknd = " << storage.dump(weeknd) << endl;
    return 0;
}
