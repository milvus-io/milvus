
#include <sqlite_orm/sqlite_orm.h>
#include <string>
#include <vector>
#include <iostream>
#include <cassert>

using std::cout;
using std::endl;

struct User {
    int id;
    std::string name;
    std::vector<char> hash; //  binary format
};

int main(int argc, char **argv) {
    using namespace sqlite_orm;
    auto storage = make_storage("blob.sqlite",
                                make_table("users",
                                           make_column("id", &User::id, primary_key()),
                                           make_column("name", &User::name),
                                           make_column("hash", &User::hash)));
    storage.sync_schema();
    storage.remove_all<User>();

    User alex{
        0,
        "Alex",
        { 0x10, 0x20, 0x30, 0x40 },
    };
    alex.id = storage.insert(alex);

    cout << "users count = " << storage.count<User>() << endl;

    cout << "alex = " << storage.dump(storage.get<User>(alex.id)) << endl;

    auto hash = storage.get<User>(alex.id).hash;
    assert(hash.size() == 4);
    assert(hash[0] == 0x10);
    assert(hash[1] == 0x20);
    assert(hash[2] == 0x30);
    assert(hash[3] == 0x40);

    return 0;
}
