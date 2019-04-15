
#include <sqlite_orm/sqlite_orm.h>
#include <string>
#include <sys/time.h>
#include <iostream>

using std::cout;
using std::endl;

struct User {
    int id;
    std::string name;
    time_t createdAt;
};

struct Foo {
    std::string text;
    int baz;
};

int main(int argc, char** argv) {

    using namespace sqlite_orm;
    auto storage = make_storage("collate.sqlite",
                                make_table("users",
                                           make_column("id",
                                                       &User::id,
                                                       primary_key()),
                                           make_column("name",
                                                       &User::name),
                                           make_column("created_at",
                                                       &User::createdAt)),
                                make_table("foo",
                                           make_column("text",
                                                       &Foo::text,
                                                       collate_nocase()),
                                           make_column("baz",
                                                       &Foo::baz)));
    storage.sync_schema();
    storage.remove_all<User>();
    storage.remove_all<Foo>();

    storage.insert(User{
        0,
        "Lil Kim",
        time(nullptr),
    });
    storage.insert(User{
        0,
        "lil kim",
        time(nullptr),
    });
    storage.insert(User{
        0,
        "Nicki Minaj",
        time(nullptr),
    });

    //  SELECT COUNT(*) FROM users WHERE name = 'lil kim'
    auto preciseLilKimsCount = storage.count<User>(where(is_equal(&User::name, "lil kim")));
    cout << "preciseLilKimsCount = " << preciseLilKimsCount << endl;

    //  SELECT COUNT(*) FROM users WHERE name = 'lil kim' COLLATE NOCASE
    auto nocaseCount = storage.count<User>(where(is_equal(&User::name, "lil kim").collate_nocase()));
    cout << "nocaseCount = " << nocaseCount << endl;

    //  SELECT COUNT(*) FROM users
    cout << "total users count = " << storage.count<User>() << endl;

    storage.insert(Foo{
        "Touch",
        10,
    });
    storage.insert(Foo{
        "touch",
        20,
    });

    cout << "foo count = " << storage.count<Foo>(where(c(&Foo::text) == "touch")) << endl;

    //  SELECT id
    //  FROM users
    //  ORDER BY name COLLATE RTRIM ASC
    auto rows = storage.select(&User::id, order_by(&User::name).collate_rtrim().asc());
    cout << "rows count = " << rows.size() << endl;

    return 0;
}
