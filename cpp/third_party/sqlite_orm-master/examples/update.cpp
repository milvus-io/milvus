/**
 *  Example implemented from here https://www.tutorialspoint.com/sqlite/sqlite_update_query.htm
 */

#include <sqlite_orm/sqlite_orm.h>
#include <iostream>
#include <memory>

using std::cout;
using std::endl;

struct Employee {
    int id;
    std::string name;
    int age;
    std::string address;
    double salary;
};

inline auto initStorage(const std::string &path) {
    using namespace sqlite_orm;
    return make_storage(path,
                        make_table("COMPANY",
                                   make_column("ID", &Employee::id, primary_key()),
                                   make_column("NAME", &Employee::name),
                                   make_column("AGE", &Employee::age),
                                   make_column("ADDRESS", &Employee::address),
                                   make_column("SALARY", &Employee::salary)));
}

using Storage = decltype(initStorage(""));

static std::unique_ptr<Storage> stor;

int main(int argc, char **argv) {
    stor = std::make_unique<Storage>(initStorage("update.sqlite"));
    stor->sync_schema();
    stor->remove_all<Employee>();

    //  insert initial values
    stor->replace(Employee{ 1, "Paul", 32, "California", 20000.0 });
    stor->replace(Employee{ 2, "Allen", 25, "Texas", 15000.0 });
    stor->replace(Employee{ 3, "Teddy", 23, "Norway", 20000.0 });
    stor->replace(Employee{ 4, "Mark", 25, "Rich-Mond", 65000.0 });
    stor->replace(Employee{ 5, "David", 27, "Texas", 85000.0 });
    stor->replace(Employee{ 6, "Kim", 22, "South-Hall", 45000.0 });
    stor->replace(Employee{ 7, "James", 24, "Houston", 10000.0 });

    //  show 'COMPANY' table contents
    for(auto &employee : stor->iterate<Employee>()) {
        cout << stor->dump(employee) << endl;
    }
    cout << endl;

    //  'UPDATE COMPANY SET ADDRESS = 'Texas' WHERE ID = 6'

    auto employee6 = stor->get<Employee>(6);
    employee6.address = "Texas";
    stor->update(employee6);    //  actually this call updates all non-primary-key columns' values to passed object's fields

    //  show 'COMPANY' table contents again
    for(auto &employee : stor->iterate<Employee>()) {
        cout << stor->dump(employee) << endl;
    }
    cout << endl;

    //  'UPDATE COMPANY SET ADDRESS = 'Texas', SALARY = 20000.00 WHERE AGE < 30'
    using namespace sqlite_orm;
    stor->update_all(set(c(&Employee::address) = "Texas",
                         c(&Employee::salary) = 20000.00),
                     where(c(&Employee::age) < 30));

    //  show 'COMPANY' table contents one more time
    for(auto &employee : stor->iterate<Employee>()) {
        cout << stor->dump(employee) << endl;
    }
    cout << endl;

    return 0;
}
