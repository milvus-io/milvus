//
//  This example is implemented from here https://www.tutorialspoint.com/sqlite/sqlite_distinct_keyword.htm
//

#include <sqlite_orm/sqlite_orm.h>
#include <iostream>

using std::cout;
using std::endl;

struct Employee {
    int id;
    std::string name;
    int age;
    std::string address;
    double salary;
};

auto initStorage(const std::string &path) {
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

int main(int argc, char **argv) {
    
    Storage storage = initStorage("distinct.sqlite");
    
    //  sync schema in case this is a very first launch
    storage.sync_schema();
    
    //  remove all employees if it is not the very first launch
    storage.remove_all<Employee>();
    
    //  now let's insert start values to wirk with. We'll make it within a transaction
    //  for better perfomance
    storage.begin_transaction();
    storage.insert(Employee{
        -1,
        "Paul",
        32,
        "California",
        20000.0,
    });
    storage.insert(Employee{
        -1,
        "Allen",
        25,
        "Texas",
        15000.0,
    });
    storage.insert(Employee{
        -1,
        "Teddy",
        23,
        "Norway",
        20000.0,
    });
    storage.insert(Employee{
        -1,
        "Mark",
        25,
        "Rich-Mond",
        65000.0,
    });
    storage.insert(Employee{
        -1,
        "David",
        27,
        "Texas",
        85000.0,
    });
    storage.insert(Employee{
        -1,
        "Kim",
        22,
        "South-Hall",
        45000.0,
    });
    storage.insert(Employee{
        -1,
        "James",
        24,
        "Houston",
        10000.0,
    });
    storage.insert(Employee{
        -1,
        "Paul",
        24,
        "Houston",
        20000.0,
    });
    storage.insert(Employee{
        -1,
        "James",
        44,
        "Norway",
        5000.0,
    });
    storage.insert(Employee{
        -1,
        "James",
        45,
        "Texas",
        5000.0,
    });
    storage.commit();
    
    //  SELECT 'NAME' FROM 'COMPANY'
    auto pureNames = storage.select(&Employee::name);
    cout << "NAME" << endl;
    cout << "----------" << endl;
    for(auto &name : pureNames) {
        cout << name << endl;
    }
    cout << endl;
    
    using namespace sqlite_orm;
    //  SELECT DISTINCT 'NAME' FROM 'COMPANY'
    auto distinctNames = storage.select(distinct(&Employee::name));
    cout << "NAME" << endl;
    cout << "----------" << endl;
    for(auto &name : distinctNames) {
        cout << name << endl;
    }
    cout << endl;
    
    //  SELECT DISTINCT 'ADDRESS', 'NAME' FROM 'COMPANY'
    auto severalColumns = storage.select(distinct(columns(&Employee::address, &Employee::name)));
    cout << "ADDRESS" << '\t' << "NAME" << endl;
    cout << "----------" << endl;
    for(auto &t : severalColumns) {
        cout << std::get<0>(t) << '\t' << std::get<1>(t) << endl;
    }
    return 0;
}
