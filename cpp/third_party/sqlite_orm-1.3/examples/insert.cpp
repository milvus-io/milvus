/**
 *  This code is an implementation of this article https://www.tutorialspoint.com/sqlite/sqlite_insert_query.htm
 *  with C++ specific features like creating object and inserting within
 *  single line and separatedly, inserting a subclass object and inserting a vector.
 */

#include <sqlite_orm/sqlite_orm.h>
#include <string>
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

struct DetailedEmployee : public Employee {
    std::string birthDate;
};

int main(int argc, char **argv) {
    using namespace sqlite_orm;
    
    auto storage = make_storage("insert.sqlite",
                                make_table("COMPANY",
                                           make_column("ID",
                                                       &Employee::id,
                                                       primary_key()),
                                           make_column("NAME",
                                                       &Employee::name),
                                           make_column("AGE",
                                                       &Employee::age),
                                           make_column("ADDRESS",
                                                       &Employee::address),
                                           make_column("SALARY",
                                                       &Employee::salary)));
    storage.sync_schema();
    storage.remove_all<Employee>();
    
    Employee paul {
        -1,
        "Paul",
        32,
        "California",
        20000.00,
    };
    
    //  insert returns inserted id
    paul.id = storage.insert(paul);
    
    storage.insert(Employee{
        -1,
        "Allen",
        25,
        "Texas",
        15000.00,
    });
    
    DetailedEmployee teddy;
    teddy.name = "Teddy";
    teddy.age = 23;
    teddy.address = "Norway";
    teddy.salary = 20000.00;
    
    //  to insert subclass object as a superclass you have to specify type explicitly
    teddy.id = storage.insert<Employee>(teddy);
    
    std::vector<Employee> otherEmployees;
    otherEmployees.push_back(Employee{
        -1,
        "Mark",
        25,
        "Rich-Mond",
        65000.00,
    });
    otherEmployees.push_back(Employee{
        -1,
        "David",
        27,
        "Texas",
        85000.00,
    });
    otherEmployees.push_back(Employee{
        -1,
        "Kim",
        22,
        "South-Hall",
        45000.00,
    });
    //  transaction is optional. It is used here to optimize sqlite usage - every insert opens
    //  and closes database. So triple insert will open and close the db three times.
    //  Transaction openes and closes the db only once.
    storage.transaction([&]{
        for(auto &employee : otherEmployees) {
            storage.insert(employee);
        }
        return true;    //  commit
    });
    
    Employee james{
        -1,
        "James",
        24,
        "Houston",
        10000.00,
    };
    james.id = storage.insert(james);
    
    cout << "---------------------" << endl;
    for(auto &employee : storage.iterate<Employee>()){
        cout << storage.dump(employee) << endl;
    }
    
    return 0;
}
