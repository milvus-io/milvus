/**
 *  Ths example is implemented from here https://www.tutorialspoint.com/sqlite/sqlite_having_clause.htm
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
    float salary;
};

int main() {
    
    using namespace sqlite_orm;
    
    auto storage = make_storage("having.sqlite",
                                make_table("COMPANY",
                                           make_column("ID", &Employee::id, primary_key()),
                                           make_column("NAME", &Employee::name),
                                           make_column("AGE", &Employee::age),
                                           make_column("ADDRESS", &Employee::address),
                                           make_column("SALARY", &Employee::salary)));
    storage.sync_schema();
    storage.remove_all<Employee>();
    
    storage.replace(Employee{ 1, "Paul", 32, "California", 20000.0 });
    storage.replace(Employee{ 2, "Allen", 25, "Texas", 15000.0 });
    storage.replace(Employee{ 3, "Teddy", 23, "Norway", 20000.0 });
    storage.replace(Employee{ 4, "Mark", 25, "Rich-Mond", 65000.0 });
    storage.replace(Employee{ 5, "David", 27, "Texas", 85000.0 });
    storage.replace(Employee{ 6, "Kim", 22, "South-Hall", 45000.0 });
    storage.replace(Employee{ 7, "James", 24, "Houston", 10000.0 });
    storage.replace(Employee{ 8, "Paul", 24, "Houston", 20000.0 });
    storage.replace(Employee{ 9, "James", 44, "Norway", 5000.0 });
    storage.replace(Employee{ 10, "James", 45, "Texas", 5000.0 });
    
    {
        //  SELECT *
        //  FROM COMPANY
        //  GROUP BY name
        //  HAVING count(name) < 2;
        auto rows = storage.get_all<Employee>(group_by(&Employee::name),
                                              having(lesser_than(count(&Employee::name), 2)));
        for(auto &employee : rows) {
            cout << storage.dump(employee) << endl;
        }
        cout << endl;
    }
    {
        //  SELECT *
        //  FROM COMPANY
        //  GROUP BY name
        //  HAVING count(name) > 2;
        auto rows = storage.get_all<Employee>(group_by(&Employee::name),
                                              having(greater_than(count(&Employee::name), 2)));
        for(auto &employee : rows) {
            cout << storage.dump(employee) << endl;
        }
        cout << endl;
    }
    
    return 0;
}
