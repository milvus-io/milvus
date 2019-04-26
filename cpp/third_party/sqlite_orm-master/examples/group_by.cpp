//
//  Example is implemented from here https://www.tutorialspoint.com/sqlite/sqlite_group_by.htm
//

#include <sqlite_orm/sqlite_orm.h>
#include <iostream>
#include <string>

using std::cout;
using std::endl;

struct Employee {
    int id;
    std::string name;
    int age;
    std::string address;
    double salary;
};

int main(int argc, char **argv) {
    using namespace sqlite_orm;
    
    auto storage = make_storage("group_by.sqlite",
                                make_table("COMPANY",
                                           make_column("ID", &Employee::id, primary_key()),
                                           make_column("NAME", &Employee::name),
                                           make_column("AGE", &Employee::age),
                                           make_column("ADDRESS", &Employee::address),
                                           make_column("SALARY", &Employee::salary)));
    storage.sync_schema();
    storage.remove_all<Employee>();
    
    storage.insert(Employee{ -1, "Paul", 32, "California", 20000.0});
    storage.insert(Employee{ -1, "Allen", 25, "Texas", 15000.0});
    storage.insert(Employee{ -1, "Teddy", 23, "Norway", 20000.0});
    storage.insert(Employee{ -1, "Mark", 25, "Rich-Mond", 65000.0});
    storage.insert(Employee{ -1, "David", 27, "Texas", 85000.0});
    storage.insert(Employee{ -1, "Kim", 22, "South-Hall", 45000.0});
    storage.insert(Employee{ -1, "James", 24, "Houston", 10000.0});
    
    //  If you want to know the total amount of salary on each customer, then GROUP BY query would be as follows:
    //  SELECT NAME, SUM(SALARY)
    //  FROM COMPANY
    //  GROUP BY NAME;
    auto salaryName = storage.select(columns(&Employee::name, sum(&Employee::salary)),
                                     group_by(&Employee::name));
    for(auto &t : salaryName) {
        cout << std::get<0>(t) << '\t' << *std::get<1>(t) << endl;
    }
    
    //  Now, let us create three more records in COMPANY table using the following INSERT statements:
    
    storage.insert(Employee{ -1, "Paul", 24, "Houston", 20000.00});
    storage.insert(Employee{ -1, "James", 44, "Norway", 5000.00});
    storage.insert(Employee{ -1, "James", 24, "Texas", 5000.00});
    
    cout << endl << "Now, our table has the following records with duplicate names:" << endl << endl;
    
    for(auto &employee : storage.iterate<Employee>()) {
        cout << storage.dump(employee) << endl;
    }
    
    cout << endl << "Again, let us use the same statement to group-by all the records using NAME column as follows:" << endl << endl;
    salaryName = storage.select(columns(&Employee::name, sum(&Employee::salary)),
                                group_by(&Employee::name),
                                order_by(&Employee::name));
    for(auto &t : salaryName) {
        cout << std::get<0>(t) << '\t' << *std::get<1>(t) << endl;
    }
    
    cout << endl << "Let us use ORDER BY clause along with GROUP BY clause as follows:" << endl << endl;
    salaryName = storage.select(columns(&Employee::name, sum(&Employee::salary)),
                                group_by(&Employee::name),
                                order_by(&Employee::name).desc());
    for(auto &t : salaryName) {
        cout << std::get<0>(t) << '\t' << *std::get<1>(t) << endl;
    }
    
    return 0;
}
