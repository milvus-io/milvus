/**
 *  This example was grabbed from here https://www.tutorialspoint.com/sqlite/sqlite_unions_clause.htm
 */

#include <sqlite_orm/sqlite_orm.h>
#include <string>
#include <cassert>
#include <algorithm>
#include <iostream>

using std::cout;
using std::endl;

int main() {
    
    struct Employee {
        int id;
        std::string name;
        int age;
        std::string address;
        double salary;
    };
    
    struct Department {
        int id;
        std::string dept;
        int employeeId;
    };
    
    using namespace sqlite_orm;
    
    auto storage = make_storage("union.sqlite",
                                make_table("COMPANY",
                                           make_column("ID", &Employee::id, primary_key()),
                                           make_column("NAME", &Employee::name),
                                           make_column("AGE", &Employee::age),
                                           make_column("ADDRESS", &Employee::address),
                                           make_column("SALARY", &Employee::salary)),
                                make_table("DEPARTMENT",
                                           make_column("ID", &Department::id, primary_key()),
                                           make_column("DEPT", &Department::dept),
                                           make_column("EMP_ID", &Department::employeeId),
                                           foreign_key(&Department::employeeId).references(&Employee::id)));
    storage.sync_schema();
    
    //  order matters
    storage.remove_all<Department>();
    storage.remove_all<Employee>();
    
    storage.replace(Employee{1, "Paul", 32, "California", 20000.0});
    storage.replace(Employee{2, "Allen", 25, "Texas", 15000.0});
    storage.replace(Employee{3, "Teddy", 23, "Norway", 20000.0});
    storage.replace(Employee{4, "Mark", 25, "Rich-Mond", 65000.0});
    storage.replace(Employee{5, "David", 27, "Texas", 85000.0});
    storage.replace(Employee{6, "Kim", 22, "South-Hall", 45000.0});
    storage.replace(Employee{7, "James", 24, "Houston", 10000.0});
    
    storage.replace(Department{1, "IT Billing", 1});
    storage.replace(Department{2, "Engineering", 2});
    storage.replace(Department{3, "Finance", 7});
    storage.replace(Department{4, "Engineering", 3});
    storage.replace(Department{5, "Finance", 4});
    storage.replace(Department{6, "Engineering", 5});
    storage.replace(Department{7, "Finance", 6});
    
    {
        //  SELECT EMP_ID, NAME, DEPT
        //  FROM COMPANY
        //  INNER JOIN DEPARTMENT
        //  ON COMPANY.ID = DEPARTMENT.EMP_ID
        //  UNION
        //  SELECT EMP_ID, NAME, DEPT
        //  FROM COMPANY
        //  LEFT OUTER JOIN DEPARTMENT
        //  ON COMPANY.ID = DEPARTMENT.EMP_ID;
        auto rows = storage.select(union_(select(columns(&Department::employeeId, &Employee::name, &Department::dept),
                                                 inner_join<Department>(on(is_equal(&Employee::id, &Department::employeeId)))),
                                          select(columns(&Department::employeeId, &Employee::name, &Department::dept),
                                                 left_outer_join<Department>(on(is_equal(&Employee::id, &Department::employeeId))))));
        
        assert(rows.size() == 7);
        std::sort(rows.begin(), rows.end(), [](auto &lhs, auto &rhs){
            return std::get<0>(lhs) < std::get<0>(rhs);
        });
        for(auto &row : rows) {
            cout << std::get<0>(row) << '\t' << std::get<1>(row) << '\t' << std::get<2>(row) << endl;
        }
        cout << endl;
    }
    {
        //  SELECT EMP_ID, NAME, DEPT
        //  FROM COMPANY
        //  INNER JOIN DEPARTMENT
        //  ON COMPANY.ID = DEPARTMENT.EMP_ID
        //  UNION ALL
        //  SELECT EMP_ID, NAME, DEPT
        //  FROM COMPANY
        //  LEFT OUTER JOIN DEPARTMENT
        //  ON COMPANY.ID = DEPARTMENT.EMP_ID;
        auto rows = storage.select(union_all(select(columns(&Department::employeeId, &Employee::name, &Department::dept),
                                                    inner_join<Department>(on(is_equal(&Employee::id, &Department::employeeId)))),
                                             select(columns(&Department::employeeId, &Employee::name, &Department::dept),
                                                    left_outer_join<Department>(on(is_equal(&Employee::id, &Department::employeeId))))));
        for(auto &row : rows) {
            cout << std::get<0>(row) << '\t' << std::get<1>(row) << '\t' << std::get<2>(row) << endl;
        }
        cout << endl;
    }
    
    
    return 0;
}
