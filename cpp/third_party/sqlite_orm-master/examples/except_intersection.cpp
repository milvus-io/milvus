/**
 *  Example is implemented from here https://www.tutlane.com/tutorial/sqlite/sqlite-except-operator
 */
#include <sqlite_orm/sqlite_orm.h>
#include <string>
#include <iostream>

using std::cout;
using std::endl;

struct DeptMaster {
    int deptId = 0;
    std::string deptName;
};

struct EmpMaster {
    int empId = 0;
    std::string firstName;
    std::string lastName;
    long salary;
    decltype(DeptMaster::deptId) deptId;
};

int main() {
    using namespace sqlite_orm;
    
    auto storage = make_storage("",
                                make_table("dept_master",
                                           make_column("dept_id", &DeptMaster::deptId, autoincrement(), primary_key()),
                                           make_column("dept_name", &DeptMaster::deptName)),
                                make_table("emp_master",
                                           make_column("emp_id", &EmpMaster::empId, autoincrement(), primary_key()),
                                           make_column("first_name", &EmpMaster::firstName),
                                           make_column("last_name", &EmpMaster::lastName),
                                           make_column("salary", &EmpMaster::salary),
                                           make_column("dept_id", &EmpMaster::deptId)));
    storage.sync_schema();
    storage.remove_all<DeptMaster>();
    storage.remove_all<EmpMaster>();
    {
        auto valuesToInsert = {
            DeptMaster{0, "Admin"},
            DeptMaster{0, "Sales"},
            DeptMaster{0, "Quality Control"},
            DeptMaster{0, "Marketing"},
        };
        storage.insert_range(valuesToInsert.begin(), valuesToInsert.end());
    }
    {
        auto valuesToInsert = {
            EmpMaster{1, "Honey", "Patel", 10100, 1},
            EmpMaster{2, "Shweta", "Jariwala", 19300, 2},
            EmpMaster{3, "Vinay", "Jariwala", 35100, 3},
            EmpMaster{4, "Jagruti", "Viras", 9500, 12},
        };
        storage.replace_range(valuesToInsert.begin(), valuesToInsert.end());
    }
    {
        //  SELECT dept_id
        //  FROM dept_master
        //  EXCEPT
        //  SELECT dept_id
        //  FROM emp_master
        auto rows = storage.select(except(select(&DeptMaster::deptId),
                                          select(&EmpMaster::deptId)));
        cout << "rows count = " << rows.size() << endl;
        for(auto id : rows) {
            cout << id << endl;
        }
    }
    {
        //  SELECT dept_id
        //  FROM dept_master
        //  INTERSECT
        //  SELECT dept_id
        //  FROM emp_master
        auto rows = storage.select(intersect(select(&DeptMaster::deptId),
                                             select(&EmpMaster::deptId)));
        cout << "rows count = " << rows.size() << endl;
        for(auto id : rows) {
            cout << id << endl;
        }
    }
    
    return 0;
}
