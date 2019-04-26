/**
 *  Example is taken from here https://www.tutorialspoint.com/sqlite/sqlite_alias_syntax.htm
 */

#include <sqlite_orm/sqlite_orm.h>

#include <cassert>
#include <string>
#include <iostream>

using std::cout;
using std::endl;

struct Employee {
    int id = 0;
    std::string name;
    int age = 0;
    std::string address;
    float salary = 0;
};

struct Department {
    int id = 0;
    std::string dept;
    int empId = 0;
};

using namespace sqlite_orm;

struct EmployeeIdAlias : alias_tag {
    static const std::string &get() {
        static const std::string res = "COMPANY_ID";
        return res;
    }
};

struct CompanyNameAlias : alias_tag {
    static const std::string &get() {
        static const std::string res = "COMPANY_NAME";
        return res;
    }
};

int main(int argc, char* argv[]) {
    cout << argv[0] << endl;
    
    auto storage = make_storage("custom_aliases.sqlite",
                                make_table("COMPANY",
                                           make_column("ID", &Employee::id, primary_key()),
                                           make_column("NAME", &Employee::name),
                                           make_column("AGE", &Employee::age),
                                           make_column("ADDRESS", &Employee::address),
                                           make_column("SALARY", &Employee::salary)),
                                make_table("DEPARTMENT",
                                           make_column("ID", &Department::id),
                                           make_column("DEPT", &Department::dept),
                                           make_column("EMP_ID", &Department::empId)));
    storage.sync_schema();
    storage.remove_all<Department>();
    storage.remove_all<Employee>();
    
    //  insert initial values
    storage.replace(Employee{ 1, "Paul", 32, "California", 20000.0 });
    storage.replace(Employee{ 2, "Allen", 25, "Texas", 15000.0 });
    storage.replace(Employee{ 3, "Teddy", 23, "Norway", 20000.0 });
    storage.replace(Employee{ 4, "Mark", 25, "Rich-Mond", 65000.0 });
    storage.replace(Employee{ 5, "David", 27, "Texas", 85000.0 });
    storage.replace(Employee{ 6, "Kim", 22, "South-Hall", 45000.0 });
    storage.replace(Employee{ 7, "James", 24, "Houston", 10000.0 });
    
    storage.replace(Department{1, "IT Billing", 1});
    storage.replace(Department{2, "Engineering", 2});
    storage.replace(Department{3, "Finance", 7});
    storage.replace(Department{4, "Engineering", 3});
    storage.replace(Department{5, "Finance", 4});
    storage.replace(Department{6, "Engineering", 5});
    storage.replace(Department{7, "Finance", 6});
    
    //  SELECT COMPANY.ID, COMPANY.NAME, COMPANY.AGE, DEPARTMENT.DEPT
    //  FROM COMPANY, DEPARTMENT
    //  WHERE COMPANY.ID = DEPARTMENT.EMP_ID;
    auto simpleRows = storage.select(columns(&Employee::id, &Employee::name, &Employee::age, &Department::dept),
                                     where(is_equal(&Employee::id, &Department::empId)));
    assert(simpleRows.size() == 7);
    
    cout << "ID" << '\t' << "NAME" << '\t' << "AGE" << '\t' << "DEPT" << endl;
    cout << "----------" << '\t' << "----------" << '\t' << "----------" << '\t' << "----------" << endl;
    for(auto &row : simpleRows) {
        cout << std::get<0>(row) << '\t' << std::get<1>(row) << '\t' << std::get<2>(row) << '\t' << std::get<3>(row) << endl;
    }
    
    //  SELECT C.ID, C.NAME, C.AGE, D.DEPT
    //  FROM COMPANY AS C, DEPARTMENT AS D
    //  WHERE  C.ID = D.EMP_ID;
    using als_c = alias_c<Employee>;
    using als_d = alias_d<Department>;
    auto rowsWithTableAliases = storage.select(columns(alias_column<als_c>(&Employee::id),
                                                       alias_column<als_c>(&Employee::name),
                                                       alias_column<als_c>(&Employee::age),
                                                       alias_column<als_d>(&Department::dept)),
                                               where(is_equal(alias_column<als_c>(&Employee::id), alias_column<als_d>(&Department::empId))));
    assert(rowsWithTableAliases == simpleRows);
    
    //  SELECT COMPANY.ID as COMPANY_ID, COMPANY.NAME AS COMPANY_NAME, COMPANY.AGE, DEPARTMENT.DEPT
    //  FROM COMPANY, DEPARTMENT
    //  WHERE COMPANY_ID = DEPARTMENT.EMP_ID;
    auto rowsWithColumnAliases = storage.select(columns(as<EmployeeIdAlias>(&Employee::id),
                                                        as<CompanyNameAlias>(&Employee::name),
                                                        &Employee::age,
                                                        &Department::dept),
                                                where(is_equal(get<EmployeeIdAlias>(), &Department::empId)));
    assert(rowsWithColumnAliases == rowsWithTableAliases);
    
    //  SELECT C.ID AS COMPANY_ID, C.NAME AS COMPANY_NAME, C.AGE, D.DEPT
    //  FROM COMPANY AS C, DEPARTMENT AS D
    //  WHERE  C.ID = D.EMP_ID;
    auto rowsWithBothTableAndColumnAliases = storage.select(columns(as<EmployeeIdAlias>(alias_column<als_c>(&Employee::id)),
                                                                    as<CompanyNameAlias>(alias_column<als_c>(&Employee::name)),
                                                                    alias_column<als_c>(&Employee::age),
                                                                    alias_column<als_d>(&Department::dept)),
                                                            where(is_equal(alias_column<als_c>(&Employee::id), alias_column<als_d>(&Department::empId))));
    assert(rowsWithBothTableAndColumnAliases == rowsWithBothTableAndColumnAliases);
    
    return 0;
}
