/****
 Implemented example from here https://www.tutorialspoint.com/sqlite/sqlite_select_query.htm.
 */

#include <sqlite_orm/sqlite_orm.h>

#include <iostream>

struct Employee {
    int id;
    std::string name;
    int age;
    std::unique_ptr<std::string> address;   //  optional
    std::unique_ptr<double> salary; //  optional
};

using namespace sqlite_orm;

using std::cout;
using std::endl;

int main(int argc, char **argv) {
    auto storage = make_storage("select.sqlite",
                                make_table("COMPANY",
                                           make_column("ID", &Employee::id, primary_key()),
                                           make_column("NAME", &Employee::name),
                                           make_column("AGE", &Employee::age),
                                           make_column("ADDRESS", &Employee::address),
                                           make_column("SALARY", &Employee::salary)));
    storage.sync_schema();
    storage.remove_all<Employee>(); //  remove all old employees in case they exist in db..
    
    //  create employees..
    Employee paul{-1, "Paul", 32, std::make_unique<std::string>("California"), std::make_unique<double>(20000.0) };
    Employee allen{-1, "Allen", 25, std::make_unique<std::string>("Texas"), std::make_unique<double>(15000.0) };
    Employee teddy{-1, "Teddy", 23, std::make_unique<std::string>("Norway"), std::make_unique<double>(20000.0) };
    Employee mark{-1, "Mark", 25, std::make_unique<std::string>("Rich-Mond"), std::make_unique<double>(65000.0) };
    Employee david{-1, "David", 27, std::make_unique<std::string>("Texas"), std::make_unique<double>(85000.0) };
    Employee kim{-1, "Kim", 22, std::make_unique<std::string>("South-Hall"), std::make_unique<double>(45000.0) };
    Employee james{-1, "James", 24, std::make_unique<std::string>("Houston"), std::make_unique<double>(10000.0) };
    
    //  insert employees. `insert` function returns id of inserted object..
    paul.id = storage.insert(paul);
    allen.id = storage.insert(allen);
    teddy.id = storage.insert(teddy);
    mark.id = storage.insert(mark);
    david.id = storage.insert(david);
    kim.id = storage.insert(kim);
    james.id = storage.insert(james);
    
    //  print users..
    cout << "paul = " << storage.dump(paul) << endl;
    cout << "allen = " << storage.dump(allen) << endl;
    cout << "teddy = " << storage.dump(teddy) << endl;
    cout << "mark = " << storage.dump(mark) << endl;
    cout << "david = " << storage.dump(david) << endl;
    cout << "kim = " << storage.dump(kim) << endl;
    cout << "james = " << storage.dump(james) << endl;
    
    //  select all employees..
    auto allEmployees = storage.get_all<Employee>();
    
    cout << "allEmployees[0] = " << storage.dump(allEmployees[0]) << endl;
    cout << "allEmployees count = " << allEmployees.size() << endl;
    
    //  now let's select id, name and salary..
    auto idsNamesSalarys = storage.select(columns(&Employee::id,
                                                  &Employee::name,
                                                  &Employee::salary));
    //  decltype(idsNamesSalarys) = std::vector<std::tuple<int, std::string, std::unique_ptr<double>>>
    for(auto &tpl : idsNamesSalarys) {
        cout << "id = " << std::get<0>(tpl) << ", name = " << std::get<1>(tpl) << ", salary = ";
        if(std::get<2>(tpl)){
            cout << *std::get<2>(tpl);
        }else{
            cout << "null";
        }
        cout << endl;
    }
    
    cout << endl;
    
    auto allEmployeesTuples = storage.select(asterisk<Employee>());
    cout << "allEmployeesTuples count = " << allEmployeesTuples.size() << endl;
    for(auto &row : allEmployeesTuples) {   //  row is std::tuple<int, std::string, int, std::shared_ptr<std::string>, std::shared_ptr<double>>
        cout << std::get<0>(row) << '\t' << std::get<1>(row) << '\t' << std::get<2>(row) << '\t';
        if(auto &value = std::get<3>(row)){
            cout << *value;
        }else{
            cout << "null";
        }
        cout << '\t';
        if(auto &value = std::get<4>(row)){
            cout << *value;
        }else{
            cout << "null";
        }
        cout << '\t' << endl;
    }
}
