//
//  This example is obtained from here http://www.sqlitetutorial.net/sqlite-self-join/
//

#include <sqlite_orm/sqlite_orm.h>
#include <iostream>
#include <cassert>

using std::cout;
using std::endl;

struct Employee {
    int employeeId;
    std::string lastName;
    std::string firstName;
    std::string title;
    std::unique_ptr<int> reportsTo;
    std::string birthDate;
    std::string hireDate;
    std::string address;
    std::string city;
    std::string state;
    std::string country;
    std::string postalCode;
    std::string phone;
    std::string fax;
    std::string email;
};

/**
 *  This is how custom alias is made:
 *  1) it must have `type` alias which is equal to your mapped class
 *  2) is must have static function with `get()` signature and return type with `operator<<`
 */
template<class T>
struct custom_alias : sqlite_orm::alias_tag {
    using type = T;
    
    static const std::string &get() {
        static const std::string res = "emp";
        return res;
    }
};

int main() {
    using namespace sqlite_orm;
    auto storage = make_storage("self_join.sqlite",
                                make_table("employees",
                                           make_column("EmployeeId", &Employee::employeeId, autoincrement(), primary_key()),
                                           make_column("LastName", &Employee::lastName),
                                           make_column("FirstName", &Employee::firstName),
                                           make_column("Title", &Employee::title),
                                           make_column("ReportsTo", &Employee::reportsTo),
                                           make_column("BirthDate", &Employee::birthDate),
                                           make_column("HireDate", &Employee::hireDate),
                                           make_column("Address", &Employee::address),
                                           make_column("City", &Employee::city),
                                           make_column("State", &Employee::state),
                                           make_column("Country", &Employee::country),
                                           make_column("PostalCode", &Employee::postalCode),
                                           make_column("Phone", &Employee::phone),
                                           make_column("Fax", &Employee::fax),
                                           make_column("Email", &Employee::email),
                                           foreign_key(&Employee::reportsTo).references(&Employee::employeeId)));
    storage.sync_schema();
    storage.remove_all<Employee>();
    
    storage.begin_transaction();
    storage.replace(Employee{
        1, "Adams", "Andrew", "General Manager", {}, "1962-02-18 00:00:00", "2002-08-14 00:00:00",
        "11120 Jasper Ave NW", "Edmonton", "AB", "Canada", "T5K 2N1", "+1 (780) 428-9482",
        "+1 (780) 428-3457", "andrew@chinookcorp.com",
    });
    storage.replace(Employee{
        2, "Edwards", "Nancy", "Sales Manager", std::make_unique<int>(1), "1958-12-08 00:00:00",
        "2002-05-01 00:00:00", "825 8 Ave SW", "Calgary", "AB", "Canada", "T2P 2T3",
        "+1 (403) 262-3443", "+1 (403) 262-3322", "nancy@chinookcorp.com",
    });
    storage.replace(Employee{
        3, "Peacock", "Jane", "Sales Support Agent", std::make_unique<int>(2), "1973-08-29 00:00:00",
        "2002-04-01 00:00:00", "1111 6 Ave SW", "Calgary", "AB", "Canada", "T2P 5M5",
        "+1 (403) 262-3443", "+1 (403) 262-6712", "jane@chinookcorp.com"
    });
    storage.replace(Employee{
        4, "Park", "Margaret", "Sales Support Agent", std::make_unique<int>(2), "1947-09-19 00:00:00",
        "2003-05-03 00:00:00", "683 10 Street SW", "Calgary", "AB", "Canada", "T2P 5G3",
        "+1 (403) 263-4423", "+1 (403) 263-4289", "margaret@chinookcorp.com"
    });
    storage.replace(Employee{
        5, "Johnson", "Steve", "Sales Support Agent", std::make_unique<int>(2), "1965-03-03 00:00:00",
        "2003-10-17 00:00:00", "7727B 41 Ave", "Calgary", "AB", "Canada", "T3B 1Y7",
        "1 (780) 836-9987", "1 (780) 836-9543", "steve@chinookcorp.com"
    });
    storage.replace(Employee{
        6, "Mitchell", "Michael", "IT Manager", std::make_unique<int>(1), "1973-07-01 00:00:00",
        "2003-10-17 00:00:00", "5827 Bowness Road NW", "Calgary", "AB", "Canada", "T3B 0C5",
        "+1 (403) 246-9887", "+1 (403) 246-9899", "michael@chinookcorp.com"
    });
    storage.replace(Employee{
        7, "King", "Robert", "IT Staff", std::make_unique<int>(6), "1970-05-29 00:00:00",
        "2004-01-02 00:00:00", "590 Columbia Boulevard West", "Lethbridge", "AB", "Canada",
        "T1K 5N8", "+1 (403) 456-9986", "+1 (403) 456-8485", "robert@chinookcorp.com"
    });
    storage.replace(Employee{
        8, "Callahan", "Laura", "IT Staff", std::make_unique<int>(6), "1968-01-09 00:00:00",
        "2004-03-04 00:00:00", "923 7 ST NW", "Lethbridge", "AB", "Canada",
        "T1H 1Y8", "+1 (403) 467-3351", "+1 (403) 467-8772", "laura@chinookcorp.com"
    });
    storage.commit();
    
    {
        //  SELECT m.FirstName || ' ' || m.LastName,
        //      employees.FirstName || ' ' || employees.LastName
        //  FROM employees
        //  INNER JOIN employees m
        //  ON m.ReportsTo = employees.EmployeeId
        using als = alias_m<Employee>;
        auto firstNames = storage.select(columns(c(alias_column<als>(&Employee::firstName)) || " " || c(alias_column<als>(&Employee::lastName)),
                                                 c(&Employee::firstName) || " " || c(&Employee::lastName)),
                                         inner_join<als>(on(alias_column<als>(&Employee::reportsTo) == c(&Employee::employeeId))));
        cout << "firstNames count = " << firstNames.size() << endl;
        for(auto &row : firstNames) {
            cout << std::get<0>(row) << '\t' << std::get<1>(row) << endl;
        }
        
        assert(storage.count<Employee>() == storage.count<alias_a<Employee>>());
    }
    {   //  same query with custom alias 'emp'
        cout << endl;
        
        //  SELECT emp.FirstName || ' ' || emp.LastName,
        //      employees.FirstName || ' ' || employees.LastName
        //  FROM employees
        //  INNER JOIN employees emp
        //  ON emp.ReportsTo = employees.EmployeeId
        using als = custom_alias<Employee>;
        auto firstNames = storage.select(columns(c(alias_column<als>(&Employee::firstName)) || " " || c(alias_column<als>(&Employee::lastName)),
                                                 c(&Employee::firstName) || " " || c(&Employee::lastName)),
                                         inner_join<als>(on(alias_column<als>(&Employee::reportsTo) == c(&Employee::employeeId))));
        cout << "firstNames count = " << firstNames.size() << endl;
        for(auto &row : firstNames) {
            cout << std::get<0>(row) << '\t' << std::get<1>(row) << endl;
        }
    }
    
    return 0;
}
