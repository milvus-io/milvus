/**
 *  Example is implemented from here https://www.w3resource.com/sqlite/exists-operator.php
 */
#include <sqlite_orm/sqlite_orm.h>
#include <string>
#include <iostream>

using std::cout;
using std::endl;

struct Customer {
    std::string code;
    std::string name;
    std::string city;
    std::string workingArea;
    std::string country;
    int grade;
    double openingAmt;
    double receiveAmt;
    double paymentAmt;
    double outstandingAmt;
    std::string phoneNo;
    std::string agentCode;
};

struct Agent {
    std::string code;
    std::string name;
    std::string workingArea;
    double comission;
    std::string phoneNo;
    std::string country;
};

struct Order {
    std::string num;
    int amount;
    int advanceAmount;
    std::string date;
    std::string custCode;
    std::string agentCode;
};

int main(int argc, char **argv) {
    using namespace sqlite_orm;
    
    auto storage = make_storage("exists.sqlite",
                                make_table("customer",
                                           make_column("CUST_CODE", &Customer::code, primary_key()),
                                           make_column("CUST_NAME", &Customer::name),
                                           make_column("CUST_CITY", &Customer::city),
                                           make_column("WORKING_AREA", &Customer::workingArea),
                                           make_column("CUST_COUNTRY", &Customer::country),
                                           make_column("GRADE", &Customer::grade),
                                           make_column("OPENING_AMT", &Customer::openingAmt),
                                           make_column("RECEIVE_AMT", &Customer::receiveAmt),
                                           make_column("PAYMENT_AMT", &Customer::paymentAmt),
                                           make_column("OUTSTANDING_AMT", &Customer::outstandingAmt),
                                           make_column("PHONE_NO", &Customer::phoneNo),
                                           make_column("AGENT_CODE", &Customer::agentCode)),
                                make_table("agents",
                                           make_column("AGENT_CODE", &Agent::code, primary_key()),
                                           make_column("AGENT_NAME", &Agent::name),
                                           make_column("WORKING_AREA", &Agent::workingArea),
                                           make_column("COMMISSION", &Agent::comission),
                                           make_column("PHONE_NO", &Agent::phoneNo),
                                           make_column("COUNTRY", &Agent::country)),
                                make_table("orders",
                                           make_column("ORD_NUM", &Order::num, primary_key()),
                                           make_column("ORD_AMOUNT", &Order::amount),
                                           make_column("ADVANCE_AMOUNT", &Order::advanceAmount),
                                           make_column("ORD_DATE", &Order::date),
                                           make_column("CUST_CODE", &Order::custCode),
                                           make_column("AGENT_CODE", &Order::agentCode)));
    storage.sync_schema();
    storage.remove_all<Order>();
    storage.remove_all<Agent>();
    storage.remove_all<Customer>();
    
    storage.replace(Agent{"A007", "Ramasundar", "Bangalore", 0.15, "077-25814763"});
    storage.replace(Agent{"A003", "Alex", "London", 0.13, "075-12458969"});
    storage.replace(Agent{"A008", "Alford", "New York", 0.12, "044-25874365"});
    storage.replace(Agent{"A011", "Ravi Kumar", "Bangalore", 0.15, "077-45625874"});
    storage.replace(Agent{"A010", "Santakumar", "Chennai", 0.14, "007-22388644"});
    storage.replace(Agent{"A012", "Lucida", "San Jose", 0.12, "044-52981425"});
    storage.replace(Agent{"A005", "Anderson", "Brisban", 0.13, "045-21447739"});
    storage.replace(Agent{"A001", "Subbarao", "Bangalore", 0.14, "077-12346674"});
    storage.replace(Agent{"A002", "Mukesh", "Mumbai", 0.11, "029-12358964"});
    storage.replace(Agent{"A006", "McDen", "London", 0.15, "078-22255588"});
    storage.replace(Agent{"A004", "Ivan", "Torento", 0.15, "008-22544166"});
    storage.replace(Agent{"A009", "Benjamin", "Hampshair", 0.11, "008-22536178"});
    
    storage.replace(Customer{"C00013", "Holmes", "London", "London", "UK", 2, 6000.00, 5000.00, 7000.00, 4000.00, "BBBBBBB", "A003"});
    storage.replace(Customer{"C00001", "Micheal", "New York", "New York", "USA", 2, 3000.00, 5000.00, 2000.00, 6000.00, "CCCCCCC", "A008"});
    storage.replace(Customer{"C00020", "Albert", "New York", "New York", "USA", 3, 5000.00, 7000.00, 6000.00, 6000.00, "BBBBSBB", "A008"});
    storage.replace(Customer{"C00025", "Ravindran", "Bangalore", "Bangalore", "India", 2, 5000.00, 7000.00, 4000.00, 8000.00, "AVAVAVA", "A011"});
    storage.replace(Customer{"C00024", "Cook", "London", "London", "UK", 2, 4000.00, 9000.00, 7000.00, 6000.00, "FSDDSDF", "A006"});
    storage.replace(Customer{"C00015", "Stuart", "London", "London", "UK", 1, 6000.00, 8000.00, 3000.00, 11000.00, "GFSGERS", "A003"});
    storage.replace(Customer{"C00002", "Bolt", "New York", "New York", "USA", 3, 5000.00, 7000.00, 9000.00, 3000.00, "DDNRDRH", "A008"});
    storage.replace(Customer{"C00018", "Fleming", "Brisban", "Brisban", "Australia", 2, 7000.00, 7000.00, 9000.00, 5000.00, "NHBGVFC", "A005"});
    storage.replace(Customer{"C00021", "Jacks", "Brisban", "Brisban", "Australia", 1, 7000.00, 7000.00, 7000.00, 7000.00, "WERTGDF", "A005"});
    storage.replace(Customer{"C00019", "Yearannaidu", "Chennai", "Chennai", "India", 1, 8000.00, 7000.00, 7000.00, 8000.00, "ZZZZBFV", "A010"});
    storage.replace(Customer{"C00005", "Sasikant", "Mumbai", "Mumbai", "India", 1, 7000.00, 11000.00, 7000.00, 11000.00, "147-25896312", "A002"});
    storage.replace(Customer{"C00007", "Ramanathan", "Chennai", "Chennai", "India", 1, 7000.00, 11000.00, 9000.00, 9000.00, "GHRDWSD", "A010"});
    storage.replace(Customer{"C00022", "Avinash", "Mumbai", "Mumbai", "India", 2, 7000.00, 11000.00, 9000.00, 9000.00, "113-12345678", "A002"});
    storage.replace(Customer{"C00004", "Winston", "Brisban", "Brisban", "Australia", 1, 5000.00, 8000.00, 7000.00, 6000.00, "AAAAAAA", "A005"});
    storage.replace(Customer{"C00023", "Karl", "London", "London", "UK", 0, 4000.00, 6000.00, 7000.00, 3000.00, "AAAABAA", "A006"});
    storage.replace(Customer{"C00006", "Shilton", "Torento", "Torento", "Canada", 1, 10000.00, 7000.00, 6000.00, 11000.00, "DDDDDDD", "A004"});
    storage.replace(Customer{"C00010", "Charles", "Hampshair", "Hampshair", "UK", 3, 6000.00, 4000.00, 5000.00, 5000.00, "MMMMMMM", "A009"});
    storage.replace(Customer{"C00017", "Srinivas", "Bangalore", "Bangalore", "India", 2, 8000.00, 4000.00, 3000.00, 9000.00, "AAAAAAB", "A007"});
    storage.replace(Customer{"C00012", "Steven", "San Jose", "San Jose", "USA", 1, 5000.00, 7000.00, 9000.00, 3000.00, "KRFYGJK", "A012"});
    storage.replace(Customer{"C00008", "Karolina", "Torento", "Torento", "Canada", 1, 7000.00, 7000.00, 9000.00, 5000.00, "HJKORED", "A004"});
    storage.replace(Customer{"C00003", "Martin", "Torento", "Torento", "Canada", 2, 8000.00, 7000.00, 7000.00, 8000.00, "MJYURFD", "A004"});
    storage.replace(Customer{"C00009", "Ramesh", "Mumbai", "Mumbai", "India", 3, 8000.00, 7000.00, 3000.00, 12000.00, "Phone No", "A002"});
    storage.replace(Customer{"C00014", "Rangarappa", "Bangalore", "Bangalore", "India", 2, 8000.00, 11000.00, 7000.00, 12000.00, "AAAATGF", "A001"});
    storage.replace(Customer{"C00016", "Venkatpati", "Bangalore", "Bangalore", "India", 2, 8000.00, 11000.00, 7000.00, 12000.00, "JRTVFDD", "A007"});
    storage.replace(Customer{"C00011", "Sundariya", "Chennai", "Chennai", "India", 3, 7000.00, 11000.00, 7000.00, 11000.00, "PPHGRTS", "A010"});
    
    storage.replace(Order{"200114", 3500, 2000, "15-AUG-08", "C00002", "A008"});
    storage.replace(Order{"200122", 2500, 400, "16-SEP-08", "C00003", "A004"});
    storage.replace(Order{"200118", 500, 100, "20-JUL-08", "C00023", "A006"});
    storage.replace(Order{"200119", 4000, 700, "16-SEP-08", "C00007", "A010"});
    storage.replace(Order{"200121", 1500, 600, "23-SEP-08", "C00008", "A004"});
    storage.replace(Order{"200130", 2500, 400, "30-JUL-08", "C00025", "A011"});
    storage.replace(Order{"200134", 4200, 1800, "25-SEP-08", "C00004", "A005"});
    storage.replace(Order{"200108", 4000, 600, "15-FEB-08", "C00008", "A004"});
    storage.replace(Order{"200103", 1500, 700, "15-MAY-08", "C00021", "A005"});
    storage.replace(Order{"200105", 2500, 500, "18-JUL-08", "C00025", "A011"});
    storage.replace(Order{"200109", 3500, 800, "30-JUL-08", "C00011", "A010"});
    storage.replace(Order{"200101", 3000, 1000, "15-JUL-08", "C00001", "A008"});
    storage.replace(Order{"200111", 1000, 300, "10-JUL-08", "C00020", "A008"});
    storage.replace(Order{"200104", 1500, 500, "13-MAR-08", "C00006", "A004"});
    storage.replace(Order{"200106", 2500, 700, "20-APR-08", "C00005", "A002"});
    storage.replace(Order{"200125", 2000, 600, "10-OCT-08", "C00018", "A005"});
    storage.replace(Order{"200117", 800, 200, "20-OCT-08", "C00014", "A001"});
    storage.replace(Order{"200123", 500, 100, "16-SEP-08", "C00022", "A002"});
    storage.replace(Order{"200120", 500, 100, "20-JUL-08", "C00009", "A002"});
    storage.replace(Order{"200116", 500, 100, "13-JUL-08", "C00010", "A009"});
    storage.replace(Order{"200124", 500, 100, "20-JUN-08", "C00017", "A007"});
    storage.replace(Order{"200126", 500, 100, "24-JUN-08", "C00022", "A002"});
    storage.replace(Order{"200129", 2500, 500, "20-JUL-08", "C00024", "A006"});
    storage.replace(Order{"200127", 2500, 400, "20-JUL-08", "C00015", "A003"});
    storage.replace(Order{"200128", 3500, 1500, "20-JUL-08", "C00009", "A002"});
    storage.replace(Order{"200135", 2000, 800, "16-SEP-08", "C00007", "A010"});
    storage.replace(Order{"200131", 900, 150, "26-AUG-08", "C00012", "A012"});
    storage.replace(Order{"200133", 1200, 400, "29-JUN-08", "C00009", "A002"});
    storage.replace(Order{"200100", 1000, 600, "08-JAN-08", "C00015", "A003"});
    storage.replace(Order{"200110", 3000, 500, "15-APR-08", "C00019", "A010"});
    storage.replace(Order{"200107", 4500, 900, "30-AUG-08", "C00007", "A010"});
    storage.replace(Order{"200112", 2000, 400, "30-MAY-08", "C00016", "A007"});
    storage.replace(Order{"200113", 4000, 600, "10-JUN-08", "C00022", "A002"});
    storage.replace(Order{"200102", 2000, 300, "25-MAY-08", "C00012", "A012"});
    
    {
        //  SELECT agent_code,agent_name,working_area,commission
        //  FROM agents
        //  WHERE exists
        //      (SELECT *
        //      FROM customer
        //      WHERE grade=3 AND agents.agent_code=customer.agent_code)
        //  ORDER BY commission;
        auto rows = storage.select(columns(&Agent::code, &Agent::name, &Agent::workingArea, &Agent::comission),
                                   where(exists(select(asterisk<Customer>(),
                                                       where(is_equal(&Customer::grade, 3) and is_equal(&Agent::code, &Customer::agentCode))))),
                                   order_by(&Agent::comission));
        cout << "AGENT_CODE  AGENT_NAME                                WORKING_AREA  COMMISSION" << endl;
        cout << "----------  ----------------------------------------  ------------  ----------" << endl;
        for(auto &row : rows) {
            cout << std::get<0>(row) << '\t' <<  std::get<1>(row) << '\t' << std::get<2>(row) << '\t' << std::get<3>(row) << endl;
        }
    }
    {
        //  SELECT cust_code, cust_name, cust_city, grade
        //  FROM customer
        //  WHERE grade=2 AND EXISTS
        //      (SELECT COUNT(*)
        //      FROM customer
        //      WHERE grade=2
        //      GROUP BY grade
        //      HAVING COUNT(*)>2);
        auto rows = storage.select(columns(&Customer::code, &Customer::name, &Customer::city, &Customer::grade),
                                   where(is_equal(&Customer::grade, 2)
                                         and exists(select(count<Customer>(),
                                                           where(is_equal(&Customer::grade, 2)),
                                                           group_by(&Customer::grade),
                                                           having(greater_than(count(), 2))))));
        cout << "CUST_CODE   CUST_NAME   CUST_CITY                            GRADE" << endl;
        cout << "----------  ----------  -----------------------------------  ----------" << endl;
        for(auto &row : rows) {
            cout << std::get<0>(row) << '\t' <<  std::get<1>(row) << '\t' << std::get<2>(row) << '\t' << std::get<3>(row) << endl;
        }
    }
    {
        //  SELECT agent_code,ord_num,ord_amount,cust_code
        //  FROM orders
        //  WHERE NOT EXISTS
        //      (SELECT agent_code
        //      FROM customer
        //      WHERE payment_amt=1400);
        auto rows = storage.select(columns(&Order::agentCode, &Order::num, &Order::amount, &Order::custCode),
                                   where(not exists(select(&Customer::agentCode,
                                                           where(is_equal(&Customer::paymentAmt, 1400))))));
        cout << "AGENT_CODE  ORD_NUM     ORD_AMOUNT  CUST_CODE" << endl;
        for(auto &row : rows) {
            cout << std::get<0>(row) << '\t' <<  std::get<1>(row) << '\t' << std::get<2>(row) << '\t' << std::get<3>(row) << endl;
        }
    }
    return 0;
}
