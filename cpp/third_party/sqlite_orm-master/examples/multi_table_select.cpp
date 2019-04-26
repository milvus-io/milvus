/**
 *  Example implemented from here http://sqlite.awardspace.info/syntax/sqlitepg05.htm
 */

#include <sqlite_orm/sqlite_orm.h>
#include <string>
#include <iostream>

using std::cout;
using std::endl;

struct ReqEquip {
    int reqNumber;
    std::string requestor;
    std::string auth;
    std::string reqDate;
};

struct ReqDetail {
    int reqNumber;
    int stockNumber;
    int quantity;
    double itemCost;
};

int main(int argc, char **argv) {
    
    using namespace sqlite_orm;
    auto storage = make_storage("multi_table_select.sqlite",
                                make_table("ReqEquip",
                                           make_column("ReqNumber", &ReqEquip::reqNumber, primary_key()),
                                           make_column("Requestor", &ReqEquip::requestor),
                                           make_column("Auth", &ReqEquip::auth),
                                           make_column("ReqDate", &ReqEquip::reqDate)),
                                make_table("ReqDetail",
                                           make_column("ReqNumber", &ReqDetail::reqNumber),
                                           make_column("StockNumber", &ReqDetail::stockNumber),
                                           make_column("Quantity", &ReqDetail::quantity),
                                           make_column("ItemCost", &ReqDetail::itemCost)));
    
    storage.sync_schema();
    storage.remove_all<ReqEquip>();
    storage.remove_all<ReqDetail>();
    
    storage.replace(ReqEquip{ 1000, "Carl Jones", "A. Robinson Mgr", "2007/10/30" });
    storage.replace(ReqEquip{ 1001, "Peter Smith", "A. Robinson Mgr", "2007/11/05" });
    storage.replace(ReqEquip{ 1002, "Carl Jones", "A. Robinson Mgr", "2007/11/06" });
    
    storage.replace(ReqDetail{ 1000, 51013, 2, 7.52 });
    storage.replace(ReqDetail{ 1000, 51002, 4, .75 });
    storage.replace(ReqDetail{ 1000, 43512, 4, 1.52 });
    storage.replace(ReqDetail{ 1001, 23155, 1, 9.82 });
    storage.replace(ReqDetail{ 1001, 43111, 1, 3.69 });
    storage.replace(ReqDetail{ 1002, 51001, 1, .75 });
    storage.replace(ReqDetail{ 1002, 23155, 1, 9.82 });
    
    cout << endl;
    
    //  SELECT ReqEquip.ReqNumber, ReqEquip.Requestor, ReqDetail.Quantity, ReqDetail.StockNumber
    //  FROM ReqEquip, ReqDetail
    //  WHERE ReqEquip.ReqNumber = ReqDetail.ReqNumber;
    auto rows = storage.select(columns(&ReqEquip::reqNumber, &ReqEquip::requestor, &ReqDetail::quantity, &ReqDetail::stockNumber),
                               where(c(&ReqEquip::reqNumber) == &ReqDetail::reqNumber));
    for(auto &row : rows) {
        cout << std::get<0>(row) << '\t';
        cout << std::get<1>(row) << '\t';
        cout << std::get<2>(row) << '\t';
        cout << std::get<3>(row) << '\t';
        cout << endl;
    }
    
    cout << endl;
    
    //  SELECT ReqEquip.ReqNumber, ReqEquip.Requestor, ReqDetail.Quantity, ReqDetail.StockNumber
    //  FROM ReqEquip, ReqDetail
    //  WHERE ReqEquip.ReqNumber = ReqDetail.ReqNumber AND ReqEquip.ReqNumber = 1000;
    
    rows = storage.select(columns(&ReqEquip::reqNumber, &ReqEquip::requestor, &ReqDetail::quantity, &ReqDetail::stockNumber),
                          where(c(&ReqEquip::reqNumber) == &ReqDetail::reqNumber and c(&ReqEquip::reqNumber) == 1000));
    for(auto &row : rows) {
        cout << std::get<0>(row) << '\t';
        cout << std::get<1>(row) << '\t';
        cout << std::get<2>(row) << '\t';
        cout << std::get<3>(row) << '\t';
        cout << endl;
    }
    
    return 0;
}
