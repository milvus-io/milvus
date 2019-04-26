
#include <sqlite_orm/sqlite_orm.h>

#include <iostream>
#include <string>

using std::cout;
using std::endl;

struct DatetimeText {
    std::string d1;
    std::string d2;
};

int main() {
    using namespace sqlite_orm;
    
    auto storage = make_storage("",
                                make_table("datetime_text",
                                           make_column("d1", &DatetimeText::d1),
                                           make_column("d2", &DatetimeText::d2)));
    storage.sync_schema();
    
    //  SELECT date('now');
    auto now = storage.select(date("now")).front();
    cout << "date('now') = " << now << endl;
    
    //  SELECT date('now','start of month','+1 month','-1 day')
    auto nowWithOffset = storage.select(date("now", "start of month", "+1 month", "-1 day")).front();
    cout << "SELECT date('now','start of month','+1 month','-1 day') = " << nowWithOffset << endl;
    
    //  SELECT DATETIME('now')
    auto nowTime = storage.select(datetime("now")).front();
    cout << "DATETIME('now') = " << nowTime << endl;
    
    //  SELECT DATETIME('now','localtime')
    auto localTime = storage.select(datetime("now", "localtime")).front();
    cout << "DATETIME('now','localtime') = " << localTime << endl;
    
    //  INSERT INTO datetime_text (d1, d2)
    //  VALUES
    //  (
    //      datetime('now'),
    //      datetime('now', 'localtime')
    //  );
    DatetimeText datetimeText{
        storage.select(datetime("now")).front(),
        storage.select(datetime("now", "localtime")).front(),
    };
    storage.insert(datetimeText);
    
    cout << "SELECT * FROM datetime_text = " << storage.dump(storage.get_all<DatetimeText>().front()) << endl;
    
    auto nowWithOffset2 = storage.select(date("now", "+2 day")).front();
    cout << "SELECT date('now','+2 day') = " << nowWithOffset2 << endl;
    
    //  SELECT julianday('now')
    auto juliandayNow = storage.select(julianday("now")).front();
    cout << "SELECT julianday('now') = " << juliandayNow << endl;
    
    //  SELECT julianday('1776-07-04')
    auto oldJulianday = storage.select(julianday("1776-07-04")).front();
    cout << "SELECT julianday('1776-07-04') = " << oldJulianday << endl;
    
    //  SELECT julianday('now') + julianday('1776-07-04')
    auto julianSum = storage.select(julianday("now") + julianday("1776-07-04")).front();
    cout << "SELECT julianday('now') + julianday('1776-07-04') = " << julianSum << endl;
    
    //  SELECT julianday('now') - julianday('1776-07-04')
    auto julianDiff = storage.select(julianday("now") - julianday("1776-07-04")).front();
    cout << "SELECT julianday('now') - julianday('1776-07-04') = " << julianDiff << endl;
    
    //  SELECT (julianday('now') - 2440587.5)*86400.0;
    auto julianConverted = storage.select((julianday("now") - 2440587.5) * 86400.0);
    
    
    return 0;
}
