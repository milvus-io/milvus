
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
                                           make_column("d1",
                                                       &DatetimeText::d1),
                                           make_column("d2",
                                                       &DatetimeText::d2)));
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
    
    return 0;
}
