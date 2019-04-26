/**
 *  The example is implemented from here http://www.sqlitetutorial.net/sqlite-cross-join/
 */

#include <sqlite_orm/sqlite_orm.h>
#include <string>
#include <iostream>

using std::cout;
using std::endl;

namespace DataModel {
    
    struct Rank {
        std::string rank;
    };
    
    struct Suit {
        std::string suit;
    };
}

static auto initStorage(const std::string &path) {
    using namespace DataModel;
    using namespace sqlite_orm;
    return make_storage(path,
                        make_table("ranks",
                                   make_column("rank", &Rank::rank)),
                        make_table("suits",
                                   make_column("suit", &Suit::suit)));
}

int main(int argc, char **argv) {
    using namespace DataModel;
    
    using Storage = decltype(initStorage(""));
    
    Storage storage = initStorage("cross_join.sqlite");
    
    //  sync schema in case db/tables do not exist
    storage.sync_schema();
    
    //  remove old data if something left from after the last time
    storage.remove_all<Rank>();
    storage.remove_all<Suit>();
    
    storage.insert(Rank{"2"});
    storage.insert(Rank{"3"});
    storage.insert(Rank{"4"});
    storage.insert(Rank{"5"});
    storage.insert(Rank{"6"});
    storage.insert(Rank{"7"});
    storage.insert(Rank{"8"});
    storage.insert(Rank{"9"});
    storage.insert(Rank{"10"});
    storage.insert(Rank{"J"});
    storage.insert(Rank{"Q"});
    storage.insert(Rank{"K"});
    storage.insert(Rank{"A"});
    
    storage.insert(Suit{"Clubs"});
    storage.insert(Suit{"Diamonds"});
    storage.insert(Suit{"Hearts"});
    storage.insert(Suit{"Spades"});
    
    using namespace sqlite_orm;
    
    /**
     *  When you pass columns(...) with members sqlite_orm gathers all types and creates
     *  a std::set with respective table names. Once you passed `cross_join<T>` as an argument
     *  after columns T's table name is removed from table names set.
     */
    //  SELECT rank, suit
    //  FROM ranks
    //  CROSS JOIN suits
    //  ORDER BY suit;
    auto cards = storage.select(columns(&Rank::rank, &Suit::suit),
                                cross_join<Suit>(),
                                order_by(&Suit::suit)); //  cards is vector<tuple<string, string>>
    
    cout << "cards count = " << cards.size() << endl;
    for(auto card : cards) {
        cout << std::get<0>(card) << '\t' << std::get<1>(card) << endl;
    }
    cout << endl;
    
    return 0;
}
