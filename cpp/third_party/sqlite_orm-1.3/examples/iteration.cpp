
#include <sqlite_orm/sqlite_orm.h>
#include <iostream>
#include <algorithm>

using std::cout;
using std::endl;

struct MarvelHero {
    int id;
    std::string name;
    std::string abilities;
};

int main(int argc, char **argv) {
    using namespace sqlite_orm;
    auto storage = make_storage("iteration.sqlite",
                                make_table("marvel",
                                           make_column("id",
                                                       &MarvelHero::id,
                                                       primary_key()),
                                           make_column("name",
                                                       &MarvelHero::name),
                                           make_column("abilities",
                                                       &MarvelHero::abilities)));
    storage.sync_schema();
    
    storage.remove_all<MarvelHero>();
    
    //  insert values..
    storage.insert(MarvelHero{ -1, "Tony Stark", "Iron man, playboy, billionaire, philanthropist" });
    storage.insert(MarvelHero{ -1, "Thor", "Storm god" });
    storage.insert(MarvelHero{ -1, "Vision", "Min Stone" });
    storage.insert(MarvelHero{ -1, "Captain America", "Vibranium shield" });
    storage.insert(MarvelHero{ -1, "Hulk", "Strength" });
    storage.insert(MarvelHero{ -1, "Star Lord", "Humor" });
    storage.insert(MarvelHero{ -1, "Peter Parker", "Spiderman" });
    storage.insert(MarvelHero{ -1, "Clint Barton", "Hawkeye" });
    storage.insert(MarvelHero{ -1, "Natasha Romanoff", "Black widow" });
    storage.insert(MarvelHero{ -1, "Groot", "I am Groot!" });
    
    cout << "Heros count = " << storage.count<MarvelHero>() << endl;
    
    //  iterate through heros - iteration takes less memory than `get_all` because
    //  iteration fetches row by row once it is needed. If you break at any iteration
    //  statement will be cleared without fetching remaining rows.
    for(auto &hero : storage.iterate<MarvelHero>()) {
        cout << "hero = " << storage.dump(hero) << endl;
    }
    
    cout << "====" << endl;
    
    //  one can iterate with custom WHERE conditions..
    for(auto &hero : storage.iterate<MarvelHero>(where(c(&MarvelHero::name) == "Thor"))) {
        cout << "hero = " << storage.dump(hero) << endl;
    }
    
    cout << "Heros with LENGTH(name) < 6 :" << endl;
    for(auto &hero : storage.iterate<MarvelHero>(where(length(&MarvelHero::name) < 6))) {
        cout << "hero = " << storage.dump(hero) << endl;
    }
    
    std::vector<MarvelHero> heroesByAlgorithm;
    heroesByAlgorithm.reserve(storage.count<MarvelHero>());
    {
        auto view = storage.iterate<MarvelHero>();
        std::copy(view.begin(),
                  view.end(),
                  std::back_inserter(heroesByAlgorithm));
    }
    cout << "heroesByAlgorithm.size = " << heroesByAlgorithm.size() << endl;
    
    return 0;
}
