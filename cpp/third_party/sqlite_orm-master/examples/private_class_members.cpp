/**
 *  Sometimes data model classes may have private/protected fields to
 *  be mapped to a storage. In this case you have to pass setter and getter
 *  function instead of member pointer. Also to specify a column in select
 *  or where condition you have to add getter or setter function pointer. Both of them
 *  are mapped to the same column.
 *  getter must have definition like this: `const F& O::*() const`
 *  setter: `void O::*(F)`
 */

#include <sqlite_orm/sqlite_orm.h>
#include <iostream>
#include <cassert>

using std::cout;
using std::endl;

class Player {
    int id = 0;
    std::string name;
    
public:
    Player(){}
    
    Player(std::string name_):name(std::move(name_)){}
    
    Player(int id_, std::string name_):id(id_),name(std::move(name_)){}
    
    std::string getName() const {
        return this->name;
    }
    
    void setName(std::string name) {
        this->name = std::move(name);
    }
    
    int getId() const {
        return this->id;
    }
    
    void setId(int id) {
        this->id = id;
    }
};

int main(int argc, char **argv) {
    using namespace sqlite_orm;
    auto storage = make_storage("private.sqlite",
                                make_table("players",
                                           make_column("id",
                                                       &Player::setId,  //  setter
                                                       &Player::getId,  //  getter
                                                       primary_key()),
                                           make_column("name",
                                                       &Player::getName,    //  BTW order doesn't matter: setter can be placed before getter or opposite.
                                                       &Player::setName)));
    storage.sync_schema();
    storage.remove_all<Player>();
    
    auto soloId = storage.insert(Player("Solo"));
    
    auto playersCount = storage.count<Player>();
    cout << "players count = " << playersCount << endl;
    assert(playersCount == 1);
    
    cout << "solo = " << storage.dump(storage.get<Player>(soloId)) << endl;
    
    auto deadpoolId = storage.insert(Player("Deadpool"));
    
    cout << "deadpool = " << storage.dump(storage.get<Player>(deadpoolId)) << endl;
    
    playersCount = storage.count<Player>();
    cout << "players count = " << playersCount << endl;
    assert(playersCount == 2);
    
    auto idsOnly = storage.select(&Player::getId);  //  or storage.select(&Player::setId);
    cout << "idsOnly count = " << idsOnly.size() << endl;
    
    auto somePlayers = storage.get_all<Player>(where(lesser_than(length(&Player::getName), 5)));
    cout << "players with length(name) < 5 = " << somePlayers.size() << endl;
    assert(somePlayers.size() == 1);
    for(auto &player : somePlayers) {
        cout << storage.dump(player) << endl;
    }
}
