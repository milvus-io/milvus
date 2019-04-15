
#include <sqlite_orm/sqlite_orm.h>

#include <cassert>
#include <vector>
#include <string>
#include <iostream>
#include <memory>

using namespace sqlite_orm;

using std::cout;
using std::endl;

void testDifferentGettersAndSetters() {
    cout << __func__ << endl;
    
    struct User {
        int id;
        std::string name;
        
        int getIdByValConst() const {
            return this->id;
        }
        
        void setIdByVal(int id) {
            this->id = id;
        }
        
        std::string getNameByVal() {
            return this->name;
        }
        
        void setNameByConstRef(const std::string &name) {
            this->name = name;
        }
        
        const int& getConstIdByRefConst() const {
            return this->id;
        }
        
        void setIdByRef(int &id) {
            this->id = id;
        }
        
        const std::string& getConstNameByRefConst() const {
            return this->name;
        }
        
        void setNameByRef(std::string &name) {
            this->name = std::move(name);
        }
    };
    
    auto filename = "different.sqlite";
    auto storage0 = make_storage(filename,
                                 make_table("users",
                                            make_column("id", &User::id, primary_key()),
                                            make_column("name", &User::name)));
    auto storage1 = make_storage(filename,
                                 make_table("users",
                                            make_column("id", &User::getIdByValConst, &User::setIdByVal, primary_key()),
                                            make_column("name", &User::setNameByConstRef, &User::getNameByVal)));
    auto storage2 = make_storage(filename,
                                 make_table("users",
                                            make_column("id", &User::getConstIdByRefConst, &User::setIdByRef, primary_key()),
                                            make_column("name", &User::getConstNameByRefConst, &User::setNameByRef)));
    storage0.sync_schema();
    storage0.remove_all<User>();
    
    assert(storage0.count<User>() == 0);
    assert(storage1.count<User>() == 0);
    assert(storage2.count<User>() == 0);
    
    storage0.replace(User{ 1, "Da buzz" });
    
    assert(storage0.count<User>() == 1);
    assert(storage1.count<User>() == 1);
    assert(storage2.count<User>() == 1);
    
    {
        auto ids = storage0.select(&User::id);
        assert(ids.size() == 1);
        assert(ids.front() == 1);
        auto ids2 = storage1.select(&User::getIdByValConst);
        assert(ids == ids2);
        auto ids3 = storage1.select(&User::setIdByVal);
        assert(ids3 == ids2);
        auto ids4 = storage2.select(&User::getConstIdByRefConst);
        assert(ids4 == ids3);
        auto ids5 = storage2.select(&User::setIdByRef);
        assert(ids5 == ids4);
    }
    {
        auto ids = storage0.select(&User::id, where(is_equal(&User::name, "Da buzz")));
        assert(ids.size() == 1);
        assert(ids.front() == 1);
        auto ids2 = storage1.select(&User::getIdByValConst, where(is_equal(&User::setNameByConstRef, "Da buzz")));
        assert(ids == ids2);
        auto ids3 = storage1.select(&User::setIdByVal, where(is_equal(&User::getNameByVal, "Da buzz")));
        assert(ids3 == ids2);
        auto ids4 = storage2.select(&User::getConstIdByRefConst, where(is_equal(&User::getConstNameByRefConst, "Da buzz")));
        assert(ids4 == ids3);
        auto ids5 = storage2.select(&User::setIdByRef, where(is_equal(&User::setNameByRef, "Da buzz")));
        assert(ids5 == ids4);
    }
    {
        auto ids = storage0.select(columns(&User::id), where(is_equal(&User::name, "Da buzz")));
        assert(ids.size() == 1);
        assert(std::get<0>(ids.front()) == 1);
        auto ids2 = storage1.select(columns(&User::getIdByValConst), where(is_equal(&User::setNameByConstRef, "Da buzz")));
        assert(ids == ids2);
        auto ids3 = storage1.select(columns(&User::setIdByVal), where(is_equal(&User::getNameByVal, "Da buzz")));
        assert(ids3 == ids2);
        auto ids4 = storage2.select(columns(&User::getConstIdByRefConst), where(is_equal(&User::getConstNameByRefConst, "Da buzz")));
        assert(ids4 == ids3);
        auto ids5 = storage2.select(columns(&User::setIdByRef), where(is_equal(&User::setNameByRef, "Da buzz")));
        assert(ids5 == ids4);
    }
    {
        auto avgValue = storage0.avg(&User::id);
        assert(avgValue == storage1.avg(&User::getIdByValConst));
        assert(avgValue == storage1.avg(&User::setIdByVal));
        assert(avgValue == storage2.avg(&User::getConstIdByRefConst));
        assert(avgValue == storage2.avg(&User::setIdByRef));
    }
    {
        auto count = storage0.count(&User::id);
        assert(count == storage1.count(&User::getIdByValConst));
        assert(count == storage1.count(&User::setIdByVal));
        assert(count == storage2.count(&User::getConstIdByRefConst));
        assert(count == storage2.count(&User::setIdByRef));
    }
    {
        auto groupConcat = storage0.group_concat(&User::id);
        assert(groupConcat == storage1.group_concat(&User::getIdByValConst));
        assert(groupConcat == storage1.group_concat(&User::setIdByVal));
        assert(groupConcat == storage2.group_concat(&User::getConstIdByRefConst));
        assert(groupConcat == storage2.group_concat(&User::setIdByRef));
    }
    {
        auto arg = "ototo";
        auto groupConcat = storage0.group_concat(&User::id, arg);
        assert(groupConcat == storage1.group_concat(&User::getIdByValConst, arg));
        assert(groupConcat == storage1.group_concat(&User::setIdByVal, arg));
        assert(groupConcat == storage2.group_concat(&User::getConstIdByRefConst, arg));
        assert(groupConcat == storage2.group_concat(&User::setIdByRef, arg));
    }
    {
        auto max = storage0.max(&User::id);
        assert(max);
        assert(*max == *storage1.max(&User::getIdByValConst));
        assert(*max == *storage1.max(&User::setIdByVal));
        assert(*max == *storage2.max(&User::getConstIdByRefConst));
        assert(*max == *storage2.max(&User::setIdByRef));
    }
    {
        auto min = storage0.min(&User::id);
        assert(min);
        assert(*min == *storage1.min(&User::getIdByValConst));
        assert(*min == *storage1.min(&User::setIdByVal));
        assert(*min == *storage2.min(&User::getConstIdByRefConst));
        assert(*min == *storage2.min(&User::setIdByRef));
    }
    {
        auto sum = storage0.sum(&User::id);
        assert(sum);
        assert(*sum == *storage1.sum(&User::getIdByValConst));
        assert(*sum == *storage1.sum(&User::setIdByVal));
        assert(*sum == *storage2.sum(&User::getConstIdByRefConst));
        assert(*sum == *storage2.sum(&User::setIdByRef));
    }
    {
        auto total = storage0.total(&User::id);
        assert(total == storage1.total(&User::getIdByValConst));
        assert(total == storage1.total(&User::setIdByVal));
        assert(total == storage2.total(&User::getConstIdByRefConst));
        assert(total == storage2.total(&User::setIdByRef));
    }
}

void testExplicitColumns() {
    cout << __func__ << endl;
    
    struct Object {
        int id;
    };
    
    struct User : Object {
        std::string name;
        
        User(decltype(id) id_, decltype(name) name_): Object{id_}, name(std::move(name_)) {}
    };
    
    struct Token : Object {
        std::string token;
        int usedId;
        
        Token(decltype(id) id_, decltype(token) token_, decltype(usedId) usedId_): Object{id_}, token(std::move(token_)), usedId(usedId_) {}
    };
    
    auto storage = make_storage("column_pointer.sqlite",
                                make_table<User>("users",
                                                 make_column("id", &User::id, primary_key()),
                                                 make_column("name", &User::name)),
                                make_table<Token>("tokens",
                                                  make_column("id", &Token::id, primary_key()),
                                                  make_column("token", &Token::token),
                                                  make_column("used_id", &Token::usedId),
                                                  foreign_key(&Token::usedId).references(column<User>(&User::id))));
    storage.sync_schema();
    assert(storage.table_exists("users"));
    assert(storage.table_exists("tokens"));
    
    storage.remove_all<User>();
    storage.remove_all<Token>();
    
    auto brunoId = storage.insert(User{0, "Bruno"});
    auto zeddId = storage.insert(User{0, "Zedd"});
    
    assert(storage.count<User>() == 2);
    {
        auto w = where(is_equal(&User::name, "Bruno"));
        auto rows = storage.select(column<User>(&User::id), w);
        assert(rows.size() == 1);
        assert(rows.front() == brunoId);
        
        auto rows2 = storage.select(columns(column<User>(&User::id)), w);
        assert(rows2.size() == 1);
        assert(std::get<0>(rows2.front()) == brunoId);
        
        auto rows3 = storage.select(columns(column<User>(&Object::id)), w);
        assert(rows3 == rows2);
    }
    {
        auto rows = storage.select(column<User>(&User::id), where(is_equal(&User::name, "Zedd")));
        assert(rows.size() == 1);
        assert(rows.front() == zeddId);
    }
    
    auto abcId = storage.insert(Token(0, "abc", brunoId));
    {
        auto w = where(is_equal(&Token::token, "abc"));
        auto rows = storage.select(column<Token>(&Token::id), w);
        assert(rows.size() == 1);
        assert(rows.front() == abcId);
        
        auto rows2 = storage.select(columns(column<Token>(&Token::id), &Token::usedId), w);
        assert(rows2.size() == 1);
        assert(std::get<0>(rows2.front()) == abcId);
        assert(std::get<1>(rows2.front()) == brunoId);
    }
    
    auto joinedRows = storage.select(columns(&User::name, &Token::token),
                                     join<Token>(on(is_equal(&Token::usedId, column<User>(&User::id)))));
    assert(joinedRows.size() == 1);
    assert(std::get<0>(joinedRows.front()) == "Bruno");
    assert(std::get<1>(joinedRows.front()) == "abc");
}

void testJoinIteratorConstructorCompilationError() {
    cout << __func__ << endl;
    
    struct Tag {
        int objectId;
        std::string text;
    };
    
    auto storage = make_storage("join_error.sqlite",
                                make_table("tags",
                                           make_column("object_id",
                                                       &Tag::objectId),
                                           make_column("text",
                                                       &Tag::text)));
    storage.sync_schema();
    
    auto offs = 0;
    auto lim = 5;
    storage.select(columns(&Tag::text, count(&Tag::text)),
                   group_by(&Tag::text),
                   order_by(count(&Tag::text)).desc(),
                   limit(offs, lim));
}

void testLimits() {
    cout << __func__ << endl;
    
    auto storage2 = make_storage("limits.sqlite");
    auto storage = storage2;
    storage.sync_schema();

    {
        auto length = storage.limit.length();
        auto newLength = length - 10;
        storage.limit.length(newLength);
        length = storage.limit.length();
        assert(length == newLength);
    }
    {
        auto sqlLength = storage.limit.sql_length();
        auto newSqlLength = sqlLength - 10;
        storage.limit.sql_length(newSqlLength);
        sqlLength = storage.limit.sql_length();
        assert(sqlLength == newSqlLength);
    }
    {
        auto column = storage.limit.column();
        auto newColumn = column - 10;
        storage.limit.column(newColumn);
        column = storage.limit.column();
        assert(column == newColumn);
    }
    {
        auto exprDepth = storage.limit.expr_depth();
        auto newExprDepth = exprDepth - 10;
        storage.limit.expr_depth(newExprDepth);
        exprDepth = storage.limit.expr_depth();
        assert(exprDepth == newExprDepth);
    }
    {
        auto compoundSelect = storage.limit.compound_select();
        auto newCompoundSelect = compoundSelect - 10;
        storage.limit.compound_select(newCompoundSelect);
        compoundSelect = storage.limit.compound_select();
        assert(compoundSelect == newCompoundSelect);
    }
    {
        auto vdbeOp = storage.limit.vdbe_op();
        auto newVdbe_op = vdbeOp - 10;
        storage.limit.vdbe_op(newVdbe_op);
        vdbeOp = storage.limit.vdbe_op();
        assert(vdbeOp == newVdbe_op);
    }
    {
        auto functionArg = storage.limit.function_arg();
        auto newFunctionArg = functionArg - 10;
        storage.limit.function_arg(newFunctionArg);
        functionArg = storage.limit.function_arg();
        assert(functionArg == newFunctionArg);
    }
    {
        auto attached = storage.limit.attached();
        auto newAttached = attached - 1;
        storage.limit.attached(newAttached);
        attached = storage.limit.attached();
        assert(attached == newAttached);
    }
    {
        auto likePatternLength = storage.limit.like_pattern_length();
        auto newLikePatternLength = likePatternLength - 10;
        storage.limit.like_pattern_length(newLikePatternLength);
        likePatternLength = storage.limit.like_pattern_length();
        assert(likePatternLength == newLikePatternLength);
    }
    {
        auto variableNumber = storage.limit.variable_number();
        auto newVariableNumber = variableNumber - 10;
        storage.limit.variable_number(newVariableNumber);
        variableNumber = storage.limit.variable_number();
        assert(variableNumber == newVariableNumber);
    }
    {
        auto triggerDepth = storage.limit.trigger_depth();
        auto newTriggerDepth = triggerDepth - 10;
        storage.limit.trigger_depth(newTriggerDepth);
        triggerDepth = storage.limit.trigger_depth();
        assert(triggerDepth == newTriggerDepth);
    }
    {
        auto workerThreads = storage.limit.worker_threads();
        auto newWorkerThreads = workerThreads + 1;
        storage.limit.worker_threads(newWorkerThreads);
        workerThreads = storage.limit.worker_threads();
        assert(workerThreads == newWorkerThreads);
    }
}

void testExplicitInsert() {
    cout << __func__ << endl;
    
    struct User {
        int id;
        std::string name;
        int age;
        std::string email;
    };
    
    class Visit {
    public:
        const int& id() const {
            return _id;
        }
        
        void setId(int newValue) {
            _id = newValue;
        }
        
        const time_t& createdAt() const {
            return _createdAt;
        }
        
        void setCreatedAt(time_t newValue) {
            _createdAt = newValue;
        }
        
        const int& usedId() const {
            return _usedId;
        }
        
        void setUsedId(int newValue) {
            _usedId = newValue;
        }
        
    private:
        int _id;
        time_t _createdAt;
        int _usedId;
    };
    
    auto storage = make_storage("explicitinsert.sqlite",
                                make_table("users",
                                           make_column("id",
                                                       &User::id,
                                                       primary_key()),
                                           make_column("name",
                                                       &User::name),
                                           make_column("age",
                                                       &User::age),
                                           make_column("email",
                                                       &User::email,
                                                       default_value("dummy@email.com"))),
                                make_table("visits",
                                           make_column("id",
                                                       &Visit::setId,
                                                       &Visit::id,
                                                       primary_key()),
                                           make_column("created_at",
                                                       &Visit::createdAt,
                                                       &Visit::setCreatedAt,
                                                       default_value(10)),
                                           make_column("used_id",
                                                       &Visit::usedId,
                                                       &Visit::setUsedId)));
    
    storage.sync_schema();
    storage.remove_all<User>();
    storage.remove_all<Visit>();
    
    {
        //  insert user without id and email
        User user{};
        user.name = "Juan";
        user.age = 57;
        auto id = storage.insert(user, columns(&User::name, &User::age));
        assert(storage.get<User>(id).email == "dummy@email.com");
        
        //  insert user without email but with id
        User user2;
        user2.id = 2;
        user2.name = "Kevin";
        user2.age = 27;
        assert(user2.id == storage.insert(user2, columns(&User::id, &User::name, &User::age)));
        assert(storage.get<User>(user2.id).email == "dummy@email.com");
        
        //  insert user with both id and email
        User user3;
        user3.id = 3;
        user3.name = "Sia";
        user3.age = 42;
        user3.email = "sia@gmail.com";
        assert(user3.id == storage.insert(user3, columns(&User::id, &User::name, &User::age, &User::email)));
        auto insertedUser3 = storage.get<User>(user3.id);
        assert(insertedUser3.email == user3.email);
        assert(insertedUser3.age == user3.age);
        assert(insertedUser3.name == user3.name);
        
        //  insert without required columns and expect exception
        User user4;
        user4.name = "Egor";
        try {
            storage.insert(user4, columns(&User::name));
            assert(0);
        } catch (std::system_error e) {
            //        cout << e.what() << endl;
        }
    }
    {
        //  insert visit without id and createdAt
        Visit visit;
        visit.setUsedId(1);
        visit.setId(storage.insert(visit, columns(&Visit::usedId)));
        {
            auto visitFromStorage = storage.get<Visit>(visit.id());
            assert(visitFromStorage.createdAt() == 10);
            assert(visitFromStorage.usedId() == visit.usedId());
            storage.remove<Visit>(visitFromStorage.usedId());
        }
        
        visit.setId(storage.insert(visit, columns(&Visit::setUsedId)));
        {
            auto visitFromStorage = storage.get<Visit>(visit.id());
            assert(visitFromStorage.createdAt() == 10);
            assert(visitFromStorage.usedId() == visit.usedId());
            storage.remove<Visit>(visitFromStorage.usedId());
        }
        
        //  insert visit with id
        Visit visit2;
        visit2.setId(2);
        visit2.setUsedId(1);
        {
            assert(visit2.id() == storage.insert(visit2, columns(&Visit::id, &Visit::usedId)));
            auto visitFromStorage = storage.get<Visit>(visit2.id());
            assert(visitFromStorage.usedId() == visit2.usedId());
            storage.remove<Visit>(visit2.id());
        }
        {
            assert(visit2.id() == storage.insert(visit2, columns(&Visit::setId, &Visit::setUsedId)));
            auto visitFromStorage = storage.get<Visit>(visit2.id());
            assert(visitFromStorage.usedId() == visit2.usedId());
            storage.remove<Visit>(visit2.id());
        }
        
        //  insert without required columns and expect exception
        Visit visit3;
        visit3.setId(10);
        try {
            storage.insert(visit3, columns(&Visit::id));
            assert(0);
        } catch (std::system_error e) {
            //        cout << e.what() << endl;
        }
        
        try {
            storage.insert(visit3, columns(&Visit::setId));
            assert(0);
        } catch (std::system_error e) {
            //        cout << e.what() << endl;
        }
    }
}

void testCustomCollate() {
    cout << __func__ << endl;
    
    struct Item {
        int id;
        std::string name;
    };
    
    auto storage = make_storage("custom_collate.sqlite",
                                make_table("items",
                                           make_column("id",
                                                       &Item::id,
                                                       primary_key()),
                                           make_column("name",
                                                       &Item::name)));
//    storage.open_forever();
    storage.sync_schema();
    storage.remove_all<Item>();
    storage.insert(Item{ 0, "Mercury" });
    storage.insert(Item{ 0, "Mars" });
    storage.create_collation("ototo", [](int, const void *lhs, int, const void *rhs){
        return strcmp((const char*)lhs, (const char*)rhs);
    });
    storage.create_collation("alwaysequal", [](int, const void *lhs, int, const void *rhs){
        return 0;
    });
    auto rows = storage.select(&Item::name, where(is_equal(&Item::name, "Mercury").collate("ototo")));
    assert(rows.size() == 1);
    assert(rows.front() == "Mercury");
    
    rows = storage.select(&Item::name, where(is_equal(&Item::name, "Mercury").collate("alwaysequal")),
                          order_by(&Item::name).collate("ototo"));
    
    storage.create_collation("ototo", {});
    try {
        rows = storage.select(&Item::name, where(is_equal(&Item::name, "Mercury").collate("ototo")));
    } catch (std::system_error e) {
        cout << e.what() << endl;
    }
    try {
        rows = storage.select(&Item::name, where(is_equal(&Item::name, "Mercury").collate("ototo2")));
    } catch (std::system_error e) {
        cout << e.what() << endl;
    }
    rows = storage.select(&Item::name, where(is_equal(&Item::name, "Mercury").collate("alwaysequal")),
                                             order_by(&Item::name).collate_rtrim());
    
    rows = storage.select(&Item::name, where(is_equal(&Item::name, "Mercury").collate("alwaysequal")),
                          order_by(&Item::name).collate("alwaysequal"));
    assert(rows.size() == storage.count<Item>());
}

void testVacuum() {
    cout << __func__ << endl;
    
    struct Item {
        int id;
        std::string name;
    };
    
    auto storage = make_storage("vacuum.sqlite",
                                make_table("items",
                                           make_column("id",
                                                       &Item::id,
                                                       primary_key()),
                                           make_column("name",
                                                       &Item::name)));
    storage.sync_schema();
    storage.insert(Item{ 0, "One" });
    storage.insert(Item{ 0, "Two" });
    storage.insert(Item{ 0, "Three" });
    storage.insert(Item{ 0, "Four" });
    storage.insert(Item{ 0, "Five" });
    storage.remove_all<Item>();
    storage.vacuum();
}

void testAutoVacuum() {
    cout << __func__ << endl;
    
    auto filename = "autovacuum.sqlite";
    remove(filename);
    
    auto storage = make_storage(filename);
    
    
    storage.pragma.auto_vacuum(0);
    assert(storage.pragma.auto_vacuum() == 0);
    
    storage.pragma.auto_vacuum(1);
    assert(storage.pragma.auto_vacuum() == 1);
    
    storage.pragma.auto_vacuum(2);
    assert(storage.pragma.auto_vacuum() == 2);
}

void testOperators() {
    cout << __func__ << endl;
    
    struct Object {
        std::string name;
        int nameLen;
        int number;
    };
    
    auto storage = make_storage("",
                                make_table("objects",
                                           make_column("name",
                                                       &Object::name),
                                           make_column("name_len",
                                                       &Object::nameLen),
                                           make_column("number",
                                                       &Object::number)));
    storage.sync_schema();
    
    std::vector<std::string> names {
        "Zombie", "Eminem", "Upside down",
    };
    auto number = 10;
    for(auto &name : names) {
        storage.insert(Object{ name , int(name.length()), number });
    }
    std::string suffix = "ototo";
    auto rows = storage.select(columns(conc(&Object::name, suffix),
                                       c(&Object::name) || suffix,
                                       &Object::name || c(suffix),
                                       c(&Object::name) || c(suffix),
                                       
                                       add(&Object::nameLen, &Object::number),
                                       c(&Object::nameLen) + &Object::number,
                                       &Object::nameLen + c(&Object::number),
                                       c(&Object::nameLen) + c(&Object::number),
                                       c(&Object::nameLen) + 1000,
                                       
                                       sub(&Object::nameLen, &Object::number),
                                       c(&Object::nameLen) - &Object::number,
                                       &Object::nameLen - c(&Object::number),
                                       c(&Object::nameLen) - c(&Object::number),
                                       c(&Object::nameLen) - 1000,
                                       
                                       mul(&Object::nameLen, &Object::number),
                                       c(&Object::nameLen) * &Object::number,
                                       &Object::nameLen * c(&Object::number),
                                       c(&Object::nameLen) * c(&Object::number),
                                       c(&Object::nameLen) * 1000,
                                       
                                       div(&Object::nameLen, &Object::number),
                                       c(&Object::nameLen) / &Object::number,
                                       &Object::nameLen / c(&Object::number),
                                       c(&Object::nameLen) / c(&Object::number),
                                       c(&Object::nameLen) / 2));
    
    for(auto i = 0; i < rows.size(); ++i) {
        auto &row = rows[i];
        auto &name = names[i];
        assert(std::get<0>(row) == name + suffix);
        assert(std::get<1>(row) == std::get<0>(row));
        assert(std::get<2>(row) == std::get<1>(row));
        assert(std::get<3>(row) == std::get<2>(row));
        
        auto expectedAddNumber = int(name.length()) + number;
        assert(std::get<4>(row) == expectedAddNumber);
        assert(std::get<5>(row) == std::get<4>(row));
        assert(std::get<6>(row) == std::get<5>(row));
        assert(std::get<7>(row) == std::get<6>(row));
        assert(std::get<8>(row) == int(name.length()) + 1000);
        
        auto expectedSubNumber = int(name.length()) - number;
        assert(std::get<9>(row) == expectedSubNumber);
        assert(std::get<10>(row) == std::get<9>(row));
        assert(std::get<11>(row) == std::get<10>(row));
        assert(std::get<12>(row) == std::get<11>(row));
        assert(std::get<13>(row) == int(name.length()) - 1000);
        
        auto expectedMulNumber = int(name.length()) * number;
        assert(std::get<14>(row) == expectedMulNumber);
        assert(std::get<15>(row) == std::get<14>(row));
        assert(std::get<16>(row) == std::get<15>(row));
        assert(std::get<17>(row) == std::get<16>(row));
        assert(std::get<18>(row) == int(name.length()) * 1000);
        
        auto expectedDivNumber = int(name.length()) / number;
        assert(std::get<19>(row) == expectedDivNumber);
        assert(std::get<20>(row) == std::get<19>(row));
        assert(std::get<21>(row) == std::get<20>(row));
        assert(std::get<22>(row) == std::get<21>(row));
        assert(std::get<23>(row) == int(name.length()) / 2);
    }
    
    auto rows2 = storage.select(columns(mod(&Object::nameLen, &Object::number),
                                        c(&Object::nameLen) % &Object::number,
                                        &Object::nameLen % c(&Object::number),
                                        c(&Object::nameLen) % c(&Object::number),
                                        c(&Object::nameLen) % 5));
    for(auto i = 0; i < rows2.size(); ++i) {
        auto &row = rows2[i];
        auto &name = names[i];
        assert(std::get<0>(row) == name.length() % number);
        assert(std::get<1>(row) == std::get<0>(row));
        assert(std::get<2>(row) == std::get<1>(row));
        assert(std::get<3>(row) == std::get<2>(row));
        assert(std::get<4>(row) == name.length() % 5);
    }
}

void testMultiOrderBy() {
    cout << __func__ << endl;
    
    struct Singer {
        int id;
        std::string name;
        std::string gender;
    };
    
    auto storage = make_storage("",
                                make_table("singers",
                                           make_column("id",
                                                       &Singer::id,
                                                       primary_key()),
                                           make_column("name",
                                                       &Singer::name,
                                                       unique()),
                                           make_column("gender",
                                                       &Singer::gender)));
    storage.sync_schema();
    
    storage.insert(Singer{ 0, "Alexandra Stan", "female" });
    storage.insert(Singer{ 0, "Inna", "female" });
    storage.insert(Singer{ 0, "Krewella", "female" });
    storage.insert(Singer{ 0, "Sting", "male" });
    storage.insert(Singer{ 0, "Lady Gaga", "female" });
    storage.insert(Singer{ 0, "Rameez", "male" });
    
    {
        //  test double ORDER BY
        auto singers = storage.get_all<Singer>(multi_order_by(order_by(&Singer::name).asc().collate_nocase(), order_by(&Singer::gender).desc()));
//        cout << "singers count = " << singers.size() << endl;
        auto expectedIds = {1, 2, 3, 5, 6, 4};
        assert(expectedIds.size() == singers.size());
        auto it = expectedIds.begin();
        for(size_t i = 0; i < singers.size(); ++i) {
            assert(*it == singers[i].id);
            ++it;
        }
    }
    
    //  test multi ORDER BY ith singl column with single ORDER BY
    {
        auto singers = storage.get_all<Singer>(order_by(&Singer::id).asc());
        auto singers2 = storage.get_all<Singer>(multi_order_by(order_by(&Singer::id).asc()));
        assert(singers.size() == singers2.size());
        for(size_t i = 0; i < singers.size(); ++i) {
            assert(singers[i].id == singers2[i].id);
        }
    }
    
}

void testIssue105() {
    cout << __func__ << endl;
    struct Data {
        int str;
    };
    
    auto storage = make_storage("",
                                make_table("data",
                                           make_column("str", &Data::str, primary_key())));
    
    storage.sync_schema();
    
    Data d{0};
    storage.insert(d);
}

void testIssue86() {
    cout << __func__ << endl;
    
    struct Data
    {
        uint8_t mUsed=0;
        int mId = 0;
        int mCountryId = 0;
        int mLangId = 0;
        std::string mName;
    };
    
    auto storage = make_storage("",
                                make_table("cities",
                                           make_column("U", &Data::mUsed),
                                           make_column("Id", &Data::mId),
                                           make_column("CntId", &Data::mCountryId),
                                           make_column("LId", &Data::mLangId),
                                           make_column("N", &Data::mName)));
    storage.sync_schema();
    std::string CurrCity = "ototo";
    auto vms = storage.select(columns(&Data::mId, &Data::mLangId),
                              where(like(&Data::mName, CurrCity)));
}

void testIssue87() {
    cout << __func__ << endl;
    
    struct Data {
        uint8_t  mDefault; /**< 0=User or 1=Default*/
        uint8_t     mAppLang; //en_GB
        uint8_t     mContentLang1; //de_DE
        uint8_t     mContentLang2; //en_GB
        uint8_t     mContentLang3;
        uint8_t     mContentLang4;
        uint8_t     mContentLang5;
    };
    
    Data data;
    
    auto storage = make_storage("",
                                make_table("data",
                                           make_column("IsDef", &Data::mDefault, primary_key()),
                                           make_column("AL", &Data::mAppLang),
                                           make_column("CL1", &Data::mContentLang1),
                                           make_column("CL2", &Data::mContentLang2),
                                           make_column("CL3", &Data::mContentLang3),
                                           make_column("CL4", &Data::mContentLang4),
                                           make_column("CL5", &Data::mContentLang5)));
    storage.sync_schema();
    
    storage.update_all(set(c(&Data::mContentLang1) = data.mContentLang1,
                           c(&Data::mContentLang2) = data.mContentLang2,
                           c(&Data::mContentLang3) = data.mContentLang3,
                           c(&Data::mContentLang4) = data.mContentLang4,
                           c(&Data::mContentLang5) = data.mContentLang5),
                       where(c(&Data::mDefault) == data.mDefault));
}

void testForeignKey() {
    cout << __func__ << endl;

    struct Location {
        int id;
        std::string place;
        std::string country;
        std::string city;
        int distance;

    };

    struct Visit {
        int id;
        std::shared_ptr<int> location;
        std::shared_ptr<int> user;
        int visited_at;
        uint8_t mark;
    };

    //  this case didn't compile on linux until `typedef constraints_type` was added to `foreign_key_t`
    auto storage = make_storage("test_fk.sqlite",
                                make_table("location",
                                           make_column("id", &Location::id, primary_key()),
                                           make_column("place", &Location::place),
                                           make_column("country", &Location::country),
                                           make_column("city", &Location::city),
                                           make_column("distance", &Location::distance)),
                                make_table("visit",
                                           make_column("id", &Visit::id, primary_key()),
                                           make_column("location", &Visit::location),
                                           make_column("user", &Visit::user),
                                           make_column("visited_at", &Visit::visited_at),
                                           make_column("mark", &Visit::mark),
                                           foreign_key(&Visit::location).references(&Location::id)));
    storage.sync_schema();

    int fromDate = int(time(nullptr));
    int toDate = int(time(nullptr));
    int toDistance = 100;
    auto id = 10;
    storage.select(columns(&Visit::mark, &Visit::visited_at, &Location::place),
                   inner_join<Location>(on(is_equal(&Visit::location, &Location::id))),
                   where(is_equal(&Visit::user, id) and
                         greater_than(&Visit::visited_at, fromDate) and
                         lesser_than(&Visit::visited_at, toDate) and
                         lesser_than(&Location::distance, toDistance)),
                   order_by(&Visit::visited_at));
}

//  appeared after #57
void testForeignKey2() {
    cout << __func__ << endl;
    
    class test1 {
    public:
        // Constructors
        test1() {};
        
        // Variables
        int id;
        std::string val1;
        std::string val2;
    };
    
    class test2 {
    public:
        // Constructors
        test2() {};
        
        // Variables
        int id;
        int fk_id;
        std::string val1;
        std::string val2;
    };
    
    auto table1 = make_table("test_1",
                             make_column("id",
                                         &test1::id,
                                         primary_key()),
                             make_column("val1",
                                         &test1::val1),
                             make_column("val2",
                                         &test1::val2));

    auto table2 = make_table("test_2",
                             make_column("id",
                                         &test2::id,
                                         primary_key()),
                             make_column("fk_id",
                                         &test2::fk_id),
                             make_column("val1",
                                         &test2::val1),
                             make_column("val2",
                                         &test2::val2),
                             foreign_key(&test2::fk_id).references(&test1::id));

    auto storage = make_storage("test.sqlite",
                                table1,
                                table2);

    storage.sync_schema();


    test1 t1;
    t1.val1 = "test";
    t1.val2 = "test";
    storage.insert(t1);

    test1 t1_copy;
    t1_copy.val1 = "test";
    t1_copy.val2 = "test";
    storage.insert(t1_copy);

    test2 t2;
    t2.fk_id = 1;
    t2.val1 = "test";
    t2.val2 = "test";
    storage.insert(t2);

    t2.fk_id = 2;

    storage.update(t2);
}

void testTypeParsing() {
    cout << __func__ << endl;
 
    using namespace sqlite_orm::internal;

    //  int
    assert(*to_sqlite_type("INT") == sqlite_type::INTEGER);
    assert(*to_sqlite_type("integeer") == sqlite_type::INTEGER);
    assert(*to_sqlite_type("INTEGER") == sqlite_type::INTEGER);
    assert(*to_sqlite_type("TINYINT") == sqlite_type::INTEGER);
    assert(*to_sqlite_type("SMALLINT") == sqlite_type::INTEGER);
    assert(*to_sqlite_type("MEDIUMINT") == sqlite_type::INTEGER);
    assert(*to_sqlite_type("BIGINT") == sqlite_type::INTEGER);
    assert(*to_sqlite_type("UNSIGNED BIG INT") == sqlite_type::INTEGER);
    assert(*to_sqlite_type("INT2") == sqlite_type::INTEGER);
    assert(*to_sqlite_type("INT8") == sqlite_type::INTEGER);

    //  text
    assert(*to_sqlite_type("TEXT") == sqlite_type::TEXT);
    assert(*to_sqlite_type("CLOB") == sqlite_type::TEXT);
    for(auto i = 0; i< 255; ++i) {
        assert(*to_sqlite_type("CHARACTER(" + std::to_string(i) + ")") == sqlite_type::TEXT);
        assert(*to_sqlite_type("VARCHAR(" + std::to_string(i) + ")") == sqlite_type::TEXT);
        assert(*to_sqlite_type("VARYING CHARACTER(" + std::to_string(i) + ")") == sqlite_type::TEXT);
        assert(*to_sqlite_type("NCHAR(" + std::to_string(i) + ")") == sqlite_type::TEXT);
        assert(*to_sqlite_type("NATIVE CHARACTER(" + std::to_string(i) + ")") == sqlite_type::TEXT);
        assert(*to_sqlite_type("NVARCHAR(" + std::to_string(i) + ")") == sqlite_type::TEXT);
    }

    //  blob..
    assert(*to_sqlite_type("BLOB") == sqlite_type::BLOB);

    //  real
    assert(*to_sqlite_type("REAL") == sqlite_type::REAL);
    assert(*to_sqlite_type("DOUBLE") == sqlite_type::REAL);
    assert(*to_sqlite_type("DOUBLE PRECISION") == sqlite_type::REAL);
    assert(*to_sqlite_type("FLOAT") == sqlite_type::REAL);

    assert(*to_sqlite_type("NUMERIC") == sqlite_type::REAL);
    for(auto i = 0; i < 255; ++i) {
        for(auto j = 0; j < 10; ++j) {
            assert(*to_sqlite_type("DECIMAL(" + std::to_string(i) + "," + std::to_string(j) + ")") == sqlite_type::REAL);
        }
    }
    assert(*to_sqlite_type("BOOLEAN") == sqlite_type::REAL);
    assert(*to_sqlite_type("DATE") == sqlite_type::REAL);
    assert(*to_sqlite_type("DATETIME") == sqlite_type::REAL);



    assert(type_is_nullable<bool>::value == false);
    assert(type_is_nullable<char>::value == false);
    assert(type_is_nullable<unsigned char>::value == false);
    assert(type_is_nullable<signed char>::value == false);
    assert(type_is_nullable<short>::value == false);
    assert(type_is_nullable<unsigned short>::value == false);
    assert(type_is_nullable<int>::value == false);
    assert(type_is_nullable<unsigned int>::value == false);
    assert(type_is_nullable<long>::value == false);
    assert(type_is_nullable<unsigned long>::value == false);
    assert(type_is_nullable<long long>::value == false);
    assert(type_is_nullable<unsigned long long>::value == false);
    assert(type_is_nullable<float>::value == false);
    assert(type_is_nullable<double>::value == false);
    assert(type_is_nullable<long double>::value == false);
    assert(type_is_nullable<long double>::value == false);
    assert(type_is_nullable<std::string>::value == false);
    assert(type_is_nullable<std::shared_ptr<int>>::value == true);
    assert(type_is_nullable<std::shared_ptr<std::string>>::value == true);
    assert(type_is_nullable<std::unique_ptr<int>>::value == true);
    assert(type_is_nullable<std::unique_ptr<std::string>>::value == true);

}

/**
 *  this is the deal: assume we have a `users` table with schema
 *  `CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL, category_id INTEGER, surname TEXT)`.
 *  We create a storage and insert several objects. Next we simulate schema changing (app update for example): create
 *  another storage with a new schema partial of previous one: `CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)`.
 *  Next we call `sync_schema(true)` and assert that all users are saved. This test tests whether REMOVE COLUMN imitation works well.
 */
void testSyncSchema() {
    cout << __func__ << endl;

    //  this is an old version of user..
    struct UserBefore {
        int id;
        std::string name;
        std::shared_ptr<int> categoryId;
        std::shared_ptr<std::string> surname;
    };

    //  this is an new version of user..
    struct UserAfter {
        int id;
        std::string name;
    };

    //  create an old storage..
    auto filename = "sync_schema_text.sqlite";
    auto storage = make_storage(filename,
                                make_table("users",
                                           make_column("id",
                                                       &UserBefore::id,
                                                       primary_key()),
                                           make_column("name",
                                                       &UserBefore::name),
                                           make_column("category_id",
                                                       &UserBefore::categoryId),
                                           make_column("surname",
                                                       &UserBefore::surname)));

    //  sync in case if it is first launch..
    auto syncSchemaSimulationRes = storage.sync_schema_simulate();
    auto syncSchemaRes = storage.sync_schema();

    assert(syncSchemaRes == syncSchemaSimulationRes);

    //  remove old users in case the test was launched before..
    storage.remove_all<UserBefore>();

    //  create c++ objects to insert into table..
    std::vector<UserBefore> usersToInsert {
        { -1, "Michael", nullptr, std::make_shared<std::string>("Scofield") },
        { -1, "Lincoln", std::make_shared<int>(4), std::make_shared<std::string>("Burrows") },
        { -1, "Sucre", nullptr, nullptr },
        { -1, "Sara", std::make_shared<int>(996), std::make_shared<std::string>("Tancredi") },
        { -1, "John", std::make_shared<int>(100500), std::make_shared<std::string>("Abruzzi") },
        { -1, "Brad", std::make_shared<int>(65), nullptr },
        { -1, "Paul", std::make_shared<int>(65), nullptr },
    };

    for(auto &user : usersToInsert) {
        auto insertedId = storage.insert(user);
        user.id = insertedId;
    }

    //  assert count first cause we will be asserting row by row next..
    assert(storage.count<UserBefore>() == usersToInsert.size());

    //  now we create new storage with partial schema..
    auto newStorage = make_storage(filename,
                                   make_table("users",
                                              make_column("id",
                                                          &UserAfter::id,
                                                          primary_key()),
                                              make_column("name",
                                                          &UserAfter::name)));

    syncSchemaSimulationRes = newStorage.sync_schema_simulate(true);

    //  now call `sync_schema` with argument `preserve` as `true`. It will retain data in case `sqlite_orm` needs to remove a column..
    syncSchemaRes = newStorage.sync_schema(true);
    assert(syncSchemaRes.size() == 1);
    assert(syncSchemaRes.begin()->second == sync_schema_result::old_columns_removed);
    assert(syncSchemaSimulationRes == syncSchemaRes);

    //  get all users after syncing schema..
    auto usersFromDb = newStorage.get_all<UserAfter>(order_by(&UserAfter::id));

    assert(usersFromDb.size() == usersToInsert.size());

    for(auto i = 0; i < usersFromDb.size(); ++i) {
        auto &userFromDb = usersFromDb[i];
        auto &oldUser = usersToInsert[i];
        assert(userFromDb.id == oldUser.id);
        assert(userFromDb.name == oldUser.name);
    }

    auto usersCountBefore = newStorage.count<UserAfter>();

    syncSchemaSimulationRes = newStorage.sync_schema_simulate();
    syncSchemaRes = newStorage.sync_schema();
    assert(syncSchemaRes == syncSchemaSimulationRes);

    auto usersCountAfter = newStorage.count<UserAfter>();
    assert(usersCountBefore == usersCountAfter);

    //  test select..
    auto ids = newStorage.select(&UserAfter::id);
    auto users = newStorage.get_all<UserAfter>();
    decltype(ids) idsFromGetAll;
    idsFromGetAll.reserve(users.size());
    std::transform(users.begin(),
                   users.end(),
                   std::back_inserter(idsFromGetAll),
                   [=](auto &user) {
                       return user.id;
                   });
    assert(std::equal(ids.begin(),
                      ids.end(),
                      idsFromGetAll.begin(),
                      idsFromGetAll.end()));

}

void testSelect() {
    cout << __func__ << endl;

    sqlite3 *db;
    auto dbFileName = "test.db";
    auto rc = sqlite3_open(dbFileName, &db);
    assert(rc == SQLITE_OK);
    auto sql = "CREATE TABLE IF NOT EXISTS WORDS("
    "ID INTEGER PRIMARY        KEY AUTOINCREMENT      NOT NULL,"
    "CURRENT_WORD          TEXT     NOT NULL,"
    "BEFORE_WORD           TEXT     NOT NULL,"
    "AFTER_WORD            TEXT     NOT NULL,"
    "OCCURANCES            INT      NOT NULL);";

    char *errMsg = nullptr;
    rc = sqlite3_exec(db, sql, nullptr, nullptr, &errMsg);
    assert(rc == SQLITE_OK);

    sqlite3_stmt *stmt;

    //  delete previous words. This command is excess in travis or other docker based CI tools
    //  but it is required on local machine
    sql = "DELETE FROM WORDS";
    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr);
    assert(rc == SQLITE_OK);

    rc = sqlite3_step(stmt);
    if(rc != SQLITE_DONE){
        cout << sqlite3_errmsg(db) << endl;
        assert(0);
    }
    sqlite3_finalize(stmt);

    sql = "INSERT INTO WORDS (CURRENT_WORD, BEFORE_WORD, AFTER_WORD, OCCURANCES) VALUES(?, ?, ?, ?)";
    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr);
    assert(rc == SQLITE_OK);

    //  INSERT [ ID, 'best', 'behaviour', 'hey', 5 ]

    sqlite3_bind_text(stmt, 1, "best", -1, nullptr);
    sqlite3_bind_text(stmt, 2, "behaviour", -1, nullptr);
    sqlite3_bind_text(stmt, 3, "hey", -1, nullptr);
    sqlite3_bind_int(stmt, 4, 5);
    rc = sqlite3_step(stmt);
    if(rc != SQLITE_DONE){
        cout << sqlite3_errmsg(db) << endl;
        assert(0);
    }
    sqlite3_finalize(stmt);

    auto firstId = sqlite3_last_insert_rowid(db);

    //  INSERT [ ID, 'corruption', 'blood', 'brothers', 15 ]

    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr);
    assert(rc == SQLITE_OK);
    sqlite3_bind_text(stmt, 1, "corruption", -1, nullptr);
    sqlite3_bind_text(stmt, 2, "blood", -1, nullptr);
    sqlite3_bind_text(stmt, 3, "brothers", -1, nullptr);
    sqlite3_bind_int(stmt, 4, 15);
    rc = sqlite3_step(stmt);
    if(rc != SQLITE_DONE){
        cout << sqlite3_errmsg(db) << endl;
        assert(0);
    }
    sqlite3_finalize(stmt);

    auto secondId = sqlite3_last_insert_rowid(db);

    sqlite3_close(db);

    struct Word {
        int id;
        std::string currentWord;
        std::string beforeWord;
        std::string afterWord;
        int occurances;
    };

    auto storage = make_storage(dbFileName,
                                make_table("WORDS",
                                           make_column("ID",
                                                       &Word::id,
                                                       primary_key(),
                                                       autoincrement()),
                                           make_column("CURRENT_WORD",
                                                       &Word::currentWord),
                                           make_column("BEFORE_WORD",
                                                       &Word::beforeWord),
                                           make_column("AFTER_WORD",
                                                       &Word::afterWord),
                                           make_column("OCCURANCES",
                                                       &Word::occurances)));

    storage.sync_schema();  //  sync schema must not alter any data cause schemas are the same

    assert(storage.count<Word>() == 2);

    auto firstRow = storage.get_no_throw<Word>(firstId);
    assert(firstRow);
    assert(firstRow->currentWord == "best");
    assert(firstRow->beforeWord == "behaviour");
    assert(firstRow->afterWord == "hey");
    assert(firstRow->occurances == 5);

    auto secondRow = storage.get_no_throw<Word>(secondId);
    assert(secondRow);
    assert(secondRow->currentWord == "corruption");
    assert(secondRow->beforeWord == "blood");
    assert(secondRow->afterWord == "brothers");
    assert(secondRow->occurances == 15);

    auto cols = columns(&Word::id,
                        &Word::currentWord,
                        &Word::beforeWord,
                        &Word::afterWord,
                        &Word::occurances);
    auto rawTuples = storage.select(cols, where(eq(&Word::id, firstId)));
    assert(rawTuples.size() == 1);

    {
        auto &firstTuple = rawTuples.front();
        assert(std::get<0>(firstTuple) == firstId);
        assert(std::get<1>(firstTuple) == "best");
        assert(std::get<2>(firstTuple) == "behaviour");
        assert(std::get<3>(firstTuple) == "hey");
        assert(std::get<4>(firstTuple) == 5);
    }

    rawTuples = storage.select(cols, where(eq(&Word::id, secondId)));
    assert(rawTuples.size() == 1);

    {
        auto &secondTuple = rawTuples.front();
        assert(std::get<0>(secondTuple) == secondId);
        assert(std::get<1>(secondTuple) == "corruption");
        assert(std::get<2>(secondTuple) == "blood");
        assert(std::get<3>(secondTuple) == "brothers");
        assert(std::get<4>(secondTuple) == 15);
    }

    auto ordr = order_by(&Word::id);

    auto idsOnly = storage.select(&Word::id, ordr);
    assert(idsOnly.size() == 2);

    assert(idsOnly[0] == firstId);
    assert(idsOnly[1] == secondId);

    auto currentWordsOnly = storage.select(&Word::currentWord, ordr);
    assert(currentWordsOnly.size() == 2);

    assert(currentWordsOnly[0] == "best");
    assert(currentWordsOnly[1] == "corruption");

    auto beforeWordsOnly = storage.select(&Word::beforeWord, ordr);
    assert(beforeWordsOnly.size() == 2);

    assert(beforeWordsOnly[0] == "behaviour");
    assert(beforeWordsOnly[1] == "blood");

    auto afterWordsOnly = storage.select(&Word::afterWord, ordr);
    assert(afterWordsOnly.size() == 2);

    assert(afterWordsOnly[0] == "hey");
    assert(afterWordsOnly[1] == "brothers");

    auto occurencesOnly = storage.select(&Word::occurances, ordr);
    assert(occurencesOnly.size() == 2);

    assert(occurencesOnly[0] == 5);
    assert(occurencesOnly[1] == 15);

    //  test update_all with the same storage

    storage.update_all(set(assign(&Word::currentWord, "ototo")),
                       where(is_equal(&Word::id, firstId)));

    assert(storage.get<Word>(firstId).currentWord == "ototo");

}

void testRemove() {
    cout << __func__ << endl;

    struct Object {
        int id;
        std::string name;
    };

    {
        auto storage = make_storage("test_remove.sqlite",
                                    make_table("objects",
                                               make_column("id",
                                                           &Object::id,
                                                           primary_key()),
                                               make_column("name",
                                                           &Object::name)));
        storage.sync_schema();
        storage.remove_all<Object>();
        
        auto id1 = storage.insert(Object{ 0, "Skillet"});
        assert(storage.count<Object>() == 1);
        storage.remove<Object>(id1);
        assert(storage.count<Object>() == 0);
    }
    {
        auto storage = make_storage("test_remove.sqlite",
                                    make_table("objects",
                                               make_column("id",
                                                           &Object::id),
                                               make_column("name",
                                                           &Object::name),
                                               primary_key(&Object::id)));
        storage.sync_schema();
        storage.remove_all<Object>();
        
        auto id1 = storage.insert(Object{ 0, "Skillet"});
        assert(storage.count<Object>() == 1);
        storage.remove<Object>(id1);
        assert(storage.count<Object>() == 0);
    }
}

void testInsert() {
    cout << __func__ << endl;

    struct Object {
        int id;
        std::string name;
    };

    struct ObjectWithoutRowid {
        int id;
        std::string name;
    };

    auto storage = make_storage("test_insert.sqlite",
                                make_table("objects",
                                           make_column("id",
                                                       &Object::id,
                                                       primary_key()),
                                           make_column("name",
                                                       &Object::name)),
                                make_table("objects_without_rowid",
                                           make_column("id",
                                                       &ObjectWithoutRowid::id,
                                                       primary_key()),
                                           make_column("name",
                                                       &ObjectWithoutRowid::name)).without_rowid());

    storage.sync_schema();
    storage.remove_all<Object>();
    storage.remove_all<ObjectWithoutRowid>();

    for(auto i = 0; i < 100; ++i) {
        storage.insert(Object{
            0,
            "Skillet",
        });
        assert(storage.count<Object>() == i + 1);
    }

    auto initList = {
        Object{
            0,
            "Insane",
        },
        Object{
            0,
            "Super",
        },
        Object{
            0,
            "Sun",
        },
    };

    cout << "inserting range" << endl;
    auto countBefore = storage.count<Object>();
    storage.insert_range(initList.begin(),
                         initList.end());
    assert(storage.count<Object>() == countBefore + initList.size());


    //  test empty container
    std::vector<Object> emptyVector;
    storage.insert_range(emptyVector.begin(),
                         emptyVector.end());

    //  test insert without rowid
    storage.insert(ObjectWithoutRowid{ 10, "Life" });
    assert(storage.get<ObjectWithoutRowid>(10).name == "Life");
    storage.insert(ObjectWithoutRowid{ 20, "Death" });
    assert(storage.get<ObjectWithoutRowid>(20).name == "Death");
}

void testReplace() {
    cout << __func__ << endl;

    struct Object {
        int id;
        std::string name;
    };

    auto storage = make_storage("test_replace.sqlite",
                                make_table("objects",
                                           make_column("id",
                                                       &Object::id,
                                                       primary_key()),
                                           make_column("name",
                                                       &Object::name)));

    storage.sync_schema();
    storage.remove_all<Object>();

    storage.replace(Object{
        100,
        "Baby",
    });
    assert(storage.count<Object>() == 1);
    auto baby = storage.get<Object>(100);
    assert(baby.id == 100);
    assert(baby.name == "Baby");

    storage.replace(Object{
        200,
        "Time",
    });
    assert(storage.count<Object>() == 2);
    auto time = storage.get<Object>(200);
    assert(time.id == 200);
    assert(time.name == "Time");
    storage.replace(Object{
        100,
        "Ototo",
    });
    assert(storage.count<Object>() == 2);
    auto ototo = storage.get<Object>(100);
    assert(ototo.id == 100);
    assert(ototo.name == "Ototo");

    auto initList = {
        Object{
            300,
            "Iggy",
        },
        Object{
            400,
            "Azalea",
        },
    };
    storage.replace_range(initList.begin(), initList.end());
    assert(storage.count<Object>() == 4);

    //  test empty container
    std::vector<Object> emptyVector;
    storage.replace_range(emptyVector.begin(),
                          emptyVector.end());
}

void testEmptyStorage() {
    cout << __func__ << endl;

    auto storage = make_storage("empty.sqlite");
    storage.table_exists("table");
}

void testTransactionGuard() {
    cout << __func__ << endl;

    struct Object {
        int id;
        std::string name;
    };

    auto storage = make_storage("test_transaction_guard.sqlite",
                                make_table("objects",
                                           make_column("id",
                                                       &Object::id,
                                                       primary_key()),
                                           make_column("name",
                                                       &Object::name)));

    storage.sync_schema();
    storage.remove_all<Object>();

    storage.insert(Object{0, "Jack"});

    //  insert, call make a storage to cakk an exception and check that rollback was fired
    auto countBefore = storage.count<Object>();
    try{
        auto guard = storage.transaction_guard();

        storage.insert(Object{0, "John"});

        storage.get<Object>(-1);

        assert(false);
    }catch(...){
        auto countNow = storage.count<Object>();

        assert(countBefore == countNow);
    }

    //  check that one can call other transaction functions without exceptions
    storage.transaction([&]{return false;});

    //  commit explicitly and check that after exception data was saved
    countBefore = storage.count<Object>();
    try{
        auto guard = storage.transaction_guard();
        storage.insert(Object{0, "John"});
        guard.commit();
        storage.get<Object>(-1);
        assert(false);
    }catch(...){
        auto countNow = storage.count<Object>();

        assert(countNow == countBefore + 1);
    }

    //  rollback explicitly
    countBefore = storage.count<Object>();
    try{
        auto guard = storage.transaction_guard();
        storage.insert(Object{0, "Michael"});
        guard.rollback();
        storage.get<Object>(-1);
        assert(false);
    }catch(...){
        auto countNow = storage.count<Object>();
        assert(countNow == countBefore);
    }

    //  commit on exception
    countBefore = storage.count<Object>();
    try{
        auto guard = storage.transaction_guard();
        guard.commit_on_destroy = true;
        storage.insert(Object{0, "Michael"});
        storage.get<Object>(-1);
        assert(false);
    }catch(...){
        auto countNow = storage.count<Object>();
        assert(countNow == countBefore + 1);
    }

    //  work witout exception
    countBefore = storage.count<Object>();
    try{
        auto guard = storage.transaction_guard();
        guard.commit_on_destroy = true;
        storage.insert(Object{0, "Lincoln"});

    }catch(...){
        assert(0);
    }
    auto countNow = storage.count<Object>();
    assert(countNow == countBefore + 1);
}

/**
 *  Created by fixing https://github.com/fnc12/sqlite_orm/issues/42
 */
void testEscapeChars() {
    cout << __func__ << endl;

    struct Employee {
        int id;
        std::string name;
        int age;
        std::string address;
        double salary;
    };
    auto storage =  make_storage("test_escape_chars.sqlite",
                                 make_table("COMPANY",
                                            make_column("INDEX",
                                                        &Employee::id,
                                                        primary_key()),
                                            make_column("NAME",
                                                        &Employee::name),
                                            make_column("AGE",
                                                        &Employee::age),
                                            make_column("ADDRESS",
                                                        &Employee::address),
                                            make_column("SALARY",
                                                        &Employee::salary)));
    storage.sync_schema();
    storage.remove_all<Employee>();

    storage.insert(Employee{
        0,
        "Paul'l",
        20,
        "Sacramento 20",
        40000,
    });

    auto paulL = storage.get_all<Employee>(where(is_equal(&Employee::name, "Paul'l")));

    storage.replace(Employee{
        10,
        "Selena",
        24,
        "Florida",
        500000,
    });

    auto selena = storage.get<Employee>(10);
    auto selenaMaybe = storage.get_no_throw<Employee>(10);
    selena.name = "Gomez";
    storage.update(selena);
    storage.remove<Employee>(10);
}

//  appeared after #54
void testBlob() {
    cout << __func__ << endl;

    struct BlobData {
        std::vector<char> data;
    };
    using byte = char;

    auto generateData = [](int size) -> byte* {
        auto data = (byte*)::malloc(size * sizeof(byte));
        for (int i = 0; i < size; ++i) {
            if ((i+1) % 10 == 0) {
                data[i] = 0;
            } else {
                data[i] = (rand() % 100) + 1;
            }
        }
        return data;
    };

    auto storage = make_storage("blob.db",
                                make_table("blob",
                                           make_column("data", &BlobData::data)));
    storage.sync_schema();
    storage.remove_all<BlobData>();

    auto size = 100;
    auto data = generateData(size);

    //  write data
    BlobData d;
    std::vector<char> v(data, data + size);
    d.data = v;
    storage.insert(d);

    //  read data with get_all
    {
        auto vd = storage.get_all<BlobData>();
        assert(vd.size() == 1);
        auto &blob = vd.front();
        assert(blob.data.size() == size);
        assert(std::equal(data,
                          data + size,
                          blob.data.begin()));
    }

    //  read data with select (single column)
    {
        auto blobData = storage.select(&BlobData::data);
        assert(blobData.size() == 1);
        auto &blob = blobData.front();
        assert(blob.size() == size);
        assert(std::equal(data,
                          data + size,
                          blob.begin()));
    }

    //  read data with select (multi column)
    {
        auto blobData = storage.select(columns(&BlobData::data));
        assert(blobData.size() == 1);
        auto &blob = std::get<0>(blobData.front());
        assert(blob.size() == size);
        assert(std::equal(data,
                          data + size,
                          blob.begin()));
    }

    storage.insert(BlobData{});

    free(data);
}

//  appeared after #55
void testDefaultValue() {
    cout << __func__ << endl;

    struct User {
        int userId;
        std::string name;
        int age;
        std::string email;
    };

    auto storage1 = make_storage("test_db.sqlite",
                                 make_table("User",
                                            make_column("Id",
                                                        &User::userId,
                                                        primary_key()),
                                            make_column("Name",
                                                        &User::name),
                                            make_column("Age",
                                                        &User::age)));
    storage1.sync_schema();
    storage1.remove_all<User>();

    auto storage2 = make_storage("test_db.sqlite",
                                 make_table("User",
                                            make_column("Id",
                                                        &User::userId,
                                                        primary_key()),
                                            make_column("Name",
                                                        &User::name),
                                            make_column("Age",
                                                        &User::age),
                                            make_column("Email",
                                                        &User::email,
                                                        default_value("example@email.com"))));
    storage2.sync_schema();
}

//  after #18
void testCompositeKey() {
    cout << __func__ << endl;

    struct Record
    {
        int year;
        int month;
        int amount;
    };

    auto recordsTableName = "records";
    auto storage = make_storage("compisite_key.db",
                                make_table(recordsTableName,
                                           make_column("year", &Record::year),
                                           make_column("month", &Record::month),
                                           make_column("amount", &Record::amount),
                                           primary_key(&Record::year, &Record::month)));

    storage.sync_schema();
    assert(storage.sync_schema()[recordsTableName] == sqlite_orm::sync_schema_result::already_in_sync);

    auto storage2 = make_storage("compisite_key2.db",
                                 make_table(recordsTableName,
                                            make_column("year", &Record::year),
                                            make_column("month", &Record::month),
                                            make_column("amount", &Record::amount),
                                            primary_key(&Record::month, &Record::year)));
    storage2.sync_schema();
    assert(storage2.sync_schema()[recordsTableName] == sqlite_orm::sync_schema_result::already_in_sync);

    auto storage3 = make_storage("compisite_key3.db",
                                 make_table(recordsTableName,
                                            make_column("year", &Record::year),
                                            make_column("month", &Record::month),
                                            make_column("amount", &Record::amount),
                                            primary_key(&Record::amount, &Record::month, &Record::year)));
    storage3.sync_schema();
    assert(storage3.sync_schema()[recordsTableName] == sqlite_orm::sync_schema_result::already_in_sync);

}

void testOpenForever() {
    cout << __func__ << endl;

    struct User {
        int id;
        std::string name;
    };

    auto storage = make_storage("open_forever.sqlite",
                                make_table("users",
                                           make_column("id",
                                                       &User::id,
                                                       primary_key()),
                                           make_column("name",
                                                       &User::name)));
    storage.sync_schema();

    storage.remove_all<User>();

    storage.open_forever();

    storage.insert(User{ 1, "Demi" });
    storage.insert(User{ 2, "Luis" });
    storage.insert(User{ 3, "Shakira" });

    storage.open_forever();

    assert(storage.count<User>() == 3);

    storage.begin_transaction();
    storage.insert(User{ 4, "Calvin" });
    storage.commit();

    assert(storage.count<User>() == 4);
}

void testCurrentTimestamp() {
    cout << __func__ << endl;

    auto storage = make_storage("");
    assert(storage.current_timestamp().size());

    storage.begin_transaction();
    assert(storage.current_timestamp().size());
    storage.commit();
}

void testUserVersion() {
    cout << __func__ << endl;

    auto storage = make_storage("");
    auto version = storage.pragma.user_version();
    
    storage.pragma.user_version(version + 1);
    assert(storage.pragma.user_version() == version + 1);
    
    storage.begin_transaction();
    storage.pragma.user_version(version + 2);
    assert(storage.pragma.user_version() == version + 2);
    storage.commit();
}

void testSynchronous() {
    cout << __func__ << endl;
    
    auto storage = make_storage("");
    const auto value = 1;
    storage.pragma.synchronous(value);
    assert(storage.pragma.synchronous() == value);
    
    storage.begin_transaction();
    
    const auto newValue = 2;
    try{
        storage.pragma.synchronous(newValue);
        assert(0);
    }catch(std::system_error) {
        //  Safety level may not be changed inside a transaction
        assert(storage.pragma.synchronous() == value);
    }
    
    storage.commit();
}

void testAggregateFunctions() {
    cout << __func__ << endl;
    
    struct User {
        int id;
        std::string name;
        int age;
        
        void setId(int newValue) {
            this->id = newValue;
        }
        
        const int& getId() const {
            return this->id;
        }
        
        void setName(std::string newValue) {
            this->name = newValue;
        }
        
        const std::string& getName() const {
            return this->name;
        }
        
        void setAge(int newValue) {
            this->age = newValue;
        }
        
        const int& getAge() const {
            return this->age;
        }
    };
    
    auto storage = make_storage("test_aggregate.sqlite",
                                make_table("users",
                                           make_column("id",
                                                       &User::id,
                                                       primary_key()),
                                           make_column("name",
                                                       &User::name),
                                           make_column("age",
                                                       &User::age)));
    auto storage2 = make_storage("test_aggregate.sqlite",
                                 make_table("users",
                                            make_column("id",
                                                        &User::getId,
                                                        &User::setId,
                                                        primary_key()),
                                            make_column("name",
                                                        &User::getName,
                                                        &User::setName),
                                            make_column("age",
                                                        &User::getAge,
                                                        &User::setAge)));
    storage.sync_schema();
    storage.remove_all<User>();
    
    storage.replace(User{
        1,
        "Bebe Rexha",
        28,
    });
    storage.replace(User{
        2,
        "Rihanna",
        29,
    });
    storage.replace(User{
        3,
        "Cheryl Cole",
        34,
    });
    
    auto avgId = storage.avg(&User::id);
    assert(avgId == 2);
    
    auto avgId2 = storage2.avg(&User::getId);
    assert(avgId2 == avgId);
    
    auto avgId3 = storage2.avg(&User::setId);
    assert(avgId3 == avgId2);
    
    auto avgRaw = storage.select(avg(&User::id)).front();
    assert(avgRaw == avgId);
    
    auto distinctAvg = storage.select(distinct(avg(&User::id))).front();
    assert(distinctAvg == avgId);
    
    auto allAvg = storage.select(all(avg(&User::id))).front();
    assert(allAvg == avgId);
    
    auto avgRaw2 = storage2.select(avg(&User::getId)).front();
    assert(avgRaw2 == avgId);
    
    auto distinctAvg2 = storage2.select(distinct(avg(&User::setId))).front();
    assert(distinctAvg2 == avgId);
    
    auto allAvg2 = storage2.select(all(avg(&User::getId))).front();
    assert(allAvg2 == avgId);
}

void testBusyTimeout() {
    cout << __func__ << endl;
    
    auto storage = make_storage("testBusyTimeout.sqlite");
    storage.busy_timeout(500);
}

void testWideString() {
    cout << __func__ << endl;
    
    struct Alphabet {
        int id;
        std::wstring letters;
    };
    
    auto storage = make_storage("wideStrings.sqlite",
                                make_table("alphabets",
                                           make_column("id",
                                                       &Alphabet::id,
                                                       primary_key()),
                                           make_column("letters",
                                                       &Alphabet::letters)));
    storage.sync_schema();
    storage.remove_all<Alphabet>();
    
    std::vector<std::wstring> expectedStrings = {
        L"ﻌﺾﺒﺑﺏﺖﺘﺗﺕﺚﺜﺛﺙﺞﺠﺟﺝﺢﺤﺣﺡﺦﺨﺧﺥﺪﺩﺬﺫﺮﺭﺰﺯﺲﺴﺳﺱﺶﺸﺷﺵﺺﺼﺻﺹﻀﺿﺽﻂﻄﻃﻁﻆﻈﻇﻅﻊﻋﻉﻎﻐﻏﻍﻒﻔﻓﻑﻖﻘﻗﻕﻚﻜﻛﻙﻞﻠﻟﻝﻢﻤﻣﻡﻦﻨﻧﻥﻪﻬﻫﻩﻮﻭﻲﻴﻳﻱ",   //  arabic
        L"ԱաԲբԳգԴդԵեԶզԷէԸըԹթԺժԻիԼլԽխԾծԿկՀհՁձՂղՃճՄմՅյՆնՇշՈոՉչՊպՋջՌռՍսՎվՏտՐրՑցՒւՓփՔքՕօՖֆ",    //  armenian
        L"АаБбВвГгДдЕеЁёЖжЗзИиЙйКкЛлМмНнОоППРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЭэЮюЯя",  //  russian
        L"AaBbCcÇçDdEeFFGgĞğHhIıİiJjKkLlMmNnOoÖöPpRrSsŞşTtUuÜüVvYyZz",  //  turkish
    };
    for(auto &expectedString : expectedStrings) {
        auto id = storage.insert(Alphabet{0, expectedString});
        assert(storage.get<Alphabet>(id).letters == expectedString);
    }
    
}

void testRowId() {
    cout << __func__ << endl;
    
    struct SimpleTable {
        std::string letter;
        std::string desc;
    };
    
    auto storage = make_storage("rowid.sqlite",
                                make_table("tbl1",
                                           make_column("letter", &SimpleTable::letter),
                                           make_column("desc", &SimpleTable::desc)));
    storage.sync_schema();
    storage.remove_all<SimpleTable>();
    
    storage.insert(SimpleTable{"A", "first letter"});
    storage.insert(SimpleTable{"B", "second letter"});
    storage.insert(SimpleTable{"C", "third letter"});
    
    auto rows = storage.select(columns(rowid(),
                                       oid(),
                                       _rowid_(),
                                       rowid<SimpleTable>(),
                                       oid<SimpleTable>(),
                                       _rowid_<SimpleTable>(),
                                       &SimpleTable::letter,
                                       &SimpleTable::desc));
    for(auto i = 0; i < rows.size(); ++i) {
        auto &row = rows[i];
        assert(std::get<0>(row) == std::get<1>(row));
        assert(std::get<1>(row) == std::get<2>(row));
        assert(std::get<2>(row) == i + 1);
        assert(std::get<2>(row) == std::get<3>(row));
        assert(std::get<3>(row) == std::get<4>(row));
        assert(std::get<4>(row) == std::get<5>(row));
    }
}

int main(int argc, char **argv) {

    cout << "version = " << make_storage("").libversion() << endl;

    testTypeParsing();

    testSyncSchema();

    testInsert();

    testReplace();

    testSelect();

    testRemove();

    testEmptyStorage();

    testTransactionGuard();

    testEscapeChars();

    testForeignKey();

    testForeignKey2();

    testBlob();

    testDefaultValue();

    testCompositeKey();

    testOpenForever();

    testCurrentTimestamp();

    testUserVersion();
    
    testAggregateFunctions();
    
    testSynchronous();
    
    testBusyTimeout();
    
    testWideString();
    
    testIssue86();
    
    testIssue87();
    
    testRowId();
    
    testIssue105();
    
    testMultiOrderBy();
    
    testOperators();
    
    testAutoVacuum();
    
    testVacuum();
    
    testExplicitInsert();
    
    testCustomCollate();
    
    testLimits();
    
    testJoinIteratorConstructorCompilationError();
    
    testExplicitColumns();
    
    testDifferentGettersAndSetters();
}
