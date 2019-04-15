<p align="center">
  <img src="https://github.com/fnc12/sqlite_orm/blob/master/logo.png" alt="Sublime's custom image" width="557"/>
</p>

[![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)
[![Build Status](https://travis-ci.org/fnc12/sqlite_orm.svg?branch=master)](https://travis-ci.org/fnc12/sqlite_orm)
[![Donate using PayPal](https://img.shields.io/badge/donate-PayPal-brightgreen.svg)](https://paypal.me/fnc12)

# SQLite ORM
SQLite ORM light header only library for modern C++

# Advantages

* **No raw string queries**
* **Intuitive syntax**
* **Comfortable interface - one code line per single query**
* **Built with modern C++14 features (no macros and external scripts)**
* **CRUD support**
* **Pure select query support**
* **STL compatible**
* **Custom types binding support**
* **BLOB support** - maps to `std::vector<char>` or one can bind your custom type
* **FOREIGN KEY support**
* **Composite key support**
* **JOIN support**
* **Transactions support**
* **Migrations functionality**
* **Powerful conditions**
* **ORDER BY and LIMIT, OFFSET support**
* **GROUP BY / DISTINCT support**
* **INDEX support**
* **Follows single responsibility principle** - no need write code inside your data model classes
* **Easy integration** - single header only lib.
* **The only dependency** - libsqlite3
* **C++ standard code style**
* **No undefined behaviour** - if something goes wrong lib throws an exception
* **In memory database support** - provide `:memory:` or empty filename
* **COLLATE support**
* **Limits setting/getting support**

`sqlite_orm` library allows to create easy data model mappings to your database schema. It is built to manage (CRUD) objects with a single column with primary key and without it. It also allows you to specify table names and column names explicitly no matter how your classes actually named. Take a look at example:

```c++

struct User{
    int id;
    std::string firstName;
    std::string lastName;
    int birthDate;
    std::shared_ptr<std::string> imageUrl;      
    int typeId;
};

struct UserType {
    int id;
    std::string name;
};

```

So we have database with predefined schema like 

`CREATE TABLE users (id integer primary key autoincrement, first_name text not null, last_name text not null, birth_date integer not null, image_url text, type_id integer not null)`

`CREATE TABLE user_types (id integer primary key autoincrement, name text not null DEFAULT 'name_placeholder')`

Now we tell `sqlite_orm` library about schema and provide database filename. We create `storage` service object that has CRUD interface. Also we create every table and every column. All code is intuitive and minimalistic.

```c++

using namespace sqlite_orm;
auto storage = make_storage("db.sqlite",
                            make_table("users",
                                       make_column("id", &User::id, autoincrement(), primary_key()),
                                       make_column("first_name", &User::firstName),
                                       make_column("last_name", &User::lastName),
                                       make_column("birth_date", &User::birthDate),
                                       make_column("image_url", &User::imageUrl),
                                       make_column("type_id", &User::typeId)),
                            make_table("user_types",
                                       make_column("id", &UserType::id, autoincrement(), primary_key()),
                                       make_column("name", &UserType::name, default_value("name_placeholder"))));
```

Too easy isn't it? You do not have to specify mapped type explicitly - it is deduced from your member pointers you pass during making a column (for example: `&User::id`). To create a column you have to pass two arguments at least: its name in the table and your mapped class member pointer. You can also add extra arguments to tell your storage about column's constraints like ~~`not_null`~~ (deduced from type), `primary_key`, `autoincrement`, `default_value` or `unique`(order isn't important).

If your datamodel classes have private or protected members to map to sqlite then you can make a storage with setter and getter functions. More info in the [example](https://github.com/fnc12/sqlite_orm/blob/master/examples/private_class_members.cpp).

More details about making storage can be found in [tutorial](https://github.com/fnc12/sqlite_orm/wiki/Making-storage).

# CRUD

Let's create and insert new `User` into database. First we need to create a `User` object with any id and call `insert` function. It will return id of just created user or throw exception if something goes wrong.

```c++
User user{-1, "Jonh", "Doe", 664416000, std::make_shared<std::string>("url_to_heaven"), 3 };
    
auto insertedId = storage.insert(user);
cout << "insertedId = " << insertedId << endl;      //  insertedId = 8
user.id = insertedId;

User secondUser{-1, "Alice", "Inwonder", 831168000, {} , 2};
insertedId = storage.insert(secondUser);
secondUser.id = insertedId;

```

Next let's get our user by id.

```c++
try{
    auto user = storage.get<User>(insertedId);
    cout << "user = " << user.firstName << " " << user.lastName << endl;
}catch(std::system_error e) {
    cout << e.what() << endl;
}catch(...){
    cout << "unknown exeption" << endl;
}
```

Probably you may not like throwing exceptions. Me too. Exception `std::system_error` is thrown because return type in `get` function is not nullable. You can use alternative version `get_no_throw` which returns `std::shared_ptr` and doesn't throw `not_found_exception` if nothing found - just returns `nullptr`.

```c++
if(auto user = storage.get_no_throw<User>(insertedId)){
    cout << "user = " << user->firstName << " " << user->lastName << endl;
}else{
    cout << "no user with id " << insertedId << endl;
}
```

`std::shared_ptr` is used as optional in `sqlite_orm`. Of course there is class optional in C++14 located at `std::experimental::optional`. But we don't want to use it until it is `experimental`.

We can also update our user. It updates row by id provided in `user` object and sets all other non `primary_key` fields to values stored in the passed `user` object. So you can just assign members to `user` object you want and call `update`

```c++
user.firstName = "Nicholas";
user.imageUrl = "https://cdn1.iconfinder.com/data/icons/man-icon-set/100/man_icon-21-512.png"
storage.update(user);
```

Also there is a non-CRUD update version `update_all`:

```c++
storage.update_all(set(c(&User::lastName) = "Hardey",
                       c(&User::typeId) = 2),
                   where(c(&User::firstName) == "Tom"));
```

And delete. To delete you have to pass id only, not whole object. Also we need to explicitly tell which class of object we want to delete. Function name is `remove` not `delete` cause `delete` is a reserved word in C++.

```c++
storage.remove<User>(insertedId)
```

Also we can extract all objects into `std::vector`.

```c++
auto allUsers = storage.get_all<User>();
cout << "allUsers (" << allUsers.size() << "):" << endl;
for(auto &user : allUsers) {
    cout << storage.dump(user) << endl; //  dump returns std::string with json-like style object info. For example: { id : '1', first_name : 'Jonh', last_name : 'Doe', birth_date : '664416000', image_url : 'https://cdn1.iconfinder.com/data/icons/man-icon-set/100/man_icon-21-512.png', type_id : '3' }
}
```

And one can specify return container type explicitly: let's get all users in `std::list`, not `std::vector`:

```c++
auto allUsersList = storage.get_all<User, std::list<User>>();
```

Container must be STL compatible (must have `push_back(T&&)` function in this case).

`get_all` can be too heavy for memory so you can iterate row by row (i.e. object by object):

```c++
for(auto &user : storage.iterate<User>()) {
    cout << storage.dump(user) << endl;
}
```

`iterate` member function returns adapter object that has `begin` and `end` member functions returning iterators that fetch object on dereference operator call.

CRUD functions `get`, `get_no_throw`, `remove`, `update` (not `insert`) work only if your type has a primary key column. If you try to `get` an object that is mapped to your storage but has no primary key column a `std::system_error` will be thrown cause `sqlite_orm` cannot detect an id. If you want to know how to perform a storage without primary key take a look at `date_time.cpp` example in `examples` folder.

# Aggregate Functions

```c++
//  SELECT AVG(id) FROM users
auto averageId = storage.avg(&User::id);    
cout << "averageId = " << averageId << endl;        //  averageId = 4.5
    
//  SELECT AVG(birth_date) FROM users
auto averageBirthDate = storage.avg(&User::birthDate);  
cout << "averageBirthDate = " << averageBirthDate << endl;      //  averageBirthDate = 6.64416e+08
  
//  SELECT COUNT(*) FROM users
auto usersCount = storage.count<User>();    
cout << "users count = " << usersCount << endl;     //  users count = 8

//  SELECT COUNT(id) FROM users
auto countId = storage.count(&User::id);    
cout << "countId = " << countId << endl;        //  countId = 8

//  SELECT COUNT(image_url) FROM users
auto countImageUrl = storage.count(&User::imageUrl);   
cout << "countImageUrl = " << countImageUrl << endl;      //  countImageUrl = 5

//  SELECT GROUP_CONCAT(id) FROM users
auto concatedUserId = storage.group_concat(&User::id);      
cout << "concatedUserId = " << concatedUserId << endl;      //  concatedUserId = 1,2,3,4,5,6,7,8

//  SELECT GROUP_CONCAT(id, "---") FROM users
auto concatedUserIdWithDashes = storage.group_concat(&User::id, "---");     
cout << "concatedUserIdWithDashes = " << concatedUserIdWithDashes << endl;      //  concatedUserIdWithDashes = 1---2---3---4---5---6---7---8

//  SELECT MAX(id) FROM users
if(auto maxId = storage.max(&User::id)){    
    cout << "maxId = " << *maxId <<endl;    //  maxId = 12  (maxId is std::shared_ptr<int>)
}else{
    cout << "maxId is null" << endl;
}
    
//  SELECT MAX(first_name) FROM users
if(auto maxFirstName = storage.max(&User::firstName)){ 
    cout << "maxFirstName = " << *maxFirstName << endl; //  maxFirstName = Jonh (maxFirstName is std::shared_ptr<std::string>)
}else{
    cout << "maxFirstName is null" << endl;
}

//  SELECT MIN(id) FROM users
if(auto minId = storage.min(&User::id)){    
    cout << "minId = " << *minId << endl;   //  minId = 1 (minId is std::shared_ptr<int>)
}else{
    cout << "minId is null" << endl;
}

//  SELECT MIN(last_name) FROM users
if(auto minLastName = storage.min(&User::lastName)){
    cout << "minLastName = " << *minLastName << endl;   //  minLastName = Doe
}else{
    cout << "minLastName is null" << endl;
}

//  SELECT SUM(id) FROM users
if(auto sumId = storage.sum(&User::id)){    //  sumId is std::shared_ptr<int>
    cout << "sumId = " << *sumId << endl;
}else{
    cout << "sumId is null" << endl;
}

//  SELECT TOTAL(id) FROM users
auto totalId = storage.total(&User::id);
cout << "totalId = " << totalId << endl;    //  totalId is double (always)
```

# Where conditions

You also can select objects with custom where conditions with `=`, `!=`, `>`, `>=`, `<`, `<=`, `IN`, `BETWEEN` and `LIKE`.

For example: let's select users with id lesser than 10:

```c++
//  SELECT * FROM users WHERE id < 10
auto idLesserThan10 = storage.get_all<User>(where(c(&User::id) < 10));
cout << "idLesserThan10 count = " << idLesserThan10.size() << endl;
for(auto &user : idLesserThan10) {
    cout << storage.dump(user) << endl;
}
```

Or select all users who's first name is not equal "John":

```c++
//  SELECT * FROM users WHERE first_name != 'John'
auto notJohn = storage.get_all<User>(where(c(&User::firstName) != "John"));
cout << "notJohn count = " << notJohn.size() << endl;
for(auto &user : notJohn) {
    cout << storage.dump(user) << endl;
}
```

By the way one can implement not equal in a different way using C++ negation operator:

```c++
auto notJohn2 = storage.get_all<User>(where(not (c(&User::firstName) == "John")));
```

You can use `!` and `not` in this case cause they are equal. Also you can chain several conditions with `and` and `or` operators. Let's try to get users with query with conditions like `where id >= 5 and id <= 7 and not id = 6`:

```c++
auto id5and7 = storage.get_all<User>(where(c(&User::id) <= 7 and c(&User::id) >= 5 and not (c(&User::id) == 6)));
cout << "id5and7 count = " << id5and7.size() << endl;
for(auto &user : id5and7) {
    cout << storage.dump(user) << endl;
}
```

Or let's just export two users with id 10 or id 16 (of course if these users exist):

```c++
auto id10or16 = storage.get_all<User>(where(c(&User::id) == 10 or c(&User::id) == 16));
cout << "id10or16 count = " << id10or16.size() << endl;
for(auto &user : id10or16) {
    cout << storage.dump(user) << endl;
}
```

In fact you can chain together any number of different conditions with any operator from `and`, `or` and `not`. All conditions are templated so there is no runtime overhead. And this makes `sqlite_orm` the most powerful **sqlite** C++ ORM library!

Moreover you can use parentheses to set the priority of query conditions:

```c++
auto cuteConditions = storage.get_all<User>(where((c(&User::firstName) == "John" or c(&User::firstName) == "Alex") and c(&User::id) == 4));  //  where (first_name = 'John' or first_name = 'Alex') and id = 4
cout << "cuteConditions count = " << cuteConditions.size() << endl; //  cuteConditions count = 1
cuteConditions = storage.get_all<User>(where(c(&User::firstName) == "John" or (c(&User::firstName) == "Alex" and c(&User::id) == 4)));   //  where first_name = 'John' or (first_name = 'Alex' and id = 4)
cout << "cuteConditions count = " << cuteConditions.size() << endl; //  cuteConditions count = 2
```

Also we can implement `get` by id with `get_all` and `where` like this:

```c++
//  SELECT * FROM users WHERE ( 2 = id )
auto idEquals2 = storage.get_all<User>(where(2 == c(&User::id)));
cout << "idEquals2 count = " << idEquals2.size() << endl;
if(idEquals2.size()){
    cout << storage.dump(idEquals2.front()) << endl;
}else{
    cout << "user with id 2 doesn't exist" << endl;
}
```

Lets try the `IN` operator:

```c++
//  SELECT * FROM users WHERE id IN (2, 4, 6, 8, 10)
auto evenLesserTen10 = storage.get_all<User>(where(in(&User::id, {2, 4, 6, 8, 10})));
cout << "evenLesserTen10 count = " << evenLesserTen10.size() << endl;
for(auto &user : evenLesserTen10) {
    cout << storage.dump(user) << endl;
}

//  SELECT * FROM users WHERE last_name IN ("Doe", "White")
auto doesAndWhites = storage.get_all<User>(where(in(&User::lastName, {"Doe", "White"})));
cout << "doesAndWhites count = " << doesAndWhites.size() << endl;
for(auto &user : doesAndWhites) {
    cout << storage.dump(user) << endl;
}
```

And `BETWEEN`:

```c++
//  SELECT * FROM users WHERE id BETWEEN 66 AND 68
auto betweenId = storage.get_all<User>(where(between(&User::id, 66, 68)));
cout << "betweenId = " << betweenId.size() << endl;
for(auto &user : betweenId) {
    cout << storage.dump(user) << endl;
}
```

And even `LIKE`:

```c++
//  SELECT * FROM users WHERE last_name LIKE 'D%'
auto whereNameLike = storage.get_all<User>(where(like(&User::lastName, "D%")));
cout << "whereNameLike = " << whereNameLike.size() << endl;
for(auto &user : whereNameLike) {
    cout << storage.dump(user) << endl;
}
```

Looks like magic but it works very simple. Cute function `c` (column) takes a class pointer and returns a special expression middle object that can be used with operators overloaded in `::sqlite_orm` namespace. Operator overloads act just like functions

* is_equal
* is_not_equal
* greater_than
* greater_or_equal
* lesser_than
* lesser_or_equal
* is_null
* is_not_null

that simulate binary comparison operator so they take 2 arguments: left hand side and right hand side. Arguments may be either member pointer of mapped class or any other expression (core function or literal). Binary comparison functions map arguments to text to be passed to sqlite engine to process query. Member pointers are being mapped to column names and literals to literals (numbers to raw numbers and string to quoted strings). Next `where` function places brackets around condition and adds "WHERE" keyword before condition text. Next resulted string appends to query string and is being processed further.

If you omit `where` function in `get_all` it will return all objects from a table:

```c++
auto allUsers = storage.get_all<User>();
```

Also you can use `remove_all` function to perform `DELETE FROM ... WHERE` query with the same type of conditions.

```c++
storage.remove_all<User>(where(c(&User::id) < 100));
```

# Raw select

If you need to extract only a single column (`SELECT %column_name% FROM %table_name% WHERE %conditions%`) you can use a non-CRUD `select` function:

```c++

//  SELECT id FROM users
auto allIds = storage.select(&User::id);    
cout << "allIds count = " << allIds.size() << endl; //  allIds is std::vector<int>
for(auto &id : allIds) {
    cout << id << " ";
}
cout << endl;

//  SELECT id FROM users WHERE last_name = 'Doe'
auto doeIds = storage.select(&User::id, where(c(&User::lastName) == "Doe"));
cout << "doeIds count = " << doeIds.size() << endl; //  doeIds is std::vector<int>
for(auto &doeId : doeIds) {
    cout << doeId << " ";
}
cout << endl;

//  SELECT last_name FROM users WHERE id < 300
auto allLastNames = storage.select(&User::lastName, where(c(&User::id) < 300));    
cout << "allLastNames count = " << allLastNames.size() << endl; //  allLastNames is std::vector<std::string>
for(auto &lastName : allLastNames) {
    cout << lastName << " ";
}
cout << endl;

//  SELECT id FROM users WHERE image_url IS NULL
auto idsWithoutUrls = storage.select(&User::id, where(is_null(&User::imageUrl)));
for(auto id : idsWithoutUrls) {
    cout << "id without image url " << id << endl;
}

//  SELECT id FROM users WHERE image_url IS NOT NULL
auto idsWithUrl = storage.select(&User::id, where(is_not_null(&User::imageUrl)));
for(auto id : idsWithUrl) {
    cout << "id with image url " << id << endl;
}
auto idsWithUrl2 = storage.select(&User::id, where(not is_null(&User::imageUrl)));
assert(std::equal(idsWithUrl2.begin(),
                  idsWithUrl2.end(),
                  idsWithUrl.begin()));
```

Also you're able to select several column in a vector of tuples. Example:

```c++
//  `SELECT first_name, last_name FROM users WHERE id > 250 ORDER BY id`
auto partialSelect = storage.select(columns(&User::firstName, &User::lastName),
                                    where(c(&User::id) > 250),
                                    order_by(&User::id));
cout << "partialSelect count = " << partialSelect.size() << endl;
for(auto &t : partialSelect) {
    auto &firstName = std::get<0>(t);
    auto &lastName = std::get<1>(t);
    cout << firstName << " " << lastName << endl;
}
```

# ORDER BY support

ORDER BY query option can be applied to `get_all` and `select` functions just like `where` but with `order_by` function. It can be mixed with WHERE in a single query. Examples:

```c++
//  `SELECT * FROM users ORDER BY id`
auto orderedUsers = storage.get_all<User>(order_by(&User::id));
cout << "orderedUsers count = " << orderedUsers.size() << endl;
for(auto &user : orderedUsers) {
    cout << storage.dump(user) << endl;
}

//  `SELECT * FROM users WHERE id < 250 ORDER BY first_name`
auto orderedUsers2 = storage.get_all<User>(where(c(&User::id) < 250), order_by(&User::firstName));
cout << "orderedUsers2 count = " << orderedUsers2.size() << endl;
for(auto &user : orderedUsers2) {
    cout << storage.dump(user) << endl;
}

//  `SELECT * FROM users WHERE id > 100 ORDER BY first_name ASC`
auto orderedUsers3 = storage.get_all<User>(where(c(&User::id) > 100), order_by(&User::firstName).asc());
cout << "orderedUsers3 count = " << orderedUsers3.size() << endl;
for(auto &user : orderedUsers3) {
    cout << storage.dump(user) << endl;
}

//  `SELECT * FROM users ORDER BY id DESC`
auto orderedUsers4 = storage.get_all<User>(order_by(&User::id).desc());
cout << "orderedUsers4 count = " << orderedUsers4.size() << endl;
for(auto &user : orderedUsers4) {
    cout << storage.dump(user) << endl;
}

//  `SELECT first_name FROM users ORDER BY ID DESC`
auto orderedFirstNames = storage.select(&User::firstName, order_by(&User::id).desc());
cout << "orderedFirstNames count = " << orderedFirstNames.size() << endl;
for(auto &firstName : orderedFirstNames) {
    cout << "firstName = " << firstName << endl;
}
```

# LIMIT and OFFSET

There are three available versions of `LIMIT`/`OFFSET` options:

- LIMIT %limit%
- LIMIT %limit% OFFSET %offset%
- LIMIT %offset%, %limit%

All these versions available with the same interface:

```c++
//  `SELECT * FROM users WHERE id > 250 ORDER BY id LIMIT 5`
auto limited5 = storage.get_all<User>(where(c(&User::id) > 250),
                                      order_by(&User::id),
                                      limit(5));
cout << "limited5 count = " << limited5.size() << endl;
for(auto &user : limited5) {
    cout << storage.dump(user) << endl;
}

//  `SELECT * FROM users WHERE id > 250 ORDER BY id LIMIT 5, 10`
auto limited5comma10 = storage.get_all<User>(where(c(&User::id) > 250),
                                             order_by(&User::id),
                                             limit(5, 10));
cout << "limited5comma10 count = " << limited5comma10.size() << endl;
for(auto &user : limited5comma10) {
    cout << storage.dump(user) << endl;
}

//  `SELECT * FROM users WHERE id > 250 ORDER BY id LIMIT 5 OFFSET 10`
auto limit5offset10 = storage.get_all<User>(where(c(&User::id) > 250),
                                            order_by(&User::id),
                                            limit(5, offset(10)));
cout << "limit5offset10 count = " << limit5offset10.size() << endl;
for(auto &user : limit5offset10) {
    cout << storage.dump(user) << endl;
}
```

Please beware that queries `LIMIT 5, 10` and `LIMIT 5 OFFSET 10` mean different. `LIMIT 5, 10` means `LIMIT 10 OFFSET 5`.

# JOIN support

You can perform simple `JOIN`, `CROSS JOIN`, `INNER JOIN`, `LEFT JOIN` or `LEFT OUTER JOIN` in your query. Instead of joined table specify mapped type. Example for doctors and visits:

```c++
//  SELECT a.doctor_id, a.doctor_name,
//      c.patient_name, c.vdate
//  FROM doctors a
//  LEFT JOIN visits c
//  ON a.doctor_id=c.doctor_id;
auto rows = storage2.select(columns(&Doctor::id, &Doctor::name, &Visit::patientName, &Visit::vdate),
                            left_join<Visit>(on(c(&Doctor::id) == &Visit::doctorId)));  //  one `c` call is enough cause operator overloads are templated
for(auto &row : rows) {
    cout << std::get<0>(row) << '\t' << std::get<1>(row) << '\t' << std::get<2>(row) << '\t' << std::get<3>(row) << endl;
}
cout << endl;
```

Simple `JOIN`:

```c++
//  SELECT a.doctor_id,a.doctor_name,
//      c.patient_name,c.vdate
//  FROM doctors a
//  JOIN visits c
//  ON a.doctor_id=c.doctor_id;
rows = storage2.select(columns(&Doctor::id, &Doctor::name, &Visit::patientName, &Visit::vdate),
                       join<Visit>(on(c(&Doctor::id) == &Visit::doctorId)));
for(auto &row : rows) {
    cout << std::get<0>(row) << '\t' << std::get<1>(row) << '\t' << std::get<2>(row) << '\t' << std::get<3>(row) << endl;
}
cout << endl;
```

Two `INNER JOIN`s in one query:

```c++
//  SELECT
//      trackid,
//      tracks.name AS Track,
//      albums.title AS Album,
//      artists.name AS Artist
//  FROM
//      tracks
//  INNER JOIN albums ON albums.albumid = tracks.albumid
//  INNER JOIN artists ON artists.artistid = albums.artistid;
auto innerJoinRows2 = storage.select(columns(&Track::trackId, &Track::name, &Album::title, &Artist::name),
                                     inner_join<Album>(on(c(&Album::albumId) == &Track::albumId)),
                                     inner_join<Artist>(on(c(&Artist::artistId) == &Album::artistId)));
//  innerJoinRows2 is std::vector<std::tuple<decltype(&Track::trackId), decltype(&Track::name), decltype(&Album::title), decltype(&Artist::name)>>
```

More join examples can be found in [examples folder](https://github.com/fnc12/sqlite_orm/blob/master/examples/left_and_inner_join.cpp).

# Migrations functionality

There are no explicit `up` and `down` functions that are used to be used in migrations. Instead `sqlite_orm` offers `sync_schema` function that takes responsibility of comparing actual db file schema with one you specified in `make_storage` call and if something is not equal it alters or drops/creates schema.

```c++
storage.sync_schema();
//  or
storage.sync_schema(true);
```

Please beware that `sync_schema` doesn't guarantee that data will be saved. It *tries* to save it only. Below you can see rules list that `sync_schema` follows during call:
* if there are excess tables exist in db they are ignored (not dropped)
* every table from storage is compared with it's db analog and 
    * if table doesn't exist it is created
    * if table exists its colums are being compared with table_info from db and
        * if there are columns in db that do not exist in storage (excess) table will be dropped and recreated if `preserve` is `false`, and table will be copied into temporary table without excess columns, source table will be dropped, copied table will be renamed to source table (sqlite remove column technique) if `preserve` is `true`. `preserve` is the first argument in `sync_schema` function. It's default value is `false`. Beware that setting it to `true` may take time for copying table rows.
        * if there are columns in storage that do not exist in db they will be added using 'ALTER TABLE ... ADD COLUMN ...' command and table data will not be dropped but if any of added columns is null but has not default value table will be dropped and recreated
        * if there is any column existing in both db and storage but differs by any of properties (type, pk, notnull) table will be dropped and recreated (dflt_value isn't checked cause there can be ambiguity in default values, please beware).

The best practice is to call this function right after storage creation.

# Transactions

There are three ways to begin and commit/rollback transactions:
* explicitly call `begin_transaction();`, `rollback();` or `commit();` functions
* use `transaction` function which begins transaction implicitly and takes a lambda argument which returns true for commit and false for rollback. All storage calls performed in lambda can be commited or rollbacked by returning `true` or `false`.
* use `transaction_guard` function which returns a guard object which works just like `lock_guard` for `std::mutex`.

Example for explicit call:

```c++
auto secondUser = storage.get<User>(2);

storage.begin_transaction();
secondUser.typeId = 3;
storage.update(secondUser);
storage.rollback(); //  or storage.commit();

secondUser = storage.get<decltype(secondUser)>(secondUser.id);
assert(secondUser.typeId != 3);
```

Example for implicit call:

```c++
storage.transaction([&] () mutable {    //  mutable keyword allows make non-const function calls
    auto secondUser = storage.get<User>(2);
    secondUser.typeId = 1;
    storage.update(secondUser);
    auto gottaRollback = bool(rand() % 2);
    if(gottaRollback){  //  dummy condition for test
        return false;   //  exits lambda and calls ROLLBACK
    }
    return true;        //  exits lambda and calls COMMIT
});
```

The second way guarantees that `commit` or `rollback` will be called. You can use either way.

Trancations are useful with `changes` sqlite function that returns number of rows modified.

```c++
storage.transaction([&] () mutable {
    storage.remove_all<User>(where(c(&User::id) < 100));
    auto usersRemoved = storage.changes();
    cout << "usersRemoved = " << usersRemoved << endl;
    return true;
});
```

It will print a number of deleted users (rows). But if you call `changes` without a transaction and your database is located in file not in RAM the result will be 0 always cause `sqlite_orm` opens and closes connection every time you call a function without a transaction.

Also a `transaction` function returns `true` if transaction is commited and `false` if it is rollbacked. It can be useful if your next moves depend on transaction result:

```c++
auto commited = storage.transaction([&] () mutable {    
    auto secondUser = storage.get<User>(2);
    secondUser.typeId = 1;
    storage.update(secondUser);
    auto gottaRollback = bool(rand() % 2);
    if(gottaRollback){  //  dummy condition for test
        return false;   //  exits lambda and calls ROLLBACK
    }
    return true;        //  exits lambda and calls COMMIT
});
if(commited){
    cout << "Commited successfully, go on." << endl;
}else{
    cerr << "Commit failed, process an error" << endl;
}
```

Example for `transaction_guard` function:

```c++
try{
  auto guard = storage.transaction_guard(); //  calls BEGIN TRANSACTION and returns guard object
  user.name = "Paul";
  auto notExisting = storage.get<User>(-1); //  exception is thrown here, guard calls ROLLBACK in its destructor
  guard.commit();
}catch(...){
  cerr << "exception" << endl;
}
```

# In memory database

To manage in memory database just provide `:memory:` or `""` instead as filename to `make_storage`.

# Comparison with other C++ libs

|   |sqlite_orm|[SQLiteCpp](https://github.com/SRombauts/SQLiteCpp)|[hiberlite](https://github.com/paulftw/hiberlite)|[ODB](https://www.codesynthesis.com/products/odb/)|
|---|:---:|:---:|:---:|:---:|
|Schema sync|yes|no|yes|no|
|Single responsibility principle|yes|yes|no|no|
|STL compatible|yes|no|no|no|
|No raw string queries|yes|no|yes|yes|
|Transactions|yes|yes|no|yes|
|Custom types binding|yes|no|yes|yes|
|Doesn't use macros and/or external codegen scripts|yes|yes|no|no|
|Aggregate functions|yes|yes|no|yes|

# Notes

To work well your data model class must be default constructable and must not have const fields mapped to database cause they are assigned during queries. Otherwise code won't compile on line with member assignment operator.

For more details please check the project [wiki](https://github.com/fnc12/sqlite_orm/wiki).

# Installation

Just put `include/sqlite_orm/sqlite_orm.h` into you folder with headers. Also it is recommended to keep project libraries' sources in separate folders cause there is no normal dependency manager for C++ yet.

# Requirements

* C++14 compatible compiler (not C++11 cause of templated lambdas in the lib).
* libsqlite3 linked to your binary
