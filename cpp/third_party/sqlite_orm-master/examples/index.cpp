
#include <sqlite_orm/sqlite_orm.h>

#include <string>
#include <iostream>

using std::cout;
using std::endl;

struct Contract {
    std::string firstName;
    std::string lastName;
    std::string email;
};

using namespace sqlite_orm;

//  beware - put `make_index` before `make_table` cause `sync_schema` is called in reverse order
//  otherwise you'll receive an exception
auto storage = make_storage("index.sqlite",
                            make_index("idx_contacts_name", &Contract::firstName, &Contract::lastName),
                            make_unique_index("idx_contacts_email", &Contract::email),
                            make_table("contacts",
                                       make_column("first_name", &Contract::firstName),
                                       make_column("last_name", &Contract::lastName),
                                       make_column("email", &Contract::email)));

int main(int argc, char **argv) {
    
    storage.sync_schema();
    storage.remove_all<Contract>();
    
    storage.insert(Contract{
        "John",
        "Doe",
        "john.doe@sqlitetutorial.net",
    });
    try{
        storage.insert(Contract{
            "Johny",
            "Doe",
            "john.doe@sqlitetutorial.net",
        });
    }catch(std::system_error e){
        cout << e.what() << endl;
    }
    std::vector<Contract> moreContracts = {
        Contract{
            "David",
            "Brown",
            "david.brown@sqlitetutorial.net",
        },
        Contract{
            "Lisa",
            "Smith",
            "lisa.smith@sqlitetutorial.net",
        },
    };
    storage.insert_range(moreContracts.begin(),
                         moreContracts.end());
    
    auto lisas = storage.get_all<Contract>(where(c(&Contract::email) == "lisa.smith@sqlitetutorial.net"));
    
    storage.drop_index("idx_contacts_name");
    storage.drop_index("idx_contacts_email");
    
    return 0;
}
