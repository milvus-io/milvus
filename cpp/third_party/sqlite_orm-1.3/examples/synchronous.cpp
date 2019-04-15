
#include <sqlite_orm/sqlite_orm.h>
#include <string>

struct Query
{
    std::string src_ip;
    uint16_t src_port;
    uint16_t txn_id;
    uint32_t tv_sec;
    uint32_t tv_usec;
    std::string name;
    uint16_t type;
};

int main(int argc, char** argv) {

    using namespace sqlite_orm;

    auto storage = make_storage("synchronous.sqlite",
                                make_table("queries",
                                           make_column("tv_sec", &Query::tv_sec),
                                           make_column("tv_usec", &Query::tv_usec),
                                           make_column("name", &Query::name),
                                           make_column("type", &Query::type),
                                           make_column("src_ip", &Query::src_ip),
                                           make_column("src_port", &Query::src_port),
                                           make_column("txn_id", &Query::txn_id))
                                );
    storage.sync_schema();
    storage.pragma.synchronous(0);
    storage.remove_all<Query>();

    return 0;
}
