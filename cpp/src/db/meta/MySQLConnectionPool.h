#include "mysql++/mysql++.h"

#include <string>
#include <unistd.h>
#include <atomic>

#include "db/Log.h"

namespace zilliz {
namespace milvus {
namespace engine {
namespace meta {

class MySQLConnectionPool : public mysqlpp::ConnectionPool {

public:
    // The object's only constructor
    MySQLConnectionPool(std::string dbName,
                        std::string userName,
                        std::string passWord,
                        std::string serverIp,
                        int port = 0,
                        int maxPoolSize = 8) :
                        db_(dbName),
                        user_(userName),
                        password_(passWord),
                        server_(serverIp),
                        port_(port),
                        max_pool_size_(maxPoolSize) {

        conns_in_use_ = 0;

        max_idle_time_ = 10; //10 seconds
    }

    // The destructor.  We _must_ call ConnectionPool::clear() here,
    // because our superclass can't do it for us.
    ~MySQLConnectionPool() override {
        clear();
    }

    mysqlpp::Connection *grab() override;

    // Other half of in-use conn count limit
    void release(const mysqlpp::Connection *pc) override;

//    int getConnectionsInUse();
//
//    void set_max_idle_time(int max_idle);

    std::string getDB();

protected:

    // Superclass overrides
    mysqlpp::Connection *create() override;

    void destroy(mysqlpp::Connection *cp) override;

    unsigned int max_idle_time() override;

private:
    // Number of connections currently in use
    std::atomic<int> conns_in_use_;

    // Our connection parameters
    std::string db_, user_, password_, server_;
    int port_;

    int max_pool_size_;

    unsigned int max_idle_time_;
};

} // namespace meta
} // namespace engine
} // namespace milvus
} // namespace zilliz