#include "MySQLConnectionPool.h"

namespace zilliz {
namespace milvus {
namespace engine {
namespace meta {

    // Do a simple form of in-use connection limiting: wait to return
    // a connection until there are a reasonably low number in use
    // already.  Can't do this in create() because we're interested in
    // connections actually in use, not those created.  Also note that
    // we keep our own count; ConnectionPool::size() isn't the same!
    mysqlpp::Connection *MySQLConnectionPool::grab() {
        while (conns_in_use_ > max_pool_size_) {
            sleep(1);
        }

        ++conns_in_use_;
        return mysqlpp::ConnectionPool::grab();
    }

    // Other half of in-use conn count limit
    void MySQLConnectionPool::release(const mysqlpp::Connection *pc) {
        mysqlpp::ConnectionPool::release(pc);

        if (conns_in_use_ <= 0) {
         ENGINE_LOG_WARNING << "MySQLConnetionPool::release: conns_in_use_ is less than zero.  conns_in_use_ = " << conns_in_use_;
        } else {
            --conns_in_use_;
        }
    }

//    int MySQLConnectionPool::getConnectionsInUse() {
//        return conns_in_use_;
//    }
//
//    void MySQLConnectionPool::set_max_idle_time(int max_idle) {
//        max_idle_time_ = max_idle;
//    }

    std::string MySQLConnectionPool::getDB() {
        return db_;
    }

    // Superclass overrides
    mysqlpp::Connection *MySQLConnectionPool::create() {

        try {
            // Create connection using the parameters we were passed upon
            // creation.
            mysqlpp::Connection *conn = new mysqlpp::Connection();
            conn->set_option(new mysqlpp::ReconnectOption(true));
            conn->connect(db_.empty() ? 0 : db_.c_str(),
                          server_.empty() ? 0 : server_.c_str(),
                          user_.empty() ? 0 : user_.c_str(),
                          password_.empty() ? 0 : password_.c_str(),
                          port_);
            return conn;
        } catch (const mysqlpp::ConnectionFailed& er) {
            ENGINE_LOG_ERROR << "Failed to connect to database server" << ": " << er.what();
            return nullptr;
        }
    }

    void MySQLConnectionPool::destroy(mysqlpp::Connection *cp) {
        // Our superclass can't know how we created the Connection, so
        // it delegates destruction to us, to be safe.
        delete cp;
    }

    unsigned int MySQLConnectionPool::max_idle_time() {
        return max_idle_time_;
    }

} // namespace meta
} // namespace engine
} // namespace milvus
} // namespace zilliz
