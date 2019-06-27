#include "mysql++/mysql++.h"

#include <string>
#include <unistd.h>

#include "Log.h"

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
                        maxPoolSize_(maxPoolSize)
                        {

        conns_in_use_ = 0;

        maxIdleTime_ = 10; //10 seconds
    }

    // The destructor.  We _must_ call ConnectionPool::clear() here,
    // because our superclass can't do it for us.
    ~MySQLConnectionPool() override {
        clear();
    }

    // Do a simple form of in-use connection limiting: wait to return
    // a connection until there are a reasonably low number in use
    // already.  Can't do this in create() because we're interested in
    // connections actually in use, not those created.  Also note that
    // we keep our own count; ConnectionPool::size() isn't the same!
    mysqlpp::Connection* grab() override {
        while (conns_in_use_ > maxPoolSize_) {
//            cout.put('R'); cout.flush(); // indicate waiting for release
            sleep(1);
        }

//        ENGINE_LOG_DEBUG << "conns_in_use_ in grab: " << conns_in_use_ << std::endl;
        ++conns_in_use_;
        return mysqlpp::ConnectionPool::grab();
    }

    // Other half of in-use conn count limit
    void release(const mysqlpp::Connection* pc) override {
        mysqlpp::ConnectionPool::release(pc);
//        ENGINE_LOG_DEBUG << "conns_in_use_ in release: " << conns_in_use_ << std::endl;
        if (conns_in_use_ <= 0) {
            ENGINE_LOG_WARNING << "MySQLConnetionPool::release: conns_in_use_ is less than zero.  conns_in_use_ = " << conns_in_use_ << std::endl;
        }
        else {
            --conns_in_use_;
        }
    }

    void set_max_idle_time(int max_idle) {
        maxIdleTime_ = max_idle;
    }

protected:

    // Superclass overrides
    mysqlpp::Connection* create() override {
        // Create connection using the parameters we were passed upon
        // creation.
//        cout.put('C'); cout.flush(); // indicate connection creation
        mysqlpp::Connection* conn = new mysqlpp::Connection();
        conn->set_option(new mysqlpp::ReconnectOption(true));
        conn->connect(db_.empty() ? 0 : db_.c_str(),
                      server_.empty() ? 0 : server_.c_str(),
                      user_.empty() ? 0 : user_.c_str(),
                      password_.empty() ? 0 : password_.c_str(),
                      port_);
        return conn;
    }

    void destroy(mysqlpp::Connection* cp) override {
        // Our superclass can't know how we created the Connection, so
        // it delegates destruction to us, to be safe.
//        cout.put('D'); cout.flush(); // indicate connection destruction
        delete cp;
    }

    unsigned int max_idle_time() override {
        return maxIdleTime_;
    }

private:
    // Number of connections currently in use
    int conns_in_use_;

    // Our connection parameters
    std::string db_, user_, password_, server_;
    int port_;

    int maxPoolSize_;

    unsigned int maxIdleTime_;
};