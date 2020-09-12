// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

<<<<<<< HEAD:core/src/db/meta/backend/MySQLConnectionPool.cpp
#include "db/meta/backend/MySQLConnectionPool.h"

#include <thread>

#include <fiu/fiu-local.h>

#include "utils/Log.h"

=======
#include "db/meta/MySQLConnectionPool.h"
#include <fiu-local.h>
#include <thread>

>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda:core/src/db/meta/MySQLConnectionPool.cpp
namespace milvus::engine::meta {

// Do a simple form of in-use connection limiting: wait to return
// a connection until there are a reasonably low number in use
// already.  Can't do this in create() because we're interested in
// connections actually in use, not those created.  Also note that
// we keep our own count; ConnectionPool::size() isn't the same!
mysqlpp::Connection*
MySQLConnectionPool::grab() {
<<<<<<< HEAD:core/src/db/meta/backend/MySQLConnectionPool.cpp
    {
        std::unique_lock<std::mutex> lock(mutex_);
        full_.wait(lock, [this] { return conns_in_use_ < max_pool_size_; });
        ++conns_in_use_;
=======
    while (conns_in_use_ > max_pool_size_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda:core/src/db/meta/MySQLConnectionPool.cpp
    }
    full_.notify_one();
    return mysqlpp::ConnectionPool::grab();
}

mysqlpp::Connection*
MySQLConnectionPool::safe_grab() {
    {
        std::unique_lock<std::mutex> lock(mutex_);
        full_.wait(lock, [this] { return conns_in_use_ < max_pool_size_; });
        ++conns_in_use_;
    }
    full_.notify_one();
    return mysqlpp::ConnectionPool::safe_grab();
}

// Other half of in-use conn count limit
void
MySQLConnectionPool::release(const mysqlpp::Connection* pc) {
    mysqlpp::ConnectionPool::release(pc);

    std::lock_guard<std::mutex> lock(mutex_);
    if (conns_in_use_ <= 0) {
        LOG_ENGINE_WARNING_ << "MySQLConnetionPool::release: conns_in_use_ is less than zero.  conns_in_use_ = "
                            << conns_in_use_;
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

// Superclass overrides
mysqlpp::Connection*
MySQLConnectionPool::create() {
    try {
        fiu_do_on("MySQLConnectionPool.create.throw_exception", throw mysqlpp::ConnectionFailed());

        // Create connection using the parameters we were passed upon
        // creation.
        auto conn = new mysqlpp::Connection();
        conn->set_option(new mysqlpp::ReconnectOption(true));
        conn->set_option(new mysqlpp::ConnectTimeoutOption(5));
        conn->connect(db_name_.empty() ? nullptr : db_name_.c_str(), server_.empty() ? nullptr : server_.c_str(),
                      user_.empty() ? nullptr : user_.c_str(), password_.empty() ? nullptr : password_.c_str(), port_);
        return conn;
    } catch (const mysqlpp::ConnectionFailed& er) {
        LOG_ENGINE_ERROR_ << "Failed to connect to database server"
                          << ": " << er.what();
        return nullptr;
    }
}

void
MySQLConnectionPool::destroy(mysqlpp::Connection* cp) {
    // Our superclass can't know how we created the Connection, so
    // it delegates destruction to us, to be safe.
    delete cp;
}

unsigned int
MySQLConnectionPool::max_idle_time() {
    return max_idle_time_;
}

}  // namespace milvus::engine::meta
