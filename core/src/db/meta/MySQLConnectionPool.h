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

#include <mysql++/mysql++.h>

#include <unistd.h>
#include <atomic>
#include <string>

#include "utils/Log.h"

namespace milvus {
namespace engine {
namespace meta {

class MySQLConnectionPool : public mysqlpp::ConnectionPool {
 public:
    // The object's only constructor
    MySQLConnectionPool(const std::string& dbName, const std::string& userName, const std::string& passWord,
                        const std::string& serverIp, int port = 0, int maxPoolSize = 8)
        : db_name_(dbName),
          user_(userName),
          password_(passWord),
          server_(serverIp),
          port_(port),
          max_pool_size_(maxPoolSize) {
    }

    // The destructor.  We _must_ call ConnectionPool::clear() here,
    // because our superclass can't do it for us.
    ~MySQLConnectionPool() override {
        clear();
    }

    mysqlpp::Connection*
    grab() override;

    // Other half of in-use conn count limit
    void
    release(const mysqlpp::Connection* pc) override;

    const std::string&
    db_name() const {
        return db_name_;
    }

 protected:
    // Superclass overrides
    mysqlpp::Connection*
    create() override;

    void
    destroy(mysqlpp::Connection* cp) override;

    unsigned int
    max_idle_time() override;

 private:
    // Number of connections currently in use
    std::atomic<int> conns_in_use_ = 0;

    // Our connection parameters
    std::string db_name_, user_, password_, server_;
    int port_;

    int max_pool_size_;

    unsigned int max_idle_time_ = 0;  // 10 seconds
};

}  // namespace meta
}  // namespace engine
}  // namespace milvus
