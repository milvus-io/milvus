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

#include <boost/filesystem.hpp>
#include <iostream>
#include <string>
#include <thread>

#include "db/DBFactory.h"
#include "metrics/utils.h"

INITIALIZE_EASYLOGGINGPP

namespace {

class DBTestEnvironment : public ::testing::Environment {
 public:
    explicit DBTestEnvironment(const std::string& uri) : uri_(uri) {
    }

    std::string
    getURI() const {
        return uri_;
    }

    void
    SetUp() override {
        getURI();
    }

 private:
    std::string uri_;
};

DBTestEnvironment* test_env = nullptr;

}  // namespace

void
MetricTest::InitLog() {
    el::Configurations defaultConf;
    defaultConf.setToDefault();
    defaultConf.set(el::Level::Debug, el::ConfigurationType::Format, "[%thread-%datetime-%level]: %msg (%fbase:%line)");
    el::Loggers::reconfigureLogger("default", defaultConf);
}

milvus::engine::DBOptions
MetricTest::GetOptions() {
    auto options = milvus::engine::DBFactory::BuildOption();
    options.meta_.path_ = "/tmp/milvus_test";
    options.meta_.backend_uri_ = "sqlite://:@:/";
    return options;
}

void
MetricTest::SetUp() {
    boost::filesystem::remove_all("/tmp/milvus_test");
    InitLog();
    auto options = GetOptions();
    db_ = milvus::engine::DBFactory::Build(options);
}

void
MetricTest::TearDown() {
    db_->Stop();
    boost::filesystem::remove_all("/tmp/milvus_test");
}

int
main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);

    std::string uri;
    if (argc > 1) {
        uri = argv[1];
    }
    test_env = new DBTestEnvironment(uri);
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
