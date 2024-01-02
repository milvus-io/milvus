// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <boost/filesystem.hpp>
#include <string>

namespace milvus::test {
struct TmpPath {
    TmpPath() {
        temp_path_ = boost::filesystem::temp_directory_path() /
                     boost::filesystem::unique_path();
        boost::filesystem::create_directory(temp_path_);
    }
    ~TmpPath() {
        boost::filesystem::remove_all(temp_path_);
    }

    auto
    get() {
        return temp_path_;
    }

 private:
    boost::filesystem::path temp_path_;
};

}  // namespace milvus::test
