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
#include <boost/system/error_code.hpp>
#include <fstream>
#include <gtest/gtest.h>
#include <iostream>
#include <random>
#include <string>
#include <vector>
#include <yaml-cpp/yaml.h>

#include "storage/Event.h"
#include "storage/MinioChunkManager.h"
#include "storage/LocalChunkManager.h"
#include "storage/DiskFileManagerImpl.h"
#include "config/ConfigChunkManager.h"
#include "config/ConfigKnowhere.h"

using namespace std;
using namespace milvus;
using namespace milvus::storage;
using namespace boost::filesystem;
using namespace knowhere;

class DiskAnnFileManagerTest : public testing::Test {
 public:
    DiskAnnFileManagerTest() {
    }
    ~DiskAnnFileManagerTest() {
    }

    bool
    FindFile(const path& dir, const string& file_name, path& path_found) {
        const recursive_directory_iterator end;
        boost::system::error_code err;
        auto iter = recursive_directory_iterator(dir, err);
        while (iter != end) {
            try {
                if ((*iter).path().filename() == file_name) {
                    path_found = (*iter).path();
                    return true;
                }
                iter++;
            } catch (filesystem_error& e) {
            } catch (std::exception& e) {
                // ignore error
            }
        }
        return false;
    }

    string
    GetConfig() {
        char testPath[100];
        auto pwd = string(getcwd(testPath, sizeof(testPath)));
        path filepath;
        auto currentPath = path(pwd);
        while (!FindFile(currentPath, "milvus.yaml", filepath)) {
            currentPath = currentPath.append("../");
        }
        return filepath.string();
    }

    void
    InitRemoteChunkManager() {
        auto configPath = GetConfig();
        cout << configPath << endl;
        YAML::Node config;
        config = YAML::LoadFile(configPath);
        auto minioConfig = config["minio"];
        auto address = minioConfig["address"].as<string>();
        auto port = minioConfig["port"].as<string>();
        auto endpoint = address + ":" + port;
        auto accessKey = minioConfig["accessKeyID"].as<string>();
        auto accessValue = minioConfig["secretAccessKey"].as<string>();
        auto useSSL = minioConfig["useSSL"].as<bool>();
        auto bucketName = minioConfig["bucketName"].as<string>();

        ChunkMangerConfig::SetAddress(endpoint);
        ChunkMangerConfig::SetAccessKey(accessKey);
        ChunkMangerConfig::SetAccessValue(accessValue);
        ChunkMangerConfig::SetBucketName(bucketName);
        ChunkMangerConfig::SetUseSSL(useSSL);
    }

    void
    InitLocalChunkManager() {
        ChunkMangerConfig::SetLocalRootPath("/tmp/diskann");
        config::KnowhereSetIndexSliceSize(5);
    }

    virtual void
    SetUp() {
        InitLocalChunkManager();
        InitRemoteChunkManager();
    }
};

TEST_F(DiskAnnFileManagerTest, AddFilePositive) {
    auto& lcm = LocalChunkManager::GetInstance();
    auto& rcm = MinioChunkManager::GetInstance();

    string testBucketName = "test-diskann";
    rcm.SetBucketName(testBucketName);
    EXPECT_EQ(rcm.GetBucketName(), testBucketName);

    if (!rcm.BucketExists(testBucketName)) {
        rcm.CreateBucket(testBucketName);
    }

    std::string indexFilePath = "/tmp/diskann/index_files/1000/index";
    auto exist = lcm.Exist(indexFilePath);
    EXPECT_EQ(exist, false);
    uint64_t index_size = 1024;
    lcm.CreateFile(indexFilePath);
    std::vector<uint8_t> data(index_size);
    lcm.Write(indexFilePath, data.data(), index_size);

    // collection_id: 1, partition_id: 2, segment_id: 3
    // field_id: 100, index_build_id: 1000, index_version: 1
    FieldDataMeta filed_data_meta = {1, 2, 3, 100};
    IndexMeta index_meta = {3, 100, 1000, 1, "index"};

    int64_t slice_size = config::KnowhereGetIndexSliceSize() << 20;
    auto diskAnnFileManager = std::make_shared<DiskFileManagerImpl>(filed_data_meta, index_meta);
    diskAnnFileManager->AddFile(indexFilePath);

    // check result
    auto remotePrefix = diskAnnFileManager->GetRemoteIndexObjectPrefix();
    auto remoteIndexFiles = rcm.ListWithPrefix(remotePrefix);

    auto num_slice = index_size / slice_size;
    EXPECT_EQ(remoteIndexFiles.size(), index_size % slice_size == 0 ? num_slice : num_slice + 1);

    diskAnnFileManager->CacheIndexToDisk(remoteIndexFiles);
    auto fileSize1 = rcm.Size(remoteIndexFiles[0]);
    auto buf = std::unique_ptr<uint8_t[]>(new uint8_t[fileSize1]);
    rcm.Read(remoteIndexFiles[0], buf.get(), fileSize1);

    auto index = DeserializeFileData(buf.get(), fileSize1);
    auto payload = index->GetPayload();
    auto rows = payload->rows;
    auto rawData = payload->raw_data;

    EXPECT_EQ(rows, index_size);
    EXPECT_EQ(rawData[0], data[0]);
    EXPECT_EQ(rawData[4], data[4]);

    auto files = diskAnnFileManager->GetRemotePathsToFileSize();
    for (auto& value : files) {
        rcm.Remove(value.first);
    }
    rcm.DeleteBucket(testBucketName);
}
