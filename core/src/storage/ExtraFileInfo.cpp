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

#include <iostream>
#include <regex>
#include <utility>
#include <vector>

#include "crc32c/crc32c.h"
#include "storage/ExtraFileInfo.h"

const char* MAGIC = "Milvus";
const int64_t MAGIC_SIZE = 6;
const int64_t HEADER_SIZE = 4090;
const int64_t SUM_SIZE = 16;

bool
validate(std::string s) {
    std::regex test("[=;]+");
    return !std::regex_match(s.begin(), s.end(), test);
}

namespace milvus {
namespace storage {

bool
CheckMagic(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path) {
    if (!fs_ptr->reader_ptr_->Open(file_path.c_str())) {
        std::string err_msg = "Failed to open file: " + file_path + ", error: " + std::strerror(errno);
        LOG_ENGINE_ERROR_ << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }

    std::string magic;
    fs_ptr->reader_ptr_->Read(magic.data(), MAGIC_SIZE);
    fs_ptr->reader_ptr_->Close();

    return magic.compare(MAGIC);
}

void
WriteMagic(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path) {
    if (!fs_ptr->writer_ptr_->Open(file_path.c_str())) {
        std::string err_msg = "Failed to open file: " + file_path + ", error: " + std::strerror(errno);
        LOG_ENGINE_ERROR_ << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }
    fs_ptr->writer_ptr_->Write(MAGIC, MAGIC_SIZE);
    fs_ptr->writer_ptr_->Close();
}

std::unordered_map<std::string, std::string>
ReadHeaderValues(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path) {
    if (!fs_ptr->reader_ptr_->Open(file_path.c_str())) {
        std::string err_msg = "Failed to open file: " + file_path + ", error: " + std::strerror(errno);
        LOG_ENGINE_ERROR_ << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }
    fs_ptr->reader_ptr_->Seekg(MAGIC_SIZE);
    std::string header;
    header.resize(HEADER_SIZE);
    fs_ptr->reader_ptr_->Read(header.data(), HEADER_SIZE);

    auto result = std::unordered_map<std::string, std::string>();

    std::regex semicolon(";");
    std::vector<std::string> maps(std::sregex_token_iterator(header.begin(), header.end(), semicolon, -1),
                                  std::sregex_token_iterator());
    std::regex equal("=");
    for (auto& item : maps) {
        std::vector<std::string> pair(std::sregex_token_iterator(item.begin(), item.end(), equal, -1),
                                      std::sregex_token_iterator());
        if (pair.size() == 2) {
            result.insert(std::make_pair(pair[0], pair[1]));
        }
    }
    fs_ptr->reader_ptr_->Close();
    return result;
}

std::string
ReadHeaderValue(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, const std::string& key) {
    auto kv = ReadHeaderValues(fs_ptr, file_path);
    return kv.at(key);
}

std::uint32_t
CalculateSum(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, bool written) {
    if (!fs_ptr->reader_ptr_->Open(file_path.c_str())) {
        std::string err_msg = "Failed to open file: " + file_path + ", error: " + std::strerror(errno);
        LOG_ENGINE_ERROR_ << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }

    int size = fs_ptr->reader_ptr_->Length();
    if (written) {
        size -= SUM_SIZE;
    }
    std::string data;
    data.resize(size);
    fs_ptr->reader_ptr_->Read(data.data(), size);
    std::uint32_t result = crc32c::Crc32c(data.data(), size);
    fs_ptr->reader_ptr_->Close();
    return result;
}

void
WriteSum(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, uint32_t result, bool written) {
    if (!fs_ptr->writer_ptr_->InOpen(file_path.c_str())) {
        std::string err_msg = "Failed to open file: " + file_path + ", error: " + std::strerror(errno);
        LOG_ENGINE_ERROR_ << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }

    if (written) {
        fs_ptr->writer_ptr_->Seekp(-SUM_SIZE, std::ios_base::end);
    } else {
        fs_ptr->writer_ptr_->Seekp(0, std::ios_base::end);
    }

    fs_ptr->writer_ptr_->Write(&result, SUM_SIZE);
    fs_ptr->writer_ptr_->Close();
}

bool
CheckSum(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path) {
    uint32_t result = CalculateSum(fs_ptr, file_path, true);
    if (!fs_ptr->reader_ptr_->Open(file_path.c_str())) {
        std::string err_msg = "Failed to open file: " + file_path + ", error: " + std::strerror(errno);
        LOG_ENGINE_ERROR_ << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }
    fs_ptr->reader_ptr_->Seekg(-SUM_SIZE, std::ios_base::end);
    uint32_t record;
    fs_ptr->reader_ptr_->Read(&record, SUM_SIZE);

    fs_ptr->reader_ptr_->Close();

    return record == result;
}

bool
WriteHeaderValue(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, const std::string& key,
                 const std::string& value) {
    auto record = ReadHeaderValues(fs_ptr, file_path);
    record.insert(std::make_pair(key, value));
    WriteHeaderValues(fs_ptr, file_path, record);
    return true;
}

bool
WriteHeaderValues(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path,
                  const std::unordered_map<std::string, std::string>& maps) {
    if (!fs_ptr->writer_ptr_->InOpen(file_path.c_str())) {
        std::string err_msg = "Failed to open file: " + file_path + ", error: " + std::strerror(errno);
        LOG_ENGINE_ERROR_ << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }
    fs_ptr->writer_ptr_->Seekp(MAGIC_SIZE);

    std::string kv;
    for (auto& map : maps) {
        if (validate(map.first) && validate(map.second)) {
            kv.append(map.first + "=" + map.second + ";");
        } else {
            throw "Equal and semicolon are illegal character in header data";
        }
    }
    if (kv.size() > HEADER_SIZE) {
        throw "Exceeded the limit of header data size";
    }
    kv.resize(HEADER_SIZE, ' ');

    fs_ptr->writer_ptr_->Write(kv.data(), HEADER_SIZE);
    fs_ptr->writer_ptr_->Close();

    return true;
}
}  // namespace storage

}  // namespace milvus
