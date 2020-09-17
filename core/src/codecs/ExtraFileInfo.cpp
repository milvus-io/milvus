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

#include <regex>
#include <utility>
#include <vector>

#include "codecs/ExtraFileInfo.h"
#include "crc32c/crc32c.h"
#include "utils/Log.h"

const char* MAGIC = "Milvus";
const int64_t MAGIC_SIZE = 6;
const int64_t HEADER_SIZE = 4090;
const int64_t SUM_SIZE = sizeof(uint32_t);

bool
validate(std::string s) {
    std::regex test("[=;]+");
    return !std::regex_match(s.begin(), s.end(), test);
}

namespace milvus {
namespace codec {

void
WriteMagic(const storage::FSHandlerPtr& fs_ptr) {
    fs_ptr->writer_ptr_->Write(MAGIC, MAGIC_SIZE);
}

bool
CheckMagic(const storage::FSHandlerPtr& fs_ptr) {
    std::vector<char> magic;
    magic.resize(MAGIC_SIZE);
    fs_ptr->reader_ptr_->Read(magic.data(), MAGIC_SIZE);

    if (strncmp(magic.data(), MAGIC, MAGIC_SIZE)) {
        LOG_ENGINE_ERROR_ << "Check magic failed. Record is " << magic.data() << " while magic is Milvus";
        fs_ptr->reader_ptr_->Close();
        return false;
    }
    return true;
}

std::unordered_map<std::string, std::string>
ReadHeaderValues(const storage::FSHandlerPtr& fs_ptr) {
    fs_ptr->reader_ptr_->Seekg(MAGIC_SIZE);

    std::vector<char> data;
    data.resize(HEADER_SIZE);
    fs_ptr->reader_ptr_->Read(data.data(), HEADER_SIZE);

    std::string header(data.begin(), data.end());

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

    return result;
}

std::string
ReadHeaderValue(const storage::FSHandlerPtr& fs_ptr, const std::string& key) {
    auto kv = ReadHeaderValues(fs_ptr);
    return kv.at(key);
}

std::uint32_t
CalculateSum(const storage::FSHandlerPtr& fs_ptr, bool written) {
    auto size = fs_ptr->reader_ptr_->Length();
    if (written) {
        size -= SUM_SIZE;
    }
    fs_ptr->reader_ptr_->Seekg(0);
    std::vector<char> data;
    data.resize(size);
    fs_ptr->reader_ptr_->Read(data.data(), size);
    std::uint32_t result = crc32c::Crc32c(data.data(), size);

    return result;
}

std::uint32_t
CalculateSum(char* data, size_t size) {
    std::uint32_t result = crc32c::Crc32c(data, size);
    return result;
}

void
WriteSum(const storage::FSHandlerPtr& fs_ptr, std::string header, char* data, size_t data_size) {
    std::vector<char> total;
    total.resize(MAGIC_SIZE + HEADER_SIZE + data_size);
    memcpy(total.data(), MAGIC, MAGIC_SIZE);
    memcpy(total.data() + MAGIC_SIZE, header.data(), HEADER_SIZE);
    memcpy(total.data() + MAGIC_SIZE + HEADER_SIZE, data, data_size);
    auto result_sum = CalculateSum(total.data(), MAGIC_SIZE + HEADER_SIZE + data_size);

    fs_ptr->writer_ptr_->Write(&result_sum, SUM_SIZE);
}

bool
CheckSum(const storage::FSHandlerPtr& fs_ptr) {
    auto length = fs_ptr->reader_ptr_->Length();
    fs_ptr->reader_ptr_->Seekg(length - SUM_SIZE);
    uint32_t record;
    fs_ptr->reader_ptr_->Read(&record, SUM_SIZE);

    uint32_t result = CalculateSum(fs_ptr, true);
    if (record != result) {
        LOG_ENGINE_ERROR_ << "CheckSum failed. Record is " << record << ". Calculate sum is " << result;
        fs_ptr->reader_ptr_->Close();
        return false;
    }
    return true;
}

bool
WriteHeaderValues(const storage::FSHandlerPtr& fs_ptr, const std::string& kv) {
    fs_ptr->writer_ptr_->Write(kv.data(), HEADER_SIZE);
    return true;
}

std::string
HeaderWrapper(const std::unordered_map<std::string, std::string>& maps) {
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

    return kv;
}

}  // namespace codec

}  // namespace milvus
