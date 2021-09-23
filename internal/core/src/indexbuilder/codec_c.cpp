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

#include <exceptions/EasyAssert.h>
#include "pb/index_cgo_msg.pb.h"
#include "indexbuilder/codec_c.h"
#include "indexbuilder/IndexWrapper.h"

CStatus
AssembleBinarySet(const char* original, int32_t size, CBinary* assembled) {
    auto status = CStatus();
    try {
        namespace indexcgo = milvus::proto::indexcgo;
        indexcgo::BinarySet blob_buffer;
        auto data = std::string(original, size);
        auto ok = blob_buffer.ParseFromString(data);
        Assert(ok);

        milvus::knowhere::BinarySet binarySet;
        for (auto i = 0; i < blob_buffer.datas_size(); i++) {
            const auto& binary = blob_buffer.datas(i);
            auto deleter = [&](uint8_t*) {};  // avoid repeated deconstruction
            auto bptr = std::make_shared<milvus::knowhere::Binary>();
            bptr->data = std::shared_ptr<uint8_t[]>((uint8_t*)binary.value().c_str(), deleter);
            bptr->size = binary.value().length();
            binarySet.Append(binary.key(), bptr);
        }

        auto slice_meta = binarySet.Erase("SLICE_META");
        milvus::json meta_data =
            milvus::json::parse(std::string(reinterpret_cast<char*>(slice_meta->data.get()), slice_meta->size));

        indexcgo::BinarySet ret;
        auto b = ret.add_datas();
        b->set_key("SLICE_META");
        b->set_value(meta_data.dump());

        std::string serialized_data;
        ok = ret.SerializeToString(&serialized_data);
        Assert(ok);

        auto binary = std::make_unique<milvus::indexbuilder::IndexWrapper::Binary>();
        binary->data.resize(serialized_data.length());
        memcpy(binary->data.data(), serialized_data.c_str(), serialized_data.length());

        *assembled = binary.release();

        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }

    return status;
}
