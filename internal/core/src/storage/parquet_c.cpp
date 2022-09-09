// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "storage/parquet_c.h"
#include "storage/PayloadReader.h"
#include "storage/PayloadWriter.h"
#include "common/CGoHelper.h"

using Payload = milvus::storage::Payload;
using PayloadWriter = milvus::storage::PayloadWriter;
using PayloadReader = milvus::storage::PayloadReader;

static const char*
ErrorMsg(const std::string& msg) {
    if (msg.empty())
        return nullptr;
    auto ret = (char*)malloc(msg.size() + 1);
    std::memcpy(ret, msg.c_str(), msg.size());
    ret[msg.size()] = '\0';
    return ret;
}

extern "C" CPayloadWriter
NewPayloadWriter(int columnType) {
    auto data_type = static_cast<milvus::DataType>(columnType);
    auto p = std::make_unique<PayloadWriter>(data_type);

    return reinterpret_cast<CPayloadWriter>(p.release());
}

CPayloadWriter
NewVectorPayloadWriter(int columnType, int dim) {
    auto data_type = static_cast<milvus::DataType>(columnType);
    auto p = std::make_unique<PayloadWriter>(data_type, dim);

    return reinterpret_cast<CPayloadWriter>(p.release());
}

CStatus
AddValuesToPayload(CPayloadWriter payloadWriter, const Payload& info) {
    try {
        auto p = reinterpret_cast<PayloadWriter*>(payloadWriter);
        p->add_payload(info);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

extern "C" CStatus
AddBooleanToPayload(CPayloadWriter payloadWriter, bool* values, int length) {
    auto raw_data_info = Payload{milvus::DataType::BOOL, reinterpret_cast<const uint8_t*>(values), length};
    return AddValuesToPayload(payloadWriter, raw_data_info);
}

extern "C" CStatus
AddInt8ToPayload(CPayloadWriter payloadWriter, int8_t* values, int length) {
    auto raw_data_info = Payload{milvus::DataType::INT8, reinterpret_cast<const uint8_t*>(values), length};
    return AddValuesToPayload(payloadWriter, raw_data_info);
}

extern "C" CStatus
AddInt16ToPayload(CPayloadWriter payloadWriter, int16_t* values, int length) {
    auto raw_data_info = Payload{milvus::DataType::INT16, reinterpret_cast<const uint8_t*>(values), length};
    return AddValuesToPayload(payloadWriter, raw_data_info);
}

extern "C" CStatus
AddInt32ToPayload(CPayloadWriter payloadWriter, int32_t* values, int length) {
    auto raw_data_info = Payload{milvus::DataType::INT32, reinterpret_cast<const uint8_t*>(values), length};
    return AddValuesToPayload(payloadWriter, raw_data_info);
}

extern "C" CStatus
AddInt64ToPayload(CPayloadWriter payloadWriter, int64_t* values, int length) {
    auto raw_data_info = Payload{milvus::DataType::INT64, reinterpret_cast<const uint8_t*>(values), length};
    return AddValuesToPayload(payloadWriter, raw_data_info);
}

extern "C" CStatus
AddFloatToPayload(CPayloadWriter payloadWriter, float* values, int length) {
    auto raw_data_info = Payload{milvus::DataType::FLOAT, reinterpret_cast<const uint8_t*>(values), length};
    return AddValuesToPayload(payloadWriter, raw_data_info);
}

extern "C" CStatus
AddDoubleToPayload(CPayloadWriter payloadWriter, double* values, int length) {
    auto raw_data_info = Payload{milvus::DataType::DOUBLE, reinterpret_cast<const uint8_t*>(values), length};
    return AddValuesToPayload(payloadWriter, raw_data_info);
}

extern "C" CStatus
AddOneStringToPayload(CPayloadWriter payloadWriter, char* cstr, int str_size) {
    try {
        auto p = reinterpret_cast<PayloadWriter*>(payloadWriter);
        p->add_one_string_payload(cstr, str_size);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

extern "C" CStatus
AddBinaryVectorToPayload(CPayloadWriter payloadWriter, uint8_t* values, int dimension, int length) {
    try {
        auto p = reinterpret_cast<PayloadWriter*>(payloadWriter);
        auto raw_data_info = Payload{milvus::DataType::VECTOR_BINARY, values, length, dimension};
        p->add_payload(raw_data_info);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

extern "C" CStatus
AddFloatVectorToPayload(CPayloadWriter payloadWriter, float* values, int dimension, int length) {
    try {
        auto p = reinterpret_cast<PayloadWriter*>(payloadWriter);
        auto raw_data_info =
            Payload{milvus::DataType::VECTOR_FLOAT, reinterpret_cast<const uint8_t*>(values), length, dimension};
        p->add_payload(raw_data_info);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

extern "C" CStatus
FinishPayloadWriter(CPayloadWriter payloadWriter) {
    try {
        auto p = reinterpret_cast<PayloadWriter*>(payloadWriter);
        p->finish();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CBuffer
GetPayloadBufferFromWriter(CPayloadWriter payloadWriter) {
    CBuffer buf;

    auto p = reinterpret_cast<PayloadWriter*>(payloadWriter);
    if (!p->has_finished()) {
        buf.data = nullptr;
        buf.length = 0;
        return buf;
    }
    auto& output = p->get_payload_buffer();
    buf.length = static_cast<int>(output.size());
    buf.data = (char*)(output.data());
    return buf;
}

int
GetPayloadLengthFromWriter(CPayloadWriter payloadWriter) {
    auto p = reinterpret_cast<PayloadWriter*>(payloadWriter);
    return p->get_payload_length();
}

extern "C" void
ReleasePayloadWriter(CPayloadWriter handler) {
    auto p = reinterpret_cast<PayloadWriter*>(handler);
    if (p != nullptr)
        delete p;
    arrow::default_memory_pool()->ReleaseUnused();
}

extern "C" CPayloadReader
NewPayloadReader(int columnType, uint8_t* buffer, int64_t buf_size) {
    auto column_type = static_cast<milvus::DataType>(columnType);
    switch (column_type) {
        case milvus::DataType::BOOL:
        case milvus::DataType::INT8:
        case milvus::DataType::INT16:
        case milvus::DataType::INT32:
        case milvus::DataType::INT64:
        case milvus::DataType::FLOAT:
        case milvus::DataType::DOUBLE:
        case milvus::DataType::STRING:
        case milvus::DataType::VARCHAR:
        case milvus::DataType::VECTOR_BINARY:
        case milvus::DataType::VECTOR_FLOAT: {
            break;
        }
        default: {
            return nullptr;
        }
    }

    auto p = std::make_unique<PayloadReader>(buffer, buf_size, column_type);
    return reinterpret_cast<CPayloadReader>(p.release());
}

extern "C" CStatus
GetBoolFromPayload(CPayloadReader payloadReader, int idx, bool* value) {
    try {
        auto p = reinterpret_cast<PayloadReader*>(payloadReader);
        *value = p->get_bool_payload(idx);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

extern "C" CStatus
GetInt8FromPayload(CPayloadReader payloadReader, int8_t** values, int* length) {
    try {
        auto p = reinterpret_cast<PayloadReader*>(payloadReader);
        auto ret = p->get_payload();
        auto raw_data = const_cast<uint8_t*>(ret->raw_data);
        *values = reinterpret_cast<int8_t*>(raw_data);
        *length = ret->rows;
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

extern "C" CStatus
GetInt16FromPayload(CPayloadReader payloadReader, int16_t** values, int* length) {
    try {
        auto p = reinterpret_cast<PayloadReader*>(payloadReader);
        auto ret = p->get_payload();
        auto raw_data = const_cast<uint8_t*>(ret->raw_data);
        *values = reinterpret_cast<int16_t*>(raw_data);
        *length = ret->rows;
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

extern "C" CStatus
GetInt32FromPayload(CPayloadReader payloadReader, int32_t** values, int* length) {
    try {
        auto p = reinterpret_cast<PayloadReader*>(payloadReader);
        auto ret = p->get_payload();
        auto raw_data = const_cast<uint8_t*>(ret->raw_data);
        *values = reinterpret_cast<int32_t*>(raw_data);
        *length = ret->rows;
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

extern "C" CStatus
GetInt64FromPayload(CPayloadReader payloadReader, int64_t** values, int* length) {
    try {
        auto p = reinterpret_cast<PayloadReader*>(payloadReader);
        auto ret = p->get_payload();
        auto raw_data = const_cast<uint8_t*>(ret->raw_data);
        *values = reinterpret_cast<int64_t*>(raw_data);
        *length = ret->rows;
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

extern "C" CStatus
GetFloatFromPayload(CPayloadReader payloadReader, float** values, int* length) {
    try {
        auto p = reinterpret_cast<PayloadReader*>(payloadReader);
        auto ret = p->get_payload();
        auto raw_data = const_cast<uint8_t*>(ret->raw_data);
        *values = reinterpret_cast<float*>(raw_data);
        *length = ret->rows;
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

extern "C" CStatus
GetDoubleFromPayload(CPayloadReader payloadReader, double** values, int* length) {
    try {
        auto p = reinterpret_cast<PayloadReader*>(payloadReader);
        auto ret = p->get_payload();
        auto raw_data = const_cast<uint8_t*>(ret->raw_data);
        *values = reinterpret_cast<double*>(raw_data);
        *length = ret->rows;
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

extern "C" CStatus
GetOneStringFromPayload(CPayloadReader payloadReader, int idx, char** cstr, int* str_size) {
    try {
        auto p = reinterpret_cast<PayloadReader*>(payloadReader);
        p->get_one_string_Payload(idx, cstr, str_size);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

extern "C" CStatus
GetBinaryVectorFromPayload(CPayloadReader payloadReader, uint8_t** values, int* dimension, int* length) {
    try {
        auto p = reinterpret_cast<PayloadReader*>(payloadReader);
        auto ret = p->get_payload();
        *values = const_cast<uint8_t*>(ret->raw_data);
        *length = ret->rows;
        *dimension = ret->dimension.value();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

extern "C" CStatus
GetFloatVectorFromPayload(CPayloadReader payloadReader, float** values, int* dimension, int* length) {
    try {
        auto p = reinterpret_cast<PayloadReader*>(payloadReader);
        auto ret = p->get_payload();
        auto raw_data = const_cast<uint8_t*>(ret->raw_data);
        *values = reinterpret_cast<float*>(raw_data);
        *length = ret->rows;
        *dimension = ret->dimension.value();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

extern "C" int
GetPayloadLengthFromReader(CPayloadReader payloadReader) {
    auto p = reinterpret_cast<PayloadReader*>(payloadReader);
    return p->get_payload_length();
}

extern "C" void
ReleasePayloadReader(CPayloadReader payloadReader) {
    auto p = reinterpret_cast<PayloadReader*>(payloadReader);
    delete (p);
    arrow::default_memory_pool()->ReleaseUnused();
}
