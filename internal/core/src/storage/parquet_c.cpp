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

#include <mutex>

#include "storage/parquet_c.h"
#include "storage/PayloadReader.h"
#include "storage/PayloadWriter.h"
#include "storage/FieldData.h"
#include "common/CGoHelper.h"
#include "storage/Util.h"

using Payload = milvus::storage::Payload;
using PayloadWriter = milvus::storage::PayloadWriter;
using PayloadReader = milvus::storage::PayloadReader;

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
    auto raw_data_info = Payload{milvus::DataType::BOOL,
                                 reinterpret_cast<const uint8_t*>(values),
                                 length};
    return AddValuesToPayload(payloadWriter, raw_data_info);
}

extern "C" CStatus
AddInt8ToPayload(CPayloadWriter payloadWriter, int8_t* values, int length) {
    auto raw_data_info = Payload{milvus::DataType::INT8,
                                 reinterpret_cast<const uint8_t*>(values),
                                 length};
    return AddValuesToPayload(payloadWriter, raw_data_info);
}

extern "C" CStatus
AddInt16ToPayload(CPayloadWriter payloadWriter, int16_t* values, int length) {
    auto raw_data_info = Payload{milvus::DataType::INT16,
                                 reinterpret_cast<const uint8_t*>(values),
                                 length};
    return AddValuesToPayload(payloadWriter, raw_data_info);
}

extern "C" CStatus
AddInt32ToPayload(CPayloadWriter payloadWriter, int32_t* values, int length) {
    auto raw_data_info = Payload{milvus::DataType::INT32,
                                 reinterpret_cast<const uint8_t*>(values),
                                 length};
    return AddValuesToPayload(payloadWriter, raw_data_info);
}

extern "C" CStatus
AddInt64ToPayload(CPayloadWriter payloadWriter, int64_t* values, int length) {
    auto raw_data_info = Payload{milvus::DataType::INT64,
                                 reinterpret_cast<const uint8_t*>(values),
                                 length};
    return AddValuesToPayload(payloadWriter, raw_data_info);
}

extern "C" CStatus
AddFloatToPayload(CPayloadWriter payloadWriter, float* values, int length) {
    auto raw_data_info = Payload{milvus::DataType::FLOAT,
                                 reinterpret_cast<const uint8_t*>(values),
                                 length};
    return AddValuesToPayload(payloadWriter, raw_data_info);
}

extern "C" CStatus
AddDoubleToPayload(CPayloadWriter payloadWriter, double* values, int length) {
    auto raw_data_info = Payload{milvus::DataType::DOUBLE,
                                 reinterpret_cast<const uint8_t*>(values),
                                 length};
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
AddOneArrayToPayload(CPayloadWriter payloadWriter, uint8_t* data, int length) {
    try {
        auto p = reinterpret_cast<PayloadWriter*>(payloadWriter);
        p->add_one_binary_payload(data, length);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

extern "C" CStatus
AddOneJSONToPayload(CPayloadWriter payloadWriter, uint8_t* data, int length) {
    try {
        auto p = reinterpret_cast<PayloadWriter*>(payloadWriter);
        p->add_one_binary_payload(data, length);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

extern "C" CStatus
AddBinaryVectorToPayload(CPayloadWriter payloadWriter,
                         uint8_t* values,
                         int dimension,
                         int length) {
    try {
        auto p = reinterpret_cast<PayloadWriter*>(payloadWriter);
        auto raw_data_info =
            Payload{milvus::DataType::VECTOR_BINARY, values, length, dimension};
        p->add_payload(raw_data_info);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

extern "C" CStatus
AddFloatVectorToPayload(CPayloadWriter payloadWriter,
                        float* values,
                        int dimension,
                        int length) {
    try {
        auto p = reinterpret_cast<PayloadWriter*>(payloadWriter);
        auto raw_data_info = Payload{milvus::DataType::VECTOR_FLOAT,
                                     reinterpret_cast<const uint8_t*>(values),
                                     length,
                                     dimension};
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
    if (p != nullptr) {
        delete p;
        milvus::storage::ReleaseArrowUnused();
    }
}

extern "C" CStatus
NewPayloadReader(int columnType,
                 uint8_t* buffer,
                 int64_t buf_size,
                 CPayloadReader* c_reader) {
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
            return milvus::FailureCStatus(UnexpectedError,
                                          "unsupported data type");
        }
    }

    try {
        auto p = std::make_unique<PayloadReader>(buffer, buf_size, column_type);
        *c_reader = (CPayloadReader)(p.release());
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

extern "C" CStatus
GetBoolFromPayload(CPayloadReader payloadReader, int idx, bool* value) {
    try {
        auto p = reinterpret_cast<PayloadReader*>(payloadReader);
        auto field_data = p->get_field_data();
        *value = *reinterpret_cast<const bool*>(field_data->RawValue(idx));
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

extern "C" CStatus
GetInt8FromPayload(CPayloadReader payloadReader, int8_t** values, int* length) {
    try {
        auto p = reinterpret_cast<PayloadReader*>(payloadReader);
        auto field_data = p->get_field_data();
        *length = field_data->get_num_rows();
        *values =
            reinterpret_cast<int8_t*>(const_cast<void*>(field_data->Data()));
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

extern "C" CStatus
GetInt16FromPayload(CPayloadReader payloadReader,
                    int16_t** values,
                    int* length) {
    try {
        auto p = reinterpret_cast<PayloadReader*>(payloadReader);
        auto field_data = p->get_field_data();
        *length = field_data->get_num_rows();
        *values =
            reinterpret_cast<int16_t*>(const_cast<void*>(field_data->Data()));
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

extern "C" CStatus
GetInt32FromPayload(CPayloadReader payloadReader,
                    int32_t** values,
                    int* length) {
    try {
        auto p = reinterpret_cast<PayloadReader*>(payloadReader);
        auto field_data = p->get_field_data();
        *length = field_data->get_num_rows();
        *values =
            reinterpret_cast<int32_t*>(const_cast<void*>(field_data->Data()));
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

extern "C" CStatus
GetInt64FromPayload(CPayloadReader payloadReader,
                    int64_t** values,
                    int* length) {
    try {
        auto p = reinterpret_cast<PayloadReader*>(payloadReader);
        auto field_data = p->get_field_data();
        *length = field_data->get_num_rows();
        *values =
            reinterpret_cast<int64_t*>(const_cast<void*>(field_data->Data()));
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

extern "C" CStatus
GetFloatFromPayload(CPayloadReader payloadReader, float** values, int* length) {
    try {
        auto p = reinterpret_cast<PayloadReader*>(payloadReader);
        auto field_data = p->get_field_data();
        *length = field_data->get_num_rows();
        *values =
            reinterpret_cast<float*>(const_cast<void*>(field_data->Data()));
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

extern "C" CStatus
GetDoubleFromPayload(CPayloadReader payloadReader,
                     double** values,
                     int* length) {
    try {
        auto p = reinterpret_cast<PayloadReader*>(payloadReader);
        auto field_data = p->get_field_data();
        *length = field_data->get_num_rows();
        *values =
            reinterpret_cast<double*>(const_cast<void*>(field_data->Data()));
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

extern "C" CStatus
GetOneStringFromPayload(CPayloadReader payloadReader,
                        int idx,
                        char** cstr,
                        int* str_size) {
    try {
        auto p = reinterpret_cast<PayloadReader*>(payloadReader);
        auto field_data = p->get_field_data();
        auto str = const_cast<void*>(field_data->RawValue(idx));
        *cstr = (char*)(*static_cast<std::string*>(str)).c_str();
        *str_size = field_data->Size(idx);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

extern "C" CStatus
GetBinaryVectorFromPayload(CPayloadReader payloadReader,
                           uint8_t** values,
                           int* dimension,
                           int* length) {
    try {
        auto p = reinterpret_cast<PayloadReader*>(payloadReader);
        auto field_data = p->get_field_data();
        *values = (uint8_t*)field_data->Data();
        *dimension = field_data->get_dim();
        *length = field_data->get_num_rows();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

extern "C" CStatus
GetFloatVectorFromPayload(CPayloadReader payloadReader,
                          float** values,
                          int* dimension,
                          int* length) {
    try {
        auto p = reinterpret_cast<PayloadReader*>(payloadReader);
        auto field_data = p->get_field_data();
        *values = (float*)field_data->Data();
        *dimension = field_data->get_dim();
        *length = field_data->get_num_rows();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

extern "C" int
GetPayloadLengthFromReader(CPayloadReader payloadReader) {
    auto p = reinterpret_cast<PayloadReader*>(payloadReader);
    auto field_data = p->get_field_data();
    return field_data->get_num_rows();
}

extern "C" CStatus
ReleasePayloadReader(CPayloadReader payloadReader) {
    try {
        AssertInfo(payloadReader != nullptr,
                   "released payloadReader should not be null pointer");
        auto p = reinterpret_cast<PayloadReader*>(payloadReader);
        delete (p);

        milvus::storage::ReleaseArrowUnused();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}
