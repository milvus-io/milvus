// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "wrapper/VecIndex.h"
#include "VecImpl.h"
#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/IndexIDMAP.h"
#include "knowhere/index/vector_index/IndexIVF.h"
#include "knowhere/index/vector_index/IndexIVFPQ.h"
#include "knowhere/index/vector_index/IndexIVFSQ.h"
#include "knowhere/index/vector_index/IndexNSG.h"
//#include "knowhere/index/vector_index/IndexSPTAG.h"
#include "server/Config.h"
#include "storage/file/FileIOReader.h"
#include "storage/file/FileIOWriter.h"
#include "storage/s3/S3IOReader.h"
#include "storage/s3/S3IOWriter.h"
#include "utils/Exception.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#ifdef MILVUS_GPU_VERSION
#include <cuda.h>
#include "knowhere/index/vector_index/IndexGPUIDMAP.h"
#include "knowhere/index/vector_index/IndexGPUIVF.h"
#include "knowhere/index/vector_index/IndexGPUIVFPQ.h"
#include "knowhere/index/vector_index/IndexGPUIVFSQ.h"
#include "knowhere/index/vector_index/IndexIVFSQHybrid.h"
#include "wrapper/gpu/GPUVecImpl.h"
#endif

namespace milvus {
namespace engine {

int64_t
VecIndex::Size() {
    if (size_ != 0) {
        return size_;
    }
    return Count() * Dimension() * sizeof(float);
}

void
VecIndex::set_size(int64_t size) {
    size_ = size;
}

VecIndexPtr
GetVecIndexFactory(const IndexType& type, const Config& cfg) {
    std::shared_ptr<knowhere::VectorIndex> index;
    auto gpu_device = -1;  // TODO(linxj): remove hardcode here
    switch (type) {
        case IndexType::FAISS_IDMAP: {
            index = std::make_shared<knowhere::IDMAP>();
            return std::make_shared<BFIndex>(index);
        }
        case IndexType::FAISS_IVFFLAT_CPU: {
            index = std::make_shared<knowhere::IVF>();
            break;
        }
        case IndexType::FAISS_IVFPQ_CPU: {
            index = std::make_shared<knowhere::IVFPQ>();
            break;
        }
//        case IndexType::SPTAG_KDT_RNT_CPU: {
//            index = std::make_shared<knowhere::CPUSPTAGRNG>("KDT");
//            break;
//        }
//        case IndexType::SPTAG_BKT_RNT_CPU: {
//            index = std::make_shared<knowhere::CPUSPTAGRNG>("BKT");
//            break;
//        }
        case IndexType::FAISS_IVFSQ8_CPU: {
            index = std::make_shared<knowhere::IVFSQ>();
            break;
        }

#ifdef MILVUS_GPU_VERSION
        case IndexType::FAISS_IVFFLAT_GPU: {
            index = std::make_shared<knowhere::GPUIVF>(gpu_device);
            break;
        }
        case IndexType::FAISS_IVFFLAT_MIX: {
            index = std::make_shared<knowhere::GPUIVF>(gpu_device);
            return std::make_shared<IVFMixIndex>(index, IndexType::FAISS_IVFFLAT_MIX);
        }
        case IndexType::FAISS_IVFPQ_GPU: {
            index = std::make_shared<knowhere::GPUIVFPQ>(gpu_device);
            break;
        }
        case IndexType::FAISS_IVFPQ_MIX: {
            index = std::make_shared<knowhere::GPUIVFPQ>(gpu_device);
            return std::make_shared<IVFMixIndex>(index, IndexType::FAISS_IVFPQ_MIX);
        }
        case IndexType::FAISS_IVFSQ8_MIX: {
            index = std::make_shared<knowhere::GPUIVFSQ>(gpu_device);
            return std::make_shared<IVFMixIndex>(index, IndexType::FAISS_IVFSQ8_MIX);
        }
        case IndexType::FAISS_IVFSQ8_GPU: {
            index = std::make_shared<knowhere::GPUIVFSQ>(gpu_device);
            break;
        }

#endif
#ifdef CUSTOMIZATION
#ifdef MILVUS_GPU_VERSION
        case IndexType::FAISS_IVFSQ8_HYBRID: {
            server::Config& config = server::Config::GetInstance();
            bool gpu_resource_enable = true;
            config.GetGpuResourceConfigEnable(gpu_resource_enable);
            if (gpu_resource_enable) {
                index = std::make_shared<knowhere::IVFSQHybrid>(gpu_device);
                return std::make_shared<IVFHybridIndex>(index, IndexType::FAISS_IVFSQ8_HYBRID);
            } else {
                throw Exception(DB_ERROR, "No GPU resources for IndexType::FAISS_IVFSQ8_HYBRID");
            }
        }
#endif
#endif
        case IndexType::NSG_MIX: {  // TODO(linxj): bug.
            index = std::make_shared<knowhere::NSG>(gpu_device);
            break;
        }
        default: { return nullptr; }
    }
    return std::make_shared<VecIndexImpl>(index, type);
}

VecIndexPtr
LoadVecIndex(const IndexType& index_type, const knowhere::BinarySet& index_binary, int64_t size) {
    auto index = GetVecIndexFactory(index_type);
    if (index == nullptr)
        return nullptr;
    // else
    index->Load(index_binary);
    index->set_size(size);
    return index;
}

VecIndexPtr
read_index(const std::string& location) {
    TimeRecorder recorder("read_index");
    knowhere::BinarySet load_data_list;

    bool minio_enable = false;
    server::Config& config = server::Config::GetInstance();
    config.GetStorageConfigMinioEnable(minio_enable);

    std::shared_ptr<storage::IOReader> reader_ptr;
    if (minio_enable) {
        reader_ptr = std::make_shared<storage::S3IOReader>(location);
    } else {
        reader_ptr = std::make_shared<storage::FileIOReader>(location);
    }

    recorder.RecordSection("Start");

    size_t length = reader_ptr->length();
    if (length <= 0) {
        return nullptr;
    }

    size_t rp = 0;
    reader_ptr->seekg(0);

    auto current_type = IndexType::INVALID;
    reader_ptr->read(&current_type, sizeof(current_type));
    rp += sizeof(current_type);
    reader_ptr->seekg(rp);

    while (rp < length) {
        size_t meta_length;
        reader_ptr->read(&meta_length, sizeof(meta_length));
        rp += sizeof(meta_length);
        reader_ptr->seekg(rp);

        auto meta = new char[meta_length];
        reader_ptr->read(meta, meta_length);
        rp += meta_length;
        reader_ptr->seekg(rp);

        size_t bin_length;
        reader_ptr->read(&bin_length, sizeof(bin_length));
        rp += sizeof(bin_length);
        reader_ptr->seekg(rp);

        auto bin = new uint8_t[bin_length];
        reader_ptr->read(bin, bin_length);
        rp += bin_length;
        reader_ptr->seekg(rp);

        auto binptr = std::make_shared<uint8_t>();
        binptr.reset(bin);
        load_data_list.Append(std::string(meta, meta_length), binptr, bin_length);
        delete[] meta;
    }

    double span = recorder.RecordSection("End");
    double rate = length * 1000000.0 / span / 1024 / 1024;
    STORAGE_LOG_DEBUG << "read_index(" << location << ") rate " << rate << "MB/s";

    return LoadVecIndex(current_type, load_data_list, length);
}

Status
write_index(VecIndexPtr index, const std::string& location) {
    try {
        TimeRecorder recorder("write_index");

        auto binaryset = index->Serialize();
        auto index_type = index->GetType();

        bool minio_enable = false;
        server::Config& config = server::Config::GetInstance();
        config.GetStorageConfigMinioEnable(minio_enable);

        std::shared_ptr<storage::IOWriter> writer_ptr;
        if (minio_enable) {
            writer_ptr = std::make_shared<storage::S3IOWriter>(location);
        } else {
            writer_ptr = std::make_shared<storage::FileIOWriter>(location);
        }

        recorder.RecordSection("Start");

        writer_ptr->write(&index_type, sizeof(IndexType));

        for (auto& iter : binaryset.binary_map_) {
            auto meta = iter.first.c_str();
            size_t meta_length = iter.first.length();
            writer_ptr->write(&meta_length, sizeof(meta_length));
            writer_ptr->write((void*)meta, meta_length);

            auto binary = iter.second;
            int64_t binary_length = binary->size;
            writer_ptr->write(&binary_length, sizeof(binary_length));
            writer_ptr->write((void*)binary->data.get(), binary_length);
        }

        double span = recorder.RecordSection("End");
        double rate = writer_ptr->length() * 1000000.0 / span / 1024 / 1024;
        STORAGE_LOG_DEBUG << "write_index(" << location << ") rate " << rate << "MB/s";
    } catch (knowhere::KnowhereException& e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_UNEXPECTED_ERROR, e.what());
    } catch (std::exception& e) {
        WRAPPER_LOG_ERROR << e.what();
        std::string estring(e.what());
        if (estring.find("No space left on device") != estring.npos) {
            WRAPPER_LOG_ERROR << "No space left on the device";
            return Status(KNOWHERE_NO_SPACE, "No space left on the device");
        } else {
            return Status(KNOWHERE_ERROR, e.what());
        }
    }
    return Status::OK();
}

IndexType
ConvertToCpuIndexType(const IndexType& type) {
    // TODO(linxj): add IDMAP
    switch (type) {
        case IndexType::FAISS_IVFFLAT_GPU:
        case IndexType::FAISS_IVFFLAT_MIX: {
            return IndexType::FAISS_IVFFLAT_CPU;
        }
        case IndexType::FAISS_IVFSQ8_GPU:
        case IndexType::FAISS_IVFSQ8_MIX: {
            return IndexType::FAISS_IVFSQ8_CPU;
        }
        case IndexType::FAISS_IVFPQ_GPU:
        case IndexType::FAISS_IVFPQ_MIX: {
            return IndexType::FAISS_IVFPQ_CPU;
        }
        default: { return type; }
    }
}

IndexType
ConvertToGpuIndexType(const IndexType& type) {
    switch (type) {
        case IndexType::FAISS_IVFFLAT_MIX:
        case IndexType::FAISS_IVFFLAT_CPU: {
            return IndexType::FAISS_IVFFLAT_GPU;
        }
        case IndexType::FAISS_IVFSQ8_MIX:
        case IndexType::FAISS_IVFSQ8_CPU: {
            return IndexType::FAISS_IVFSQ8_GPU;
        }
        case IndexType::FAISS_IVFPQ_MIX:
        case IndexType::FAISS_IVFPQ_CPU: {
            return IndexType::FAISS_IVFPQ_GPU;
        }
        default: { return type; }
    }
}
}  // namespace engine
}  // namespace milvus
