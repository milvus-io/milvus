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
#include "knowhere/index/vector_index/IndexIVF.h"
#include "knowhere/index/vector_index/IndexGPUIVF.h"
#include "knowhere/index/vector_index/IndexIVFSQ.h"
#include "knowhere/index/vector_index/IndexGPUIVFSQ.h"
#include "knowhere/index/vector_index/IndexIVFPQ.h"
#include "knowhere/index/vector_index/IndexGPUIVFPQ.h"
#include "knowhere/index/vector_index/IndexIDMAP.h"
#include "knowhere/index/vector_index/IndexKDT.h"
#include "knowhere/index/vector_index/IndexNSG.h"
#include "knowhere/common/Exception.h"
#include "VecImpl.h"
#include "utils/Log.h"

#include <cuda.h>

namespace zilliz {
namespace milvus {
namespace engine {

struct FileIOReader {
    std::fstream fs;
    std::string name;

    explicit FileIOReader(const std::string &fname);

    ~FileIOReader();

    size_t
    operator()(void *ptr, size_t size);

    size_t
    operator()(void *ptr, size_t size, size_t pos);
};

FileIOReader::FileIOReader(const std::string &fname) {
    name = fname;
    fs = std::fstream(name, std::ios::in | std::ios::binary);
}

FileIOReader::~FileIOReader() {
    fs.close();
}

size_t
FileIOReader::operator()(void *ptr, size_t size) {
    fs.read(reinterpret_cast<char *>(ptr), size);
}

size_t
FileIOReader::operator()(void *ptr, size_t size, size_t pos) {
    return 0;
}

struct FileIOWriter {
    std::fstream fs;
    std::string name;

    explicit FileIOWriter(const std::string &fname);
    ~FileIOWriter();
    size_t operator()(void *ptr, size_t size);
};

FileIOWriter::FileIOWriter(const std::string &fname) {
    name = fname;
    fs = std::fstream(name, std::ios::out | std::ios::binary);
}

FileIOWriter::~FileIOWriter() {
    fs.close();
}

size_t
FileIOWriter::operator()(void *ptr, size_t size) {
    fs.write(reinterpret_cast<char *>(ptr), size);
}

VecIndexPtr
GetVecIndexFactory(const IndexType &type, const Config &cfg) {
    std::shared_ptr<zilliz::knowhere::VectorIndex> index;
    auto gpu_device = -1; // TODO(linxj): remove hardcode here
    switch (type) {
        case IndexType::FAISS_IDMAP: {
            index = std::make_shared<zilliz::knowhere::IDMAP>();
            return std::make_shared<BFIndex>(index);
        }
        case IndexType::FAISS_IVFFLAT_CPU: {
            index = std::make_shared<zilliz::knowhere::IVF>();
            break;
        }
        case IndexType::FAISS_IVFFLAT_GPU: {
            index = std::make_shared<zilliz::knowhere::GPUIVF>(gpu_device);
            break;
        }
        case IndexType::FAISS_IVFFLAT_MIX: {
            index = std::make_shared<zilliz::knowhere::GPUIVF>(gpu_device);
            return std::make_shared<IVFMixIndex>(index, IndexType::FAISS_IVFFLAT_MIX);
        }
        case IndexType::FAISS_IVFPQ_CPU: {
            index = std::make_shared<zilliz::knowhere::IVFPQ>();
            break;
        }
        case IndexType::FAISS_IVFPQ_GPU: {
            index = std::make_shared<zilliz::knowhere::GPUIVFPQ>(gpu_device);
            break;
        }
        case IndexType::SPTAG_KDT_RNT_CPU: {
            index = std::make_shared<zilliz::knowhere::CPUKDTRNG>();
            break;
        }
        case IndexType::FAISS_IVFSQ8_MIX: {
            index = std::make_shared<zilliz::knowhere::GPUIVFSQ>(gpu_device);
            return std::make_shared<IVFMixIndex>(index, IndexType::FAISS_IVFSQ8_MIX);
        }
        case IndexType::FAISS_IVFSQ8_CPU: {
            index = std::make_shared<zilliz::knowhere::IVFSQ>();
            break;
        }
        case IndexType::FAISS_IVFSQ8_GPU: {
            index = std::make_shared<zilliz::knowhere::GPUIVFSQ>(gpu_device);
            break;
        }
        case IndexType::NSG_MIX: { // TODO(linxj): bug.
            index = std::make_shared<zilliz::knowhere::NSG>(gpu_device);
            break;
        }
        default: {
            return nullptr;
        }
    }
    return std::make_shared<VecIndexImpl>(index, type);
}

VecIndexPtr
LoadVecIndex(const IndexType &index_type, const zilliz::knowhere::BinarySet &index_binary) {
    auto index = GetVecIndexFactory(index_type);
    index->Load(index_binary);
    return index;
}

VecIndexPtr
read_index(const std::string &location) {
    knowhere::BinarySet load_data_list;
    FileIOReader reader(location);
    reader.fs.seekg(0, reader.fs.end);
    int64_t length = reader.fs.tellg();
    if (length <= 0) {
        return nullptr;
    }

    reader.fs.seekg(0);

    size_t rp = 0;
    auto current_type = IndexType::INVALID;
    reader(&current_type, sizeof(current_type));
    rp += sizeof(current_type);
    while (rp < length) {
        size_t meta_length;
        reader(&meta_length, sizeof(meta_length));
        rp += sizeof(meta_length);
        reader.fs.seekg(rp);

        auto meta = new char[meta_length];
        reader(meta, meta_length);
        rp += meta_length;
        reader.fs.seekg(rp);

        size_t bin_length;
        reader(&bin_length, sizeof(bin_length));
        rp += sizeof(bin_length);
        reader.fs.seekg(rp);

        auto bin = new uint8_t[bin_length];
        reader(bin, bin_length);
        rp += bin_length;

        auto binptr = std::make_shared<uint8_t>();
        binptr.reset(bin);
        load_data_list.Append(std::string(meta, meta_length), binptr, bin_length);
        delete[] meta;
    }

    return LoadVecIndex(current_type, load_data_list);
}

Status
write_index(VecIndexPtr index, const std::string &location) {
    try {
        auto binaryset = index->Serialize();
        auto index_type = index->GetType();

        FileIOWriter writer(location);
        writer(&index_type, sizeof(IndexType));
        for (auto &iter : binaryset.binary_map_) {
            auto meta = iter.first.c_str();
            size_t meta_length = iter.first.length();
            writer(&meta_length, sizeof(meta_length));
            writer((void *) meta, meta_length);

            auto binary = iter.second;
            int64_t binary_length = binary->size;
            writer(&binary_length, sizeof(binary_length));
            writer((void *) binary->data.get(), binary_length);
        }
    } catch (knowhere::KnowhereException &e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_UNEXPECTED_ERROR, e.what());
    } catch (std::exception &e) {
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
ConvertToCpuIndexType(const IndexType &type) {
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
        default: {
            return type;
        }
    }
}

IndexType
ConvertToGpuIndexType(const IndexType &type) {
    switch (type) {
        case IndexType::FAISS_IVFFLAT_MIX:
        case IndexType::FAISS_IVFFLAT_CPU: {
            return IndexType::FAISS_IVFFLAT_GPU;
        }
        case IndexType::FAISS_IVFSQ8_MIX:
        case IndexType::FAISS_IVFSQ8_CPU: {
            return IndexType::FAISS_IVFSQ8_GPU;
        }
        default: {
            return type;
        }
    }
}

} // namespace engine
} // namespace milvus
} // namespace zilliz
