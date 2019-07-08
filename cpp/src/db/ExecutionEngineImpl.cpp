/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include <src/server/ServerConfig.h>
#include "Log.h"

#include "src/cache/CpuCacheMgr.h"
#include "ExecutionEngineImpl.h"
#include "wrapper/knowhere/vec_index.h"
#include "wrapper/knowhere/vec_impl.h"


namespace zilliz {
namespace milvus {
namespace engine {

struct FileIOWriter {
    std::fstream fs;
    std::string name;

    FileIOWriter(const std::string &fname);
    ~FileIOWriter();
    size_t operator()(void *ptr, size_t size);
};

struct FileIOReader {
    std::fstream fs;
    std::string name;

    FileIOReader(const std::string &fname);
    ~FileIOReader();
    size_t operator()(void *ptr, size_t size);
    size_t operator()(void *ptr, size_t size, size_t pos);
};

FileIOReader::FileIOReader(const std::string &fname) {
    name = fname;
    fs = std::fstream(name, std::ios::in | std::ios::binary);
}

FileIOReader::~FileIOReader() {
    fs.close();
}

size_t FileIOReader::operator()(void *ptr, size_t size) {
    fs.read(reinterpret_cast<char *>(ptr), size);
}

size_t FileIOReader::operator()(void *ptr, size_t size, size_t pos) {
    return 0;
}

FileIOWriter::FileIOWriter(const std::string &fname) {
    name = fname;
    fs = std::fstream(name, std::ios::out | std::ios::binary);
}

FileIOWriter::~FileIOWriter() {
    fs.close();
}

size_t FileIOWriter::operator()(void *ptr, size_t size) {
    fs.write(reinterpret_cast<char *>(ptr), size);
}

ExecutionEngineImpl::ExecutionEngineImpl(uint16_t dimension,
                                         const std::string &location,
                                         EngineType type)
    : location_(location), dim(dimension), build_type(type) {
    index_ = CreatetVecIndex(EngineType::FAISS_IDMAP);
    current_type = EngineType::FAISS_IDMAP;
    std::static_pointer_cast<BFIndex>(index_)->Build(dimension);
}

ExecutionEngineImpl::ExecutionEngineImpl(VecIndexPtr index,
                                         const std::string &location,
                                         EngineType type)
    : index_(std::move(index)), location_(location), build_type(type) {
    current_type = type;
}

VecIndexPtr ExecutionEngineImpl::CreatetVecIndex(EngineType type) {
    std::shared_ptr<VecIndex> index;
    switch (type) {
        case EngineType::FAISS_IDMAP: {
            index = GetVecIndexFactory(IndexType::FAISS_IDMAP);
            break;
        }
        case EngineType::FAISS_IVFFLAT_GPU: {
            index = GetVecIndexFactory(IndexType::FAISS_IVFFLAT_GPU);
            break;
        }
        case EngineType::FAISS_IVFFLAT_CPU: {
            index = GetVecIndexFactory(IndexType::FAISS_IVFFLAT_CPU);
            break;
        }
        case EngineType::SPTAG_KDT_RNT_CPU: {
            index = GetVecIndexFactory(IndexType::SPTAG_KDT_RNT_CPU);
            break;
        }
        default: {
            ENGINE_LOG_ERROR << "Invalid engine type";
            return nullptr;
        }
    }
    return index;
}

Status ExecutionEngineImpl::AddWithIds(long n, const float *xdata, const long *xids) {
    index_->Add(n, xdata, xids, Config::object{{"dim", dim}});
    return Status::OK();
}

size_t ExecutionEngineImpl::Count() const {
    return index_->Count();
}

size_t ExecutionEngineImpl::Size() const {
    return (size_t) (Count() * Dimension()) * sizeof(float);
}

size_t ExecutionEngineImpl::Dimension() const {
    return index_->Dimension();
}

size_t ExecutionEngineImpl::PhysicalSize() const {
    return (size_t) (Count() * Dimension()) * sizeof(float);
}

Status ExecutionEngineImpl::Serialize() {
    auto binaryset = index_->Serialize();

    FileIOWriter writer(location_);
    writer(&current_type, sizeof(current_type));
    for (auto &iter: binaryset.binary_map_) {
        auto meta = iter.first.c_str();
        size_t meta_length = iter.first.length();
        writer(&meta_length, sizeof(meta_length));
        writer((void *) meta, meta_length);

        auto binary = iter.second;
        size_t binary_length = binary->size;
        writer(&binary_length, sizeof(binary_length));
        writer((void *) binary->data.get(), binary_length);
    }
    return Status::OK();
}

Status ExecutionEngineImpl::Load() {
    index_ = Load(location_);
    return Status::OK();
}

VecIndexPtr ExecutionEngineImpl::Load(const std::string &location) {
    knowhere::BinarySet load_data_list;
    FileIOReader reader(location);
    reader.fs.seekg(0, reader.fs.end);
    size_t length = reader.fs.tellg();
    reader.fs.seekg(0);

    size_t rp = 0;
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

        auto xx = std::make_shared<uint8_t>();
        xx.reset(bin);
        load_data_list.Append(std::string(meta, meta_length), xx, bin_length);
    }

    auto index_type = IndexType::INVALID;
    switch (current_type) {
        case EngineType::FAISS_IDMAP: {
            index_type = IndexType::FAISS_IDMAP;
            break;
        }
        case EngineType::FAISS_IVFFLAT_CPU: {
            index_type = IndexType::FAISS_IVFFLAT_CPU;
            break;
        }
        case EngineType::FAISS_IVFFLAT_GPU: {
            index_type = IndexType::FAISS_IVFFLAT_GPU;
            break;
        }
        case EngineType::SPTAG_KDT_RNT_CPU: {
            index_type = IndexType::SPTAG_KDT_RNT_CPU;
            break;
        }
    }

    return LoadVecIndex(index_type, load_data_list);
}

Status ExecutionEngineImpl::Merge(const std::string &location) {
    if (location == location_) {
        return Status::Error("Cannot Merge Self");
    }
    ENGINE_LOG_DEBUG << "Merge index file: " << location << " to: " << location_;

    auto to_merge = zilliz::milvus::cache::CpuCacheMgr::GetInstance()->GetIndex(location);
    if (!to_merge) {
        to_merge = Load(location);
    }

    auto file_index = std::dynamic_pointer_cast<BFIndex>(to_merge);
    index_->Add(file_index->Count(), file_index->GetRawVectors(), file_index->GetRawIds());
    return Status::OK();
}

// TODO(linxj): add config
ExecutionEnginePtr
ExecutionEngineImpl::BuildIndex(const std::string &location) {
    ENGINE_LOG_DEBUG << "Build index file: " << location << " from: " << location_;

    auto from_index = std::dynamic_pointer_cast<BFIndex>(index_);
    auto to_index = CreatetVecIndex(build_type);
    to_index->BuildAll(Count(),
                       from_index->GetRawVectors(),
                       from_index->GetRawIds(),
                       Config::object{{"dim", Dimension()}, {"gpu_id", gpu_num}});

    return std::make_shared<ExecutionEngineImpl>(to_index, location, build_type);
}

Status ExecutionEngineImpl::Search(long n,
                                   const float *data,
                                   long k,
                                   float *distances,
                                   long *labels) const {
    index_->Search(n, data, distances, labels, Config::object{{"k", k}, {"nprobe", nprobe_}});
    return Status::OK();
}

Status ExecutionEngineImpl::Cache() {
    zilliz::milvus::cache::CpuCacheMgr::GetInstance()->InsertItem(location_, index_);

    return Status::OK();
}

Status ExecutionEngineImpl::Init() {
    using namespace zilliz::milvus::server;
    ServerConfig &config = ServerConfig::GetInstance();
    ConfigNode server_config = config.GetConfig(CONFIG_SERVER);
    gpu_num = server_config.GetInt32Value("gpu_index", 0);

    switch (build_type) {
        case EngineType::FAISS_IVFFLAT_GPU: {
        }
        case EngineType::FAISS_IVFFLAT_CPU: {
            ConfigNode engine_config = config.GetConfig(CONFIG_ENGINE);
            nprobe_ = engine_config.GetInt32Value(CONFIG_NPROBE, 1000);
            break;
        }
    }

    return Status::OK();
}


} // namespace engine
} // namespace milvus
} // namespace zilliz
