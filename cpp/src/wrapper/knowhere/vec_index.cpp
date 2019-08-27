////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include "knowhere/index/vector_index/ivf.h"
#include "knowhere/index/vector_index/idmap.h"
#include "knowhere/index/vector_index/gpu_ivf.h"
#include "knowhere/index/vector_index/cpu_kdt_rng.h"
#include "knowhere/index/vector_index/nsg_index.h"
#include "knowhere/common/exception.h"

#include "vec_index.h"
#include "vec_impl.h"
#include "wrapper_log.h"


namespace zilliz {
namespace milvus {
namespace engine {

static constexpr float TYPICAL_COUNT = 1000000.0;

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


VecIndexPtr GetVecIndexFactory(const IndexType &type) {
    std::shared_ptr<zilliz::knowhere::VectorIndex> index;
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
            index = std::make_shared<zilliz::knowhere::GPUIVF>(0);
            break;
        }
        case IndexType::FAISS_IVFFLAT_MIX: {
            index = std::make_shared<zilliz::knowhere::GPUIVF>(0);
            return std::make_shared<IVFMixIndex>(index, IndexType::FAISS_IVFFLAT_MIX);
        }
        case IndexType::FAISS_IVFPQ_CPU: {
            index = std::make_shared<zilliz::knowhere::IVFPQ>();
            break;
        }
        case IndexType::FAISS_IVFPQ_GPU: {
            index = std::make_shared<zilliz::knowhere::GPUIVFPQ>(0);
            break;
        }
        case IndexType::SPTAG_KDT_RNT_CPU: {
            index = std::make_shared<zilliz::knowhere::CPUKDTRNG>();
            break;
        }
        case IndexType::FAISS_IVFSQ8_MIX: {
            index = std::make_shared<zilliz::knowhere::GPUIVFSQ>(0);
            return std::make_shared<IVFMixIndex>(index, IndexType::FAISS_IVFSQ8_MIX);
        }
        case IndexType::NSG_MIX: { // TODO(linxj): bug.
            index = std::make_shared<zilliz::knowhere::NSG>(0);
            break;
        }
        default: {
            return nullptr;
        }
    }
    return std::make_shared<VecIndexImpl>(index, type);
}

VecIndexPtr LoadVecIndex(const IndexType &index_type, const zilliz::knowhere::BinarySet &index_binary) {
    auto index = GetVecIndexFactory(index_type);
    index->Load(index_binary);
    return index;
}

VecIndexPtr read_index(const std::string &location) {
    knowhere::BinarySet load_data_list;
    FileIOReader reader(location);
    reader.fs.seekg(0, reader.fs.end);
    size_t length = reader.fs.tellg();
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
    }

    return LoadVecIndex(current_type, load_data_list);
}

server::KnowhereError write_index(VecIndexPtr index, const std::string &location) {
    try {
        auto binaryset = index->Serialize();
        auto index_type = index->GetType();

        FileIOWriter writer(location);
        writer(&index_type, sizeof(IndexType));
        for (auto &iter: binaryset.binary_map_) {
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
        return server::KNOWHERE_UNEXPECTED_ERROR;
    } catch (std::exception &e) {
        WRAPPER_LOG_ERROR << e.what();
        return server::KNOWHERE_ERROR;
    }
    return server::KNOWHERE_SUCCESS;
}


// TODO(linxj): redo here.
void AutoGenParams(const IndexType &type, const long &size, zilliz::knowhere::Config &cfg) {
    auto nlist = cfg.get_with_default("nlist", 0);
    if (size <= TYPICAL_COUNT/16384 + 1) {
        //handle less row count, avoid nlist set to 0
        cfg["nlist"] = 1;
    } else if (int(size/TYPICAL_COUNT) * nlist == 0) {
        //calculate a proper nlist if nlist not specified or size less than TYPICAL_COUNT
        cfg["nlist"] = int(size / TYPICAL_COUNT * 16384);
    }

    if (!cfg.contains("gpu_id")) { cfg["gpu_id"] = int(0); }
    if (!cfg.contains("metric_type")) { cfg["metric_type"] = "L2"; }

    switch (type) {
        case IndexType::FAISS_IVFSQ8_MIX: {
            if (!cfg.contains("nbits")) { cfg["nbits"] = int(8); }
            break;
        }
        case IndexType::NSG_MIX: {
            auto scale_factor = round(cfg["dim"].as<int>() / 128.0);
            scale_factor = scale_factor >= 4 ? 4 : scale_factor;
            cfg["nlist"] = int(size / 1000000.0 * 8192);
            if (!cfg.contains("nprobe")) { cfg["nprobe"] = 6 + 10 * scale_factor; }
            if (!cfg.contains("knng")) { cfg["knng"] = 100 + 100 * scale_factor; }
            if (!cfg.contains("search_length")) { cfg["search_length"] = 40 + 5 * scale_factor; }
            if (!cfg.contains("out_degree")) { cfg["out_degree"] = 50 + 5 * scale_factor; }
            if (!cfg.contains("candidate_pool_size")) { cfg["candidate_pool_size"] = 200 + 100 * scale_factor; }
            WRAPPER_LOG_DEBUG << pretty_print(cfg);
            break;
        }
    }
}

}
}
}
