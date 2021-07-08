//
// Created by ezeharia on 5/2/21.
//
#ifdef MILVUS_APU_VERSION

#include "knowhere/index/vector_index/fpga/GsiHammingIndex.h"

#include "scheduler/task/SearchTask.h"
#include "ApuInst.h"


namespace milvus{
namespace knowhere {


void
GsiHammingIndex::CopyIndexToFpga( uint32_t row_count ,const std::string& location) {


    num_bfeatures_ = Dim();
    num_bytes_in_rec_=num_bfeatures_ / CHAR_BITS ;// duplicate to PopulateAPuParams
    index_size_ = row_count*num_bytes_in_rec_;

    auto apu = Fpga::ApuInst::getInstance();
    apu->cleanApuResources(Fpga::APU_CLEAN_TYPE::NEW_DB);
    apu->PopulateApuParams(num_bfeatures_ , row_count , location);
    apu->createBdb();
    apu->loadSeesionToApu(Fpga::APU_METRIC_TYPE::HAMMING);
}

DatasetPtr
GsiHammingIndex::Query(const DatasetPtr &dataset, const Config &config , faiss::ConcurrentBitsetPtr blacklist) {

    auto apu = Fpga::ApuInst::getInstance();

    num_bfeatures_= Dim();
    num_queries_ = static_cast<uint32_t>(dataset.get()->Get<long>(meta::ROWS));
    num_bytes_in_rec_=num_bfeatures_ / CHAR_BITS ;
    topK_=config[meta::TOPK];

    apu->setTopK(topK_);
    AllocateMemory(dataset , config);

    apu->Query( indices_ , distances_, queries_ , Fpga::APU_METRIC_TYPE::HAMMING)  ;

    auto ret_ds = std::make_shared<Dataset>();

    auto start_calloc = std::chrono::steady_clock::now();
    int64_t* ids_int64 =(int64_t*)calloc(topK_ * num_queries_, sizeof(int64_t));
    auto end_calloc = std::chrono::steady_clock::now();
    std::cout << "Elapsed time of calloc in microseconds: "
              << std::chrono::duration_cast<std::chrono::microseconds>(end_calloc - start_calloc).count() << " µs"
              << std::endl;

    auto start_conv = std::chrono::steady_clock::now();
    convertToInt64_t(&indices_ , ids_int64);
    auto end_conv = std::chrono::steady_clock::now();
    std::cout << "Elapsed time for conversion in microseconds: "
              << std::chrono::duration_cast<std::chrono::microseconds>(end_conv - start_conv).count() << " µs"
              << std::endl;

    ret_ds->Set(meta::IDS, ids_int64);
    ret_ds->Set(meta::DISTANCE, distances_.rows_f32);
    free((void*)indices_.rows_u32);
    return ret_ds;
}
#endif
}//knowhere
}//milvus
