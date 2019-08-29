
#include <random>
#include <iostream>
#include <memory>
#include "SPTAG/AnnService/inc/Core/Common.h"
#include "SPTAG/AnnService/inc/Core/VectorIndex.h"


int
main(int argc, char *argv[]) {
    using namespace SPTAG;
    const int d = 128;
    const int n = 100;

    auto p_data = new float[n * d];

    auto index = VectorIndex::CreateInstance(IndexAlgoType::KDT, VectorValueType::Float);

    std::random_device rd;
    std::mt19937 mt(rd());
    std::uniform_real_distribution<double> dist(1.0, 2.0);

    for (auto i = 0; i < n; i++) {
        for (auto j = 0; j < d; j++) {
            p_data[i * d + j] = dist(mt) - 1;
        }
    }
    std::cout << "generate random n * d finished.";
    ByteArray data((uint8_t *) p_data, n * d * sizeof(float), true);

    auto vectorset = std::make_shared<BasicVectorSet>(data, VectorValueType::Float, d, n);
    index->BuildIndex(vectorset, nullptr);

    std::cout << index->GetFeatureDim();
}

