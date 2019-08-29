// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Test.h"
#include "inc/Helper/SimpleIniReader.h"
#include "inc/Core/VectorIndex.h"


template<typename T>
void Test(SPTAG::IndexAlgoType algo, std::string distCalcMethod) {
    int n = 100, q = 3, m = 10, k = 3;
    std::vector<T> vec;
    for (int i = 0; i < n; i++) {
        for (int j = 0; j < m; j++) {
            vec.push_back((T) i);
        }
    }

    std::vector<T> query;
    for (int i = 0; i < q; i++) {
        for (int j = 0; j < m; j++) {
            query.push_back((T) i * 2);
        }
    }

    std::shared_ptr<SPTAG::VectorSet> vecset(new SPTAG::BasicVectorSet(
        SPTAG::ByteArray((std::uint8_t *) vec.data(), n * m * sizeof(T), false),
        SPTAG::GetEnumValueType<T>(), m, n));

    std::vector<void *> blobs;
    std::vector<int64_t> len;
    {
        std::shared_ptr<SPTAG::VectorIndex> vecIndex =
            SPTAG::VectorIndex::CreateInstance(algo, SPTAG::GetEnumValueType<T>());
        vecIndex->SetParameter("DistCalcMethod", distCalcMethod);
        BOOST_CHECK(SPTAG::ErrorCode::Success == vecIndex->BuildIndex(vecset, nullptr));
        BOOST_CHECK(SPTAG::ErrorCode::Success == vecIndex->SaveIndexToMemory(blobs, len));
    }

    std::vector<void *> clone_blobs;
    std::vector<int64_t> clone_len;
    for (auto i = 0; i < blobs.size(); ++i) {
        auto mem = malloc(len[i]);
        BOOST_CHECK(NULL != mem);
        memcpy(mem, blobs[i], len[i]);
        clone_blobs.push_back(mem);
        clone_len.push_back(len[i]);
    }

    std::shared_ptr<SPTAG::VectorIndex> clone_index =
        SPTAG::VectorIndex::CreateInstance(algo, SPTAG::GetEnumValueType<T>());
    clone_index->SetParameter("DistCalcMethod", distCalcMethod);
    BOOST_CHECK(SPTAG::ErrorCode::Success == clone_index->LoadIndexFromMemory(clone_blobs));

    SPTAG::QueryResult res(vec.data(), k, true);
    clone_index->SearchIndex(res);
    for (int i = 0; i < k; i++) {
        std::cout << res.GetResult(i)->Dist << "@(" << res.GetResult(i)->VID << ","
                  << std::string((char *) res.GetMetadata(i).Data(), res.GetMetadata(i).Length()) << ") ";
    }
    std::cout << std::endl;

    for (auto &blob : blobs)
        free(blob);
    for (auto &blob : clone_blobs)
        free(blob);
}

BOOST_AUTO_TEST_SUITE (SerializeTest)

BOOST_AUTO_TEST_CASE(KDTree) {
    Test<float>(SPTAG::IndexAlgoType::KDT, "L2");
}

BOOST_AUTO_TEST_CASE(BKTree) {
    Test<float>(SPTAG::IndexAlgoType::BKT, "L2");
}

BOOST_AUTO_TEST_SUITE_END()
