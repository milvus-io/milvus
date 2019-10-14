// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Test.h"
#include "inc/Helper/SimpleIniReader.h"
#include "inc/Core/VectorIndex.h"

template <typename T>
void Build(SPTAG::IndexAlgoType algo, std::string distCalcMethod, T* vec, int n, int m)
{
    std::vector<char> meta;
    std::vector<long long> metaoffset;
    for (int i = 0; i < n; i++) {
        metaoffset.push_back(meta.size());
        std::string a = std::to_string(i);
        for (int j = 0; j < a.length(); j++)
            meta.push_back(a[j]);
    }
    metaoffset.push_back(meta.size());

    std::shared_ptr<SPTAG::VectorSet> vecset(new SPTAG::BasicVectorSet(
        SPTAG::ByteArray((std::uint8_t*)vec, n * m * sizeof(T), false),
        SPTAG::GetEnumValueType<T>(), m, n));

    std::shared_ptr<SPTAG::MetadataSet> metaset(new SPTAG::MemMetadataSet(
        SPTAG::ByteArray((std::uint8_t*)meta.data(), meta.size() * sizeof(char), false),
        SPTAG::ByteArray((std::uint8_t*)metaoffset.data(), metaoffset.size() * sizeof(long long), false),
        n));
    std::shared_ptr<SPTAG::VectorIndex> vecIndex = SPTAG::VectorIndex::CreateInstance(algo, SPTAG::GetEnumValueType<T>());
    vecIndex->SetParameter("DistCalcMethod", distCalcMethod);
    BOOST_CHECK(nullptr != vecIndex);
    BOOST_CHECK(SPTAG::ErrorCode::Success == vecIndex->BuildIndex(vecset, metaset));
    BOOST_CHECK(SPTAG::ErrorCode::Success == vecIndex->SaveIndex("origindices"));
}

template <typename T>
void Search(std::string folder, T* vec, int k)
{
    std::shared_ptr<SPTAG::VectorIndex> vecIndex;
    BOOST_CHECK(SPTAG::ErrorCode::Success == SPTAG::VectorIndex::LoadIndex(folder, vecIndex));
    BOOST_CHECK(nullptr != vecIndex);

    SPTAG::QueryResult res(vec, k, true);
    vecIndex->SearchIndex(res);
    for (int i = 0; i < k; i++) {
        std::cout << res.GetResult(i)->Dist << "@(" << res.GetResult(i)->VID << "," << std::string((char*)res.GetMetadata(i).Data(), res.GetMetadata(i).Length()) << ") ";
    }
    std::cout << std::endl;
    vecIndex.reset();
}

template <typename T>
void Add(T* vec, int n)
{
    std::shared_ptr<SPTAG::VectorIndex> vecIndex;
    BOOST_CHECK(SPTAG::ErrorCode::Success == SPTAG::VectorIndex::LoadIndex("origindices", vecIndex));
    BOOST_CHECK(nullptr != vecIndex);

    std::vector<char> meta;
    std::vector<long long> metaoffset;
    for (int i = 0; i < n; i++) {
        metaoffset.push_back(meta.size());
        std::string a = std::to_string(vecIndex->GetNumSamples() + i);
        for (int j = 0; j < a.length(); j++)
            meta.push_back(a[j]);
    }
    metaoffset.push_back(meta.size());

    int m = vecIndex->GetFeatureDim();
    std::shared_ptr<SPTAG::VectorSet> vecset(new SPTAG::BasicVectorSet(
        SPTAG::ByteArray((std::uint8_t*)vec, n * m * sizeof(T), false),
        SPTAG::GetEnumValueType<T>(), m, n));

    std::shared_ptr<SPTAG::MetadataSet> metaset(new SPTAG::MemMetadataSet(
        SPTAG::ByteArray((std::uint8_t*)meta.data(), meta.size() * sizeof(char), false),
        SPTAG::ByteArray((std::uint8_t*)metaoffset.data(), metaoffset.size() * sizeof(long long), false),
        n));

    BOOST_CHECK(SPTAG::ErrorCode::Success == vecIndex->AddIndex(vecset, metaset));
    BOOST_CHECK(SPTAG::ErrorCode::Success == vecIndex->SaveIndex("addindices"));
    vecIndex.reset();
}

template <typename T>
void Delete(T* vec, int n)
{
    std::shared_ptr<SPTAG::VectorIndex> vecIndex;
    BOOST_CHECK(SPTAG::ErrorCode::Success == SPTAG::VectorIndex::LoadIndex("addindices", vecIndex));
    BOOST_CHECK(nullptr != vecIndex);

    BOOST_CHECK(SPTAG::ErrorCode::Success == vecIndex->DeleteIndex((const void*)vec, n));
    BOOST_CHECK(SPTAG::ErrorCode::Success == vecIndex->SaveIndex("delindices"));
    vecIndex.reset();
}

template <typename T>
void Test(SPTAG::IndexAlgoType algo, std::string distCalcMethod)
{
    int n = 100, q = 3, m = 10, k = 3;
    std::vector<T> vec;
    for (int i = 0; i < n; i++) {
        for (int j = 0; j < m; j++) {
            vec.push_back((T)i);
        }
    }
    
    std::vector<T> query;
    for (int i = 0; i < q; i++) {
        for (int j = 0; j < m; j++) {
            query.push_back((T)i*2);
        }
    }
    
    Build<T>(algo, distCalcMethod, vec.data(), n, m);
    Search<T>("origindices", query.data(), k);
    Add<T>(query.data(), q);
    Search<T>("addindices", query.data(), k);
    Delete<T>(query.data(), q);
    Search<T>("delindices", query.data(), k);
}

BOOST_AUTO_TEST_SUITE (AlgoTest)

BOOST_AUTO_TEST_CASE(KDTTest)
{
    Test<float>(SPTAG::IndexAlgoType::KDT, "L2");
}

BOOST_AUTO_TEST_CASE(BKTTest)
{
    Test<float>(SPTAG::IndexAlgoType::BKT, "L2");
}

BOOST_AUTO_TEST_SUITE_END()
