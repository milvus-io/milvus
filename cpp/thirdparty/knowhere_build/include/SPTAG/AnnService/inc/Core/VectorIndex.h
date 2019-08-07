// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_VECTORINDEX_H_
#define _SPTAG_VECTORINDEX_H_

#include "Common.h"
#include "SearchQuery.h"
#include "VectorSet.h"
#include "MetadataSet.h"
#include "inc/Helper/SimpleIniReader.h"

namespace SPTAG
{

class VectorIndex
{
public:
    VectorIndex();

    virtual ~VectorIndex();

    virtual ErrorCode SaveIndex(const std::string& p_folderPath, std::ofstream& p_configout) = 0;

    virtual ErrorCode LoadIndex(const std::string& p_folderPath, Helper::IniReader& p_reader) = 0;

    virtual ErrorCode SaveIndexToMemory(std::vector<void*>& p_indexBlobs, std::vector<int64_t>& p_indexBlobsLen) = 0;

    virtual ErrorCode LoadIndexFromMemory(const std::vector<void*>& p_indexBlobs) = 0;

    virtual ErrorCode BuildIndex(const void* p_data, int p_vectorNum, int p_dimension) = 0;

    virtual ErrorCode SearchIndex(QueryResult& p_results) const = 0;

    virtual ErrorCode AddIndex(const void* p_vectors, int p_vectorNum, int p_dimension) = 0;

    virtual ErrorCode DeleteIndex(const void* p_vectors, int p_vectorNum) = 0;

    //virtual ErrorCode AddIndexWithID(const void* p_vector, const int& p_id) = 0;

    //virtual ErrorCode DeleteIndexWithID(const void* p_vector, const int& p_id) = 0;
    
    virtual float ComputeDistance(const void* pX, const void* pY) const = 0;
    virtual const void* GetSample(const int idx) const = 0;
    virtual int GetFeatureDim() const = 0;
    virtual int GetNumSamples() const = 0;

    virtual DistCalcMethod GetDistCalcMethod() const = 0;
    virtual IndexAlgoType GetIndexAlgoType() const = 0;
    virtual VectorValueType GetVectorValueType() const = 0;
    virtual int GetNumThreads() const = 0;

    virtual std::string GetParameter(const char* p_param) const = 0;
    virtual ErrorCode SetParameter(const char* p_param, const char* p_value) = 0;

    virtual ErrorCode LoadIndex(const std::string& p_folderPath);

    virtual ErrorCode SaveIndex(const std::string& p_folderPath);

    virtual ErrorCode BuildIndex(std::shared_ptr<VectorSet> p_vectorSet, std::shared_ptr<MetadataSet> p_metadataSet);
    
    virtual ErrorCode SearchIndex(const void* p_vector, int p_neighborCount, std::vector<BasicResult>& p_results) const;
    
    virtual ErrorCode AddIndex(std::shared_ptr<VectorSet> p_vectorSet, std::shared_ptr<MetadataSet> p_metadataSet);

    virtual std::string GetParameter(const std::string& p_param) const;
    virtual ErrorCode SetParameter(const std::string& p_param, const std::string& p_value);

    virtual ByteArray GetMetadata(IndexType p_vectorID) const;
    virtual void SetMetadata(const std::string& p_metadataFilePath, const std::string& p_metadataIndexPath);

    virtual std::string GetIndexName() const 
    { 
        if (m_sIndexName == "")
            return Helper::Convert::ConvertToString(GetIndexAlgoType());
        return m_sIndexName; 
    }
    virtual void SetIndexName(std::string p_name) { m_sIndexName = p_name; }

    static std::shared_ptr<VectorIndex> CreateInstance(IndexAlgoType p_algo, VectorValueType p_valuetype);

    static ErrorCode MergeIndex(const char* p_indexFilePath1, const char* p_indexFilePath2);
    
    static ErrorCode LoadIndex(const std::string& p_loaderFilePath, std::shared_ptr<VectorIndex>& p_vectorIndex);

protected:
    std::string m_sIndexName;
    std::shared_ptr<MetadataSet> m_pMetadata;
};


} // namespace SPTAG

#endif // _SPTAG_VECTORINDEX_H_
