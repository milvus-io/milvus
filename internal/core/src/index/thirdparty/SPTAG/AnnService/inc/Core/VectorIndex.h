// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_VECTORINDEX_H_
#define _SPTAG_VECTORINDEX_H_

#include "Common.h"
#include "SearchQuery.h"
#include "VectorSet.h"
#include "MetadataSet.h"
#include "inc/Helper/SimpleIniReader.h"

#include <unordered_map>

namespace SPTAG
{

class VectorIndex
{
public:
    VectorIndex();

    virtual ~VectorIndex();

    virtual ErrorCode BuildIndex(const void* p_data, SizeType p_vectorNum, DimensionType p_dimension) = 0;

    virtual ErrorCode AddIndex(const void* p_vectors, SizeType p_vectorNum, DimensionType p_dimension, SizeType* p_start = nullptr) = 0;

    virtual ErrorCode DeleteIndex(const void* p_vectors, SizeType p_vectorNum) = 0;

    virtual ErrorCode SearchIndex(QueryResult& p_results) const = 0;
    
    virtual float ComputeDistance(const void* pX, const void* pY) const = 0;
    virtual const void* GetSample(const SizeType idx) const = 0;
    virtual bool ContainSample(const SizeType idx) const = 0;
    virtual bool NeedRefine() const = 0;
   
    virtual DimensionType GetFeatureDim() const = 0;
    virtual SizeType GetNumSamples() const = 0;
    virtual SizeType GetIndexSize() const = 0;

    virtual DistCalcMethod GetDistCalcMethod() const = 0;
    virtual IndexAlgoType GetIndexAlgoType() const = 0;
    virtual VectorValueType GetVectorValueType() const = 0;

    virtual std::string GetParameter(const char* p_param) const = 0;
    virtual ErrorCode SetParameter(const char* p_param, const char* p_value) = 0;

    virtual std::shared_ptr<std::vector<std::uint64_t>> CalculateBufferSize() const;

    virtual ErrorCode LoadIndex(const std::string& p_config, const std::vector<ByteArray>& p_indexBlobs);

    virtual ErrorCode LoadIndex(const std::string& p_folderPath);

    virtual ErrorCode SaveIndex(std::string& p_config, const std::vector<ByteArray>& p_indexBlobs);

    virtual ErrorCode SaveIndex(const std::string& p_folderPath);

    virtual ErrorCode BuildIndex(std::shared_ptr<VectorSet> p_vectorSet, std::shared_ptr<MetadataSet> p_metadataSet, bool p_withMetaIndex = false);
    
    virtual ErrorCode AddIndex(std::shared_ptr<VectorSet> p_vectorSet, std::shared_ptr<MetadataSet> p_metadataSet);

    virtual ErrorCode DeleteIndex(ByteArray p_meta);

    virtual const void* GetSample(ByteArray p_meta);

    virtual ErrorCode SearchIndex(const void* p_vector, int p_neighborCount, bool p_withMeta, BasicResult* p_results) const;

    virtual std::string GetParameter(const std::string& p_param) const;
    virtual ErrorCode SetParameter(const std::string& p_param, const std::string& p_value);

    virtual ByteArray GetMetadata(SizeType p_vectorID) const;
    virtual void SetMetadata(const std::string& p_metadataFilePath, const std::string& p_metadataIndexPath);

    virtual std::string GetIndexName() const 
    { 
        if (m_sIndexName == "") return Helper::Convert::ConvertToString(GetIndexAlgoType());
        return m_sIndexName; 
    }
    virtual void SetIndexName(std::string p_name) { m_sIndexName = p_name; }

    static std::shared_ptr<VectorIndex> CreateInstance(IndexAlgoType p_algo, VectorValueType p_valuetype);

    static ErrorCode MergeIndex(const char* p_indexFilePath1, const char* p_indexFilePath2);
    
    static ErrorCode LoadIndex(const std::string& p_loaderFilePath, std::shared_ptr<VectorIndex>& p_vectorIndex);

    static ErrorCode LoadIndex(const std::string& p_config, const std::vector<ByteArray>& p_indexBlobs, std::shared_ptr<VectorIndex>& p_vectorIndex);

protected:
    virtual std::shared_ptr<std::vector<std::uint64_t>> BufferSize() const = 0;

    virtual ErrorCode SaveConfig(std::ostream& p_configout) const = 0;

    virtual ErrorCode SaveIndexData(const std::string& p_folderPath) = 0;

    virtual ErrorCode SaveIndexData(const std::vector<std::ostream*>& p_indexStreams) = 0;

    virtual ErrorCode LoadConfig(Helper::IniReader& p_reader) = 0;

    virtual ErrorCode LoadIndexData(const std::string& p_folderPath) = 0;

    virtual ErrorCode LoadIndexDataFromMemory(const std::vector<ByteArray>& p_indexBlobs) = 0;

    virtual ErrorCode DeleteIndex(const SizeType& p_id) = 0;

    virtual ErrorCode RefineIndex(const std::string& p_folderPath) = 0;

    virtual ErrorCode RefineIndex(const std::vector<std::ostream*>& p_indexStreams) = 0;

private:
    void BuildMetaMapping();

    ErrorCode LoadIndexConfig(Helper::IniReader& p_reader);

    ErrorCode SaveIndexConfig(std::ostream& p_configOut);

protected:
    std::string m_sIndexName;
    std::string m_sMetadataFile = "metadata.bin";
    std::string m_sMetadataIndexFile = "metadataIndex.bin";
    std::shared_ptr<MetadataSet> m_pMetadata;
    std::unique_ptr<std::unordered_map<std::string, SizeType>> m_pMetaToVec;
};


} // namespace SPTAG

#endif // _SPTAG_VECTORINDEX_H_
