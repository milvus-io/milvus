// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_PW_COREINTERFACE_H_
#define _SPTAG_PW_COREINTERFACE_H_

#include "TransferDataType.h"
#include "inc/Core/Common.h"
#include "inc/Core/VectorIndex.h"

class AnnIndex
{
public:
    AnnIndex(int p_dimension);

    AnnIndex(const char* p_algoType, const char* p_valueType, int p_dimension);

    ~AnnIndex();

    void SetBuildParam(const char* p_name, const char* p_value);

    void SetSearchParam(const char* p_name, const char* p_value);

    bool Build(ByteArray p_data, int p_num);

    bool BuildWithMetaData(ByteArray p_data, ByteArray p_meta, int p_num);

    std::shared_ptr<QueryResult> Search(ByteArray p_data, int p_resultNum);

    std::shared_ptr<QueryResult> SearchWithMetaData(ByteArray p_data, int p_resultNum);

    bool ReadyToServe() const;

    bool Save(const char* p_saveFile) const;

    bool Add(ByteArray p_data, int p_num);

    bool AddWithMetaData(ByteArray p_data, ByteArray p_meta, int p_num);

    bool Delete(ByteArray p_data, int p_num);

    static AnnIndex Load(const char* p_loaderFile);

private:
    AnnIndex(const std::shared_ptr<SPTAG::VectorIndex>& p_index);
    
    std::shared_ptr<SPTAG::VectorIndex> m_index;

    size_t m_inputVectorSize;
    
    int m_dimension;

    SPTAG::IndexAlgoType m_algoType;

    SPTAG::VectorValueType m_inputValueType;
};

#endif // _SPTAG_PW_COREINTERFACE_H_
