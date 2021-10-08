// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_HELPER_VECTORSETREADER_H_
#define _SPTAG_HELPER_VECTORSETREADER_H_

#include "inc/Core/Common.h"
#include "inc/Core/VectorSet.h"
#include "inc/Core/MetadataSet.h"
#include "inc/Helper/ArgumentsParser.h"

#include <memory>

namespace SPTAG
{
namespace Helper
{

class ReaderOptions : public ArgumentsParser
{
public:
    ReaderOptions(VectorValueType p_valueType, DimensionType p_dimension, std::string p_vectorDelimiter = "|", std::uint32_t p_threadNum = 32);

    ~ReaderOptions();

    std::uint32_t m_threadNum;

    DimensionType m_dimension;

    std::string m_vectorDelimiter;

    SPTAG::VectorValueType m_inputValueType;
};

class VectorSetReader
{
public:
    VectorSetReader(std::shared_ptr<ReaderOptions> p_options);

    virtual ~VectorSetReader();

    virtual ErrorCode LoadFile(const std::string& p_filePath) = 0;

    virtual std::shared_ptr<VectorSet> GetVectorSet() const = 0;

    virtual std::shared_ptr<MetadataSet> GetMetadataSet() const = 0;

    static std::shared_ptr<VectorSetReader> CreateInstance(std::shared_ptr<ReaderOptions> p_options);

protected:
    std::shared_ptr<ReaderOptions> m_options;
};



} // namespace Helper
} // namespace SPTAG

#endif // _SPTAG_HELPER_VECTORSETREADER_H_
