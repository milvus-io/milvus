// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Helper/VectorSetReader.h"
#include "inc/Helper/VectorSetReaders/DefaultReader.h"


using namespace SPTAG;
using namespace SPTAG::Helper;


ReaderOptions::ReaderOptions(VectorValueType p_valueType, DimensionType p_dimension, std::string p_vectorDelimiter, std::uint32_t p_threadNum)
    : m_threadNum(p_threadNum), m_dimension(p_dimension), m_vectorDelimiter(p_vectorDelimiter), m_inputValueType(p_valueType)
{
    AddOptionalOption(m_threadNum, "-t", "--thread", "Thread Number.");
    AddOptionalOption(m_vectorDelimiter, "", "--delimiter", "Vector delimiter.");
    AddRequiredOption(m_dimension, "-d", "--dimension", "Dimension of vector.");
    AddRequiredOption(m_inputValueType, "-v", "--vectortype", "Input vector data type. Default is float.");
}


ReaderOptions::~ReaderOptions()
{
}


VectorSetReader::VectorSetReader(std::shared_ptr<ReaderOptions> p_options)
    : m_options(p_options)
{
}


VectorSetReader:: ~VectorSetReader()
{
}


std::shared_ptr<VectorSetReader>
VectorSetReader::CreateInstance(std::shared_ptr<ReaderOptions> p_options)
{
    return std::shared_ptr<VectorSetReader>(new DefaultReader(std::move(p_options)));
}


