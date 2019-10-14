// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/IndexBuilder/VectorSetReader.h"
#include "inc/IndexBuilder/VectorSetReaders/DefaultReader.h"


using namespace SPTAG;
using namespace SPTAG::IndexBuilder;

VectorSetReader::VectorSetReader(std::shared_ptr<BuilderOptions> p_options)
    : m_options(p_options)
{
}


VectorSetReader:: ~VectorSetReader()
{
}


std::shared_ptr<VectorSetReader>
VectorSetReader::CreateInstance(std::shared_ptr<BuilderOptions> p_options)
{
    return std::shared_ptr<VectorSetReader>(new DefaultReader(std::move(p_options)));
}

