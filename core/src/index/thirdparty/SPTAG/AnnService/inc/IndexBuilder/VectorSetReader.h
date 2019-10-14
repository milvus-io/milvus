// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_INDEXBUILDER_VECTORSETREADER_H_
#define _SPTAG_INDEXBUILDER_VECTORSETREADER_H_

#include "inc/Core/Common.h"
#include "inc/Core/VectorSet.h"
#include "inc/Core/MetadataSet.h"
#include "Options.h"

#include <memory>

namespace SPTAG
{
namespace IndexBuilder
{

class VectorSetReader
{
public:
    VectorSetReader(std::shared_ptr<BuilderOptions> p_options);

    virtual ~VectorSetReader();

    virtual ErrorCode LoadFile(const std::string& p_filePath) = 0;

    virtual std::shared_ptr<VectorSet> GetVectorSet() const = 0;

    virtual std::shared_ptr<MetadataSet> GetMetadataSet() const = 0;

    static std::shared_ptr<VectorSetReader> CreateInstance(std::shared_ptr<BuilderOptions> p_options);

protected:
    std::shared_ptr<BuilderOptions> m_options;
};



} // namespace IndexBuilder
} // namespace SPTAG

#endif // _SPTAG_INDEXBUILDER_VECTORSETREADER_H_
