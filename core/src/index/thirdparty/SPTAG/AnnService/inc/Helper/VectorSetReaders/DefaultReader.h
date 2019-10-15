// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_HELPER_VECTORSETREADERS_DEFAULTREADER_H_
#define _SPTAG_HELPER_VECTORSETREADERS_DEFAULTREADER_H_

#include "../VectorSetReader.h"
#include "inc/Helper/Concurrent.h"

#include <atomic>
#include <condition_variable>
#include <mutex>

namespace SPTAG
{
namespace Helper
{

class DefaultReader : public VectorSetReader
{
public:
    DefaultReader(std::shared_ptr<ReaderOptions> p_options);

    virtual ~DefaultReader();

    virtual ErrorCode LoadFile(const std::string& p_filePaths);

    virtual std::shared_ptr<VectorSet> GetVectorSet() const;

    virtual std::shared_ptr<MetadataSet> GetMetadataSet() const;

private:
    typedef std::pair<std::string, std::size_t> FileInfoPair;

    static std::vector<FileInfoPair> GetFileSizes(const std::string& p_filePaths);

    void LoadFileInternal(const std::string& p_filePath,
                          std::uint32_t p_subtaskID,
                          std::uint32_t p_fileBlockID,
                          std::size_t p_fileBlockSize);

    void MergeData();

    template<typename DataType>
    bool TranslateVector(char* p_str, DataType* p_vector)
    {
        DimensionType eleCount = 0;
        char* next = p_str;
        while ((*next) != '\0')
        {
            while ((*next) != '\0' && m_options->m_vectorDelimiter.find(*next) == std::string::npos)
            {
                ++next;
            }

            bool reachEnd = ('\0' == (*next));
            *next = '\0';
            if (p_str != next)
            {
                if (eleCount >= m_options->m_dimension)
                {
                    return false;
                }

                if (!Helper::Convert::ConvertStringTo(p_str, p_vector[eleCount++]))
                {
                    return false;
                }
            }

            if (reachEnd)
            {
                break;
            }

            ++next;
            p_str = next;
        }

        return eleCount == m_options->m_dimension;
    }

private:
    std::uint32_t m_subTaskCount;

    std::size_t m_subTaskBlocksize;

    std::atomic<SizeType> m_totalRecordCount;

    std::atomic<std::size_t> m_totalRecordVectorBytes;

    std::vector<SizeType> m_subTaskRecordCount;

    std::string m_vectorOutput;

    std::string m_metadataConentOutput;

    std::string m_metadataIndexOutput;

    Helper::Concurrent::WaitSignal m_waitSignal;
};



} // namespace Helper
} // namespace SPTAG

#endif // _SPTAG_HELPER_VECTORSETREADERS_DEFAULT_H_
