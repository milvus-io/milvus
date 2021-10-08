// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Helper/VectorSetReaders/DefaultReader.h"
#include "inc/Helper/StringConvert.h"
#include "inc/Helper/CommonHelper.h"

#include <fstream>
#include <sstream>
#include <iostream>
#include <omp.h>

using namespace SPTAG;
using namespace SPTAG::Helper;

namespace
{
namespace Local
{

class BinaryLineReader
{
public:
    BinaryLineReader(std::istream& p_inStream)
        : m_inStream(p_inStream)
    {
        m_buffer.reset(new char[c_bufferSize]);
    }


    bool Eof()
    {
        return m_inStream.eof() && (m_curOffset == m_curTotal);
    }


    std::size_t GetLine(std::unique_ptr<char[]>& p_buffer, std::size_t& p_bufferSize, std::size_t& p_length)
    {
        std::size_t consumedCount = 0;
        p_length = 0;
        while (true)
        {
            while (m_curOffset < m_curTotal)
            {
                if (p_bufferSize > p_length)
                {
                    ++consumedCount;
                    if (!IsDelimiter(m_buffer[m_curOffset]))
                    {
                        p_buffer[p_length++] = m_buffer[m_curOffset++];
                    }
                    else
                    {
                        ++m_curOffset;
                        p_buffer[p_length] = '\0';
                        return consumedCount + MoveToNextValid();
                    }
                }
                else
                {
                    p_bufferSize *= 2;
                    std::unique_ptr<char[]> newBuffer(new char[p_bufferSize]);
                    memcpy(newBuffer.get(), p_buffer.get(), p_length);
                    p_buffer.swap(newBuffer);
                }
            }

            if (m_inStream.eof())
            {
                break;
            }

            m_inStream.read(m_buffer.get(), c_bufferSize);
            m_curTotal = m_inStream.gcount();
            m_curOffset = 0;
        }

        if (p_bufferSize <= p_length)
        {
            p_bufferSize *= 2;
            std::unique_ptr<char[]> newBuffer(new char[p_bufferSize]);
            memcpy(newBuffer.get(), p_buffer.get(), p_length);
            p_buffer.swap(newBuffer);
        }

        p_buffer[p_length] = '\0';
        return consumedCount;
    }


private:
    std::size_t MoveToNextValid()
    {
        std::size_t skipped = 0;
        while (true)
        {
            while (m_curOffset < m_curTotal)
            {
                if (IsDelimiter(m_buffer[m_curOffset]))
                {
                    ++skipped;
                    ++m_curOffset;
                }
                else
                {
                    return skipped;
                }
            }

            if (m_inStream.eof())
            {
                break;
            }

            m_inStream.read(m_buffer.get(), c_bufferSize);
            m_curTotal = m_inStream.gcount();
            m_curOffset = 0;
        }

        return skipped;
    }

    bool IsDelimiter(char p_ch)
    {
        return p_ch == '\r' || p_ch == '\n';
    }

    static const std::size_t c_bufferSize = 1 << 10;

    std::unique_ptr<char[]> m_buffer;

    std::istream& m_inStream;

    std::size_t m_curOffset;

    std::size_t m_curTotal;
};

} // namespace Local
} // namespace


DefaultReader::DefaultReader(std::shared_ptr<ReaderOptions> p_options)
    : VectorSetReader(std::move(p_options)),
    m_subTaskBlocksize(0)
{
    omp_set_num_threads(m_options->m_threadNum);

    std::string tempFolder("tempfolder");
    if (!direxists(tempFolder.c_str()))
    {
        mkdir(tempFolder.c_str());
    }

    tempFolder += FolderSep;
    m_vectorOutput = tempFolder + "vectorset.bin";
    m_metadataConentOutput = tempFolder + "metadata.bin";
    m_metadataIndexOutput = tempFolder + "metadataindex.bin";
}


DefaultReader::~DefaultReader()
{
    if (fileexists(m_vectorOutput.c_str()))
    {
        remove(m_vectorOutput.c_str());
    }

    if (fileexists(m_metadataIndexOutput.c_str()))
    {
        remove(m_metadataIndexOutput.c_str());
    }

    if (fileexists(m_metadataConentOutput.c_str()))
    {
        remove(m_metadataConentOutput.c_str());
    }
}


ErrorCode
DefaultReader::LoadFile(const std::string& p_filePaths)
{
    const auto& files = GetFileSizes(p_filePaths);
    std::vector<std::function<void()>> subWorks;
    subWorks.reserve(files.size() * m_options->m_threadNum);

    m_subTaskCount = 0;
    for (const auto& fileInfo : files)
    {
        if (fileInfo.second == (std::numeric_limits<std::size_t>::max)())
        {
            std::stringstream msg;
            msg << "File " << fileInfo.first << " not exists or can't access.";
            std::cerr << msg.str() << std::endl;
            exit(1);
        }

        std::uint32_t fileTaskCount = 0;
        std::size_t blockSize = m_subTaskBlocksize;
        if (0 == blockSize)
        {
            fileTaskCount = m_options->m_threadNum;
            blockSize = (fileInfo.second + fileTaskCount - 1) / fileTaskCount;
        }
        else
        {
            fileTaskCount = static_cast<std::uint32_t>((fileInfo.second + blockSize - 1) / blockSize);
        }

        for (std::uint32_t i = 0; i < fileTaskCount; ++i)
        {
            subWorks.emplace_back(std::bind(&DefaultReader::LoadFileInternal,
                                            this,
                                            fileInfo.first,
                                            m_subTaskCount++,
                                            i,
                                            blockSize));
        }
    }

    m_totalRecordCount = 0;
    m_totalRecordVectorBytes = 0;
    m_subTaskRecordCount.clear();
    m_subTaskRecordCount.resize(m_subTaskCount, 0);

    m_waitSignal.Reset(m_subTaskCount);

#pragma omp parallel for schedule(dynamic)
    for (int64_t i = 0; i < (int64_t)subWorks.size(); i++)
    {
        subWorks[i]();
    }

    m_waitSignal.Wait();

    MergeData();

    return ErrorCode::Success;
}


std::shared_ptr<VectorSet>
DefaultReader::GetVectorSet() const
{
    ByteArray vectorSet = ByteArray::Alloc(m_totalRecordVectorBytes);
    char* vecBuf = reinterpret_cast<char*>(vectorSet.Data());

    std::ifstream inputStream;
    inputStream.open(m_vectorOutput, std::ifstream::binary);
    inputStream.seekg(sizeof(SizeType) + sizeof(DimensionType), std::ifstream::beg);
    inputStream.read(vecBuf, m_totalRecordVectorBytes);
    inputStream.close();

    return std::shared_ptr<VectorSet>(new BasicVectorSet(vectorSet,
                                                         m_options->m_inputValueType,
                                                         m_options->m_dimension,
                                                         m_totalRecordCount));
}


std::shared_ptr<MetadataSet>
DefaultReader::GetMetadataSet() const
{
    return std::shared_ptr<MetadataSet>(new FileMetadataSet(m_metadataConentOutput, m_metadataIndexOutput));
}


void
DefaultReader::LoadFileInternal(const std::string& p_filePath,
                                std::uint32_t p_subTaskID,
                                std::uint32_t p_fileBlockID,
                                std::size_t p_fileBlockSize)
{
    std::size_t lineBufferSize = 1 << 16;
    std::unique_ptr<char[]> currentLine(new char[lineBufferSize]);

    std::ifstream inputStream;
    std::ofstream outputStream;
    std::ofstream metaStreamContent;
    std::ofstream metaStreamIndex;

    SizeType recordCount = 0;
    std::uint64_t metaOffset = 0;
    std::size_t totalRead = 0;
    std::streamoff startpos = p_fileBlockID * p_fileBlockSize;

    inputStream.open(p_filePath, std::ios_base::in | std::ios_base::binary);
    if (inputStream.is_open() == false)
    {
        std::stringstream msg;
        msg << "Unable to open file: " << p_filePath << std::endl;
        const auto& msgStr = msg.str();
        std::cerr << msgStr;
        throw MyException(msgStr);
        exit(1);
    }

    {
        std::stringstream msg;
        msg << "Begin Subtask: " << p_subTaskID << ", start offset position:" << startpos << std::endl;
        std::cout << msg.str();
    }

    std::string subFileSuffix("_");
    subFileSuffix += std::to_string(p_subTaskID);
    subFileSuffix += ".tmp";

    outputStream.open(m_vectorOutput + subFileSuffix, std::ofstream::binary);
    metaStreamContent.open(m_metadataConentOutput + subFileSuffix, std::ofstream::binary);
    metaStreamIndex.open(m_metadataIndexOutput + subFileSuffix, std::ofstream::binary);

    inputStream.seekg(startpos, std::ifstream::beg);

    Local::BinaryLineReader lineReader(inputStream);

    std::size_t lineLength;
    if (p_fileBlockID != 0)
    {
        totalRead += lineReader.GetLine(currentLine, lineBufferSize, lineLength);
    }

    std::size_t vectorByteSize = GetValueTypeSize(m_options->m_inputValueType) * m_options->m_dimension;
    std::unique_ptr<std::uint8_t[]> vector;
    vector.reset(new std::uint8_t[vectorByteSize]);

    while (!lineReader.Eof() && totalRead <= p_fileBlockSize)
    {
        totalRead += lineReader.GetLine(currentLine, lineBufferSize, lineLength);
        if (0 == lineLength)
        {
            continue;
        }

        std::size_t tabIndex = lineLength - 1;
        while (tabIndex > 0 && currentLine[tabIndex] != '\t')
        {
            --tabIndex;
        }

        if (0 == tabIndex && currentLine[tabIndex] != '\t')
        {
            std::stringstream msg;
            msg << "Subtask: " << p_subTaskID << " cannot parsing line:" << currentLine.get() << std::endl;
            std::cout << msg.str();
            exit(1);
        }

        bool parseSuccess = false;
        switch (m_options->m_inputValueType)
        {
#define DefineVectorValueType(Name, Type) \
        case VectorValueType::Name: \
            parseSuccess = TranslateVector(currentLine.get() + tabIndex + 1, reinterpret_cast<Type*>(vector.get())); \
            break; \

#include "inc/Core/DefinitionList.h"
#undef DefineVectorValueType

        default:
            parseSuccess = false;
            break;
        }

        if (!parseSuccess)
        {
            std::stringstream msg;
            msg << "Subtask: " << p_subTaskID << " cannot parsing vector:" << (currentLine.get() + tabIndex + 1) << std::endl;
            std::cout << msg.str();
            exit(1);
        }

        ++recordCount;
        outputStream.write(reinterpret_cast<const char*>(vector.get()), vectorByteSize);
        metaStreamContent.write(currentLine.get(), tabIndex);
        metaStreamIndex.write(reinterpret_cast<const char*>(&metaOffset), sizeof(metaOffset));

        metaOffset += tabIndex;
    }

    metaStreamIndex.write(reinterpret_cast<const char*>(&metaOffset), sizeof(metaOffset));

    inputStream.close();
    outputStream.close();
    metaStreamContent.close();
    metaStreamIndex.close();

    m_totalRecordCount += recordCount;
    m_subTaskRecordCount[p_subTaskID] = recordCount;
    m_totalRecordVectorBytes += recordCount * vectorByteSize;

    m_waitSignal.FinishOne();
}


void
DefaultReader::MergeData()
{
    const std::size_t bufferSize = 1 << 30;
    const std::size_t bufferSizeTrim64 = (bufferSize / sizeof(std::uint64_t)) * sizeof(std::uint64_t);
    std::ifstream inputStream;
    std::ofstream outputStream;

    std::unique_ptr<char[]> bufferHolder(new char[bufferSize]);
    char* buf = bufferHolder.get();

    SizeType totalRecordCount = m_totalRecordCount;

    outputStream.open(m_vectorOutput, std::ofstream::binary);

    outputStream.write(reinterpret_cast<char*>(&totalRecordCount), sizeof(totalRecordCount));
    outputStream.write(reinterpret_cast<char*>(&(m_options->m_dimension)), sizeof(m_options->m_dimension));

    for (std::uint32_t i = 0; i < m_subTaskCount; ++i)
    {
        std::string file = m_vectorOutput;
        file += "_";
        file += std::to_string(i);
        file += ".tmp";

        inputStream.open(file, std::ifstream::binary);
        outputStream << inputStream.rdbuf();

        inputStream.close();
        remove(file.c_str());
    }

    outputStream.close();

    outputStream.open(m_metadataConentOutput, std::ofstream::binary);
    for (std::uint32_t i = 0; i < m_subTaskCount; ++i)
    {
        std::string file = m_metadataConentOutput;
        file += "_";
        file += std::to_string(i);
        file += ".tmp";

        inputStream.open(file, std::ifstream::binary);
        outputStream << inputStream.rdbuf();

        inputStream.close();
        remove(file.c_str());
    }

    outputStream.close();

    outputStream.open(m_metadataIndexOutput, std::ofstream::binary);

    outputStream.write(reinterpret_cast<char*>(&totalRecordCount), sizeof(totalRecordCount));

    std::uint64_t totalOffset = 0;
    for (std::uint32_t i = 0; i < m_subTaskCount; ++i)
    {
        std::string file = m_metadataIndexOutput;
        file += "_";
        file += std::to_string(i);
        file += ".tmp";

        inputStream.open(file, std::ifstream::binary);
        for (SizeType remains = m_subTaskRecordCount[i]; remains > 0;)
        {
            std::size_t readBytesCount = min(remains * sizeof(std::uint64_t), bufferSizeTrim64);
            inputStream.read(buf, readBytesCount);
            std::uint64_t* offset = reinterpret_cast<std::uint64_t*>(buf);
            for (std::uint64_t i = 0; i < readBytesCount / sizeof(std::uint64_t); ++i)
            {
                offset[i] += totalOffset;
            }

            outputStream.write(buf, readBytesCount);
            remains -= static_cast<SizeType>(readBytesCount / sizeof(std::uint64_t));
        }

        inputStream.read(buf, sizeof(std::uint64_t));
        totalOffset += *(reinterpret_cast<std::uint64_t*>(buf));

        inputStream.close();
        remove(file.c_str());
    }

    outputStream.write(reinterpret_cast<char*>(&totalOffset), sizeof(totalOffset));
    outputStream.close();
}


std::vector<DefaultReader::FileInfoPair>
DefaultReader::GetFileSizes(const std::string& p_filePaths)
{
    const auto& files = Helper::StrUtils::SplitString(p_filePaths, ",");
    std::vector<DefaultReader::FileInfoPair> res;
    res.reserve(files.size());

    for (const auto& filePath : files)
    {
        if (!fileexists(filePath.c_str()))
        {
            res.emplace_back(filePath, (std::numeric_limits<std::size_t>::max)());
            continue;
        }
#ifndef _MSC_VER
        struct stat stat_buf;
        stat(filePath.c_str(), &stat_buf);
#else
        struct _stat64 stat_buf;
        _stat64(filePath.c_str(), &stat_buf);
#endif
        std::size_t fileSize = stat_buf.st_size;
        res.emplace_back(filePath, static_cast<std::size_t>(fileSize));
    }

    return res;
}


