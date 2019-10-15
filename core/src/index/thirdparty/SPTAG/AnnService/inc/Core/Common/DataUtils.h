// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_COMMON_DATAUTILS_H_
#define _SPTAG_COMMON_DATAUTILS_H_

#include <sys/stat.h>
#include <atomic>
#include "CommonUtils.h"
#include "../../Helper/CommonHelper.h"

namespace SPTAG
{
    namespace COMMON
    {
        const int bufsize = 1 << 30;

        class DataUtils {
        public:
            static bool MergeIndex(const std::string& p_vectorfile1, const std::string& p_metafile1, const std::string& p_metaindexfile1,
                const std::string& p_vectorfile2, const std::string& p_metafile2, const std::string& p_metaindexfile2) {
                std::ifstream inputStream1, inputStream2;
                std::ofstream outputStream;
                std::unique_ptr<char[]> bufferHolder(new char[bufsize]);
                char * buf = bufferHolder.get();
                SizeType R1, R2;
                DimensionType C1, C2;

#define MergeVector(inputStream, vectorFile, R, C) \
                inputStream.open(vectorFile, std::ifstream::binary); \
                if (!inputStream.is_open()) { \
                    std::cout << "Cannot open vector file: " << vectorFile <<"!" << std::endl; \
                    return false; \
                } \
                inputStream.read((char *)&(R), sizeof(SizeType)); \
                inputStream.read((char *)&(C), sizeof(DimensionType)); \

                MergeVector(inputStream1, p_vectorfile1, R1, C1)
                MergeVector(inputStream2, p_vectorfile2, R2, C2)
#undef MergeVector               
                if (C1 != C2) {
                    inputStream1.close(); inputStream2.close();
                    std::cout << "Vector dimensions are not the same!" << std::endl;
                    return false;
                }
                R1 += R2;
                outputStream.open(p_vectorfile1 + "_tmp", std::ofstream::binary);
                outputStream.write((char *)&R1, sizeof(SizeType));
                outputStream.write((char *)&C1, sizeof(DimensionType));
                while (!inputStream1.eof()) {
                    inputStream1.read(buf, bufsize);
                    outputStream.write(buf, inputStream1.gcount());
                }
                while (!inputStream2.eof()) {
                    inputStream2.read(buf, bufsize);
                    outputStream.write(buf, inputStream2.gcount());
                }
                inputStream1.close(); inputStream2.close();
                outputStream.close();

                if (p_metafile1 != "" && p_metafile2 != "") {
                    outputStream.open(p_metafile1 + "_tmp", std::ofstream::binary);
#define MergeMeta(inputStream, metaFile) \
                    inputStream.open(metaFile, std::ifstream::binary); \
                    if (!inputStream.is_open()) { \
                        std::cout << "Cannot open meta file: " << metaFile << "!" << std::endl; \
                        return false; \
                    } \
                    while (!inputStream.eof()) { \
                        inputStream.read(buf, bufsize); \
                        outputStream.write(buf, inputStream.gcount()); \
                    } \
                    inputStream.close(); \

                    MergeMeta(inputStream1, p_metafile1)
                    MergeMeta(inputStream2, p_metafile2)
#undef MergeMeta
                    outputStream.close();
                    delete[] buf;

                    std::uint64_t * offsets = reinterpret_cast<std::uint64_t*>(buf);
                    std::uint64_t lastoff = 0;
                    outputStream.open(p_metaindexfile1 + "_tmp", std::ofstream::binary);
                    outputStream.write((char *)&R1, sizeof(SizeType));
#define MergeMetaIndex(inputStream, metaIndexFile) \
                    inputStream.open(metaIndexFile, std::ifstream::binary); \
                    if (!inputStream.is_open()) { \
                        std::cout << "Cannot open meta index file: " << metaIndexFile << "!" << std::endl; \
                        return false; \
                    } \
                    inputStream.read((char *)&R2, sizeof(SizeType)); \
                    inputStream.read((char *)offsets, sizeof(std::uint64_t)*(R2 + 1)); \
                    inputStream.close(); \
                    for (SizeType j = 0; j < R2 + 1; j++) offsets[j] += lastoff; \
                    outputStream.write((char *)offsets, sizeof(std::uint64_t)*R2); \
                    lastoff = offsets[R2]; \

                    MergeMetaIndex(inputStream1, p_metaindexfile1)
                    MergeMetaIndex(inputStream2, p_metaindexfile2)
#undef MergeMetaIndex
                    outputStream.write((char *)&lastoff, sizeof(std::uint64_t));
                    outputStream.close();
                    
                    rename((p_metafile1 + "_tmp").c_str(), p_metafile1.c_str());
                    rename((p_metaindexfile1 + "_tmp").c_str(), p_metaindexfile1.c_str());
                }
                rename((p_vectorfile1 + "_tmp").c_str(), p_vectorfile1.c_str());
                
                std::cout << "Merged -> numSamples:" << R1 << " D:" << C1 << std::endl;
                return true;
            }
        };
    }
}

#endif // _SPTAG_COMMON_DATAUTILS_H_
