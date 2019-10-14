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
        const int bufsize = 1024 * 1024 * 1024;

        class DataUtils {
        public:
            template <typename T>
            static void ProcessTSVData(int id, int threadbase, std::uint64_t blocksize,
                std::string filename, std::string outfile, std::string outmetafile, std::string outmetaindexfile,
                std::atomic_int& numSamples, int& D, DistCalcMethod distCalcMethod) {
                std::ifstream inputStream(filename);
                if (!inputStream.is_open()) {
                    std::cerr << "unable to open file " + filename << std::endl;
                    throw MyException("unable to open file " + filename);
                    exit(1);
                }
                std::ofstream outputStream, metaStream_out, metaStream_index;
                outputStream.open(outfile + std::to_string(id + threadbase), std::ofstream::binary);
                metaStream_out.open(outmetafile + std::to_string(id + threadbase), std::ofstream::binary);
                metaStream_index.open(outmetaindexfile + std::to_string(id + threadbase), std::ofstream::binary);
                if (!outputStream.is_open() || !metaStream_out.is_open() || !metaStream_index.is_open()) {
                    std::cerr << "unable to open output file " << outfile << " " << outmetafile << " " << outmetaindexfile << std::endl;
                    throw MyException("unable to open output files");
                    exit(1);
                }

                std::vector<float> arr;
                std::vector<T> sample;

                int base = 1;
                if (distCalcMethod == DistCalcMethod::Cosine) {
                    base = Utils::GetBase<T>();
                }
                std::uint64_t writepos = 0;
                int sampleSize = 0;
                std::uint64_t totalread = 0;
                std::streamoff startpos = id * blocksize;

#ifndef _MSC_VER
                int enter_size = 1;
#else
                int enter_size = 1;
#endif
                std::string currentLine;
                size_t index;
                inputStream.seekg(startpos, std::ifstream::beg);
                if (id != 0) {
                    std::getline(inputStream, currentLine);
                    totalread += currentLine.length() + enter_size;
                }
                std::cout << "Begin thread " << id << " begin at:" << (startpos + totalread) << std::endl;
                while (!inputStream.eof() && totalread <= blocksize) {
                    std::getline(inputStream, currentLine);
                    if (currentLine.length() <= enter_size || (index = Utils::ProcessLine(currentLine, arr, D, base, distCalcMethod)) < 0) {
                        totalread += currentLine.length() + enter_size;
                        continue;
                    }
                    sample.resize(D);
                    for (int j = 0; j < D; j++) sample[j] = (T)arr[j];

                    outputStream.write((char *)(sample.data()), sizeof(T)*D);
                    metaStream_index.write((char *)&writepos, sizeof(std::uint64_t));
                    metaStream_out.write(currentLine.c_str(), index);

                    writepos += index;
                    sampleSize += 1;
                    totalread += currentLine.length() + enter_size;
                }
                metaStream_index.write((char *)&writepos, sizeof(std::uint64_t));
                metaStream_index.write((char *)&sampleSize, sizeof(int));
                inputStream.close();
                outputStream.close();
                metaStream_out.close();
                metaStream_index.close();

                numSamples.fetch_add(sampleSize);

                std::cout << "Finish Thread[" << id << ", " << sampleSize << "] at:" << (startpos + totalread) << std::endl;
            }

            static void MergeData(int threadbase, std::string outfile, std::string outmetafile, std::string outmetaindexfile,
                std::atomic_int& numSamples, int D) {
                std::ifstream inputStream;
                std::ofstream outputStream;
                char * buf = new char[bufsize];
                std::uint64_t * offsets;
                int partSamples;
                int metaSamples = 0;
                std::uint64_t lastoff = 0;

                outputStream.open(outfile, std::ofstream::binary);
                outputStream.write((char *)&numSamples, sizeof(int));
                outputStream.write((char *)&D, sizeof(int));
                for (int i = 0; i < threadbase; i++) {
                    std::string file = outfile + std::to_string(i);
                    inputStream.open(file, std::ifstream::binary);
                    while (!inputStream.eof()) {
                        inputStream.read(buf, bufsize);
                        outputStream.write(buf, inputStream.gcount());
                    }
                    inputStream.close();
                    remove(file.c_str());
                }
                outputStream.close();

                outputStream.open(outmetafile, std::ofstream::binary);
                for (int i = 0; i < threadbase; i++) {
                    std::string file = outmetafile + std::to_string(i);
                    inputStream.open(file, std::ifstream::binary);
                    while (!inputStream.eof()) {
                        inputStream.read(buf, bufsize);
                        outputStream.write(buf, inputStream.gcount());
                    }
                    inputStream.close();
                    remove(file.c_str());
                }
                outputStream.close();
                delete[] buf;

                outputStream.open(outmetaindexfile, std::ofstream::binary);
                outputStream.write((char *)&numSamples, sizeof(int));
                for (int i = 0; i < threadbase; i++) {
                    std::string file = outmetaindexfile + std::to_string(i);
                    inputStream.open(file, std::ifstream::binary);

                    inputStream.seekg(-((long long)sizeof(int)), inputStream.end);
                    inputStream.read((char *)&partSamples, sizeof(int));
                    offsets = new std::uint64_t[partSamples + 1];

                    inputStream.seekg(0, inputStream.beg);
                    inputStream.read((char *)offsets, sizeof(std::uint64_t)*(partSamples + 1));
                    inputStream.close();
                    remove(file.c_str());

                    for (int j = 0; j < partSamples + 1; j++)
                        offsets[j] += lastoff;
                    outputStream.write((char *)offsets, sizeof(std::uint64_t)*partSamples);

                    lastoff = offsets[partSamples];
                    metaSamples += partSamples;
                    delete[] offsets;
                }
                outputStream.write((char *)&lastoff, sizeof(std::uint64_t));
                outputStream.close();

                std::cout << "numSamples:" << numSamples << " metaSamples:" << metaSamples << " D:" << D << std::endl;
            }

            static bool MergeIndex(const std::string& p_vectorfile1, const std::string& p_metafile1, const std::string& p_metaindexfile1,
                const std::string& p_vectorfile2, const std::string& p_metafile2, const std::string& p_metaindexfile2) {
                std::ifstream inputStream1, inputStream2;
                std::ofstream outputStream;
                char * buf = new char[bufsize];
                int R1, R2, C1, C2;

#define MergeVector(inputStream, vectorFile, R, C) \
                inputStream.open(vectorFile, std::ifstream::binary); \
                if (!inputStream.is_open()) { \
                    std::cout << "Cannot open vector file: " << vectorFile <<"!" << std::endl; \
                    return false; \
                } \
                inputStream.read((char *)&(R), sizeof(int)); \
                inputStream.read((char *)&(C), sizeof(int)); \

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
                outputStream.write((char *)&R1, sizeof(int));
                outputStream.write((char *)&C1, sizeof(int));
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


                    std::uint64_t * offsets;
                    int partSamples;
                    std::uint64_t lastoff = 0;
                    outputStream.open(p_metaindexfile1 + "_tmp", std::ofstream::binary);
                    outputStream.write((char *)&R1, sizeof(int));
#define MergeMetaIndex(inputStream, metaIndexFile) \
                    inputStream.open(metaIndexFile, std::ifstream::binary); \
                    if (!inputStream.is_open()) { \
                        std::cout << "Cannot open meta index file: " << metaIndexFile << "!" << std::endl; \
                        return false; \
                    } \
                    inputStream.read((char *)&partSamples, sizeof(int)); \
                    offsets = new std::uint64_t[partSamples + 1]; \
                    inputStream.read((char *)offsets, sizeof(std::uint64_t)*(partSamples + 1)); \
                    inputStream.close(); \
                    for (int j = 0; j < partSamples + 1; j++) offsets[j] += lastoff; \
                    outputStream.write((char *)offsets, sizeof(std::uint64_t)*partSamples); \
                    lastoff = offsets[partSamples]; \
                    delete[] offsets; \

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

            template <typename T>
            static void ParseData(std::string filenames, std::string outfile, std::string outmetafile, std::string outmetaindexfile,
                int threadnum, DistCalcMethod distCalcMethod) {
                omp_set_num_threads(threadnum);

                std::atomic_int numSamples = { 0 };
                int D = -1;

                int threadbase = 0;
                std::vector<std::string> inputFileNames = Helper::StrUtils::SplitString(filenames, ",");
                for (std::string inputFileName : inputFileNames)
                {
#ifndef _MSC_VER
                    struct stat stat_buf;
                    stat(inputFileName.c_str(), &stat_buf);
#else
                    struct _stat64 stat_buf;
                    int res = _stat64(inputFileName.c_str(), &stat_buf);
#endif
                    std::uint64_t blocksize = (stat_buf.st_size + threadnum - 1) / threadnum;

#pragma omp parallel for
                    for (int i = 0; i < threadnum; i++) {
                        ProcessTSVData<T>(i, threadbase, blocksize, inputFileName, outfile, outmetafile, outmetaindexfile, numSamples, D, distCalcMethod);
                    }
                    threadbase += threadnum;
                }
                MergeData(threadbase, outfile, outmetafile, outmetaindexfile, numSamples, D);
            }
        };
    }
}

#endif // _SPTAG_COMMON_DATAUTILS_H_
