// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_COMMON_DATASET_H_
#define _SPTAG_COMMON_DATASET_H_

#include <fstream>

#if defined(_MSC_VER) || defined(__INTEL_COMPILER)
#include <malloc.h>
#else
#include <mm_malloc.h>
#endif // defined(__GNUC__)

#define ALIGN 32

#define aligned_malloc(a, b) _mm_malloc(a, b)
#define aligned_free(a) _mm_free(a)

#pragma warning(disable:4996)  // 'fopen': This function or variable may be unsafe. Consider using fopen_s instead. To disable deprecation, use _CRT_SECURE_NO_WARNINGS. See online help for details.

namespace SPTAG
{
    namespace COMMON
    {
        // structure to save Data and Graph
        template <typename T>
        class Dataset
        {
        private:
            int rows;
            int cols;
            bool ownData = false;
            T* data = nullptr;
            std::vector<T> dataIncremental;

        public:
            Dataset(): rows(0), cols(1) {}
            Dataset(int rows_, int cols_, T* data_ = nullptr, bool transferOnwership_ = true)
            {
                Initialize(rows_, cols_, data_, transferOnwership_);
            }
            ~Dataset()
            {
                if (ownData) aligned_free(data);
            }
            void Initialize(int rows_, int cols_, T* data_ = nullptr, bool transferOnwership_ = true)
            {
                rows = rows_;
                cols = cols_;
                data = data_;
                if (data_ == nullptr || !transferOnwership_)
                {
                    ownData = true;
                    data = (T*)aligned_malloc(sizeof(T) * rows * cols, ALIGN);
                    if (data_ != nullptr) memcpy(data, data_, rows * cols * sizeof(T));
                    else std::memset(data, -1, rows * cols * sizeof(T));
                }
            }
            void SetR(int R_)
            {
                if (R_ >= rows)
                    dataIncremental.resize((R_ - rows) * cols);
                else
                {
                    rows = R_;
                    dataIncremental.clear();
                }
            }
            inline int R() const { return (int)(rows + dataIncremental.size() / cols); }
            inline int C() const { return cols; }
            T* operator[](int index)
            {
                if (index >= rows) {
                    return dataIncremental.data() + (size_t)(index - rows)*cols;
                }
                return data + (size_t)index*cols;
            }

            const T* operator[](int index) const
            {
                if (index >= rows) {
                    return dataIncremental.data() + (size_t)(index - rows)*cols;
                }
                return data + (size_t)index*cols;
            }

            void AddBatch(const T* pData, int num)
            {
                dataIncremental.insert(dataIncremental.end(), pData, pData + num*cols);
            }

            void AddBatch(int num)
            {
                dataIncremental.insert(dataIncremental.end(), (size_t)num*cols, T(-1));
            }

            bool Save(std::string sDataPointsFileName)
            {
                std::cout << "Save Data To " << sDataPointsFileName << std::endl;
                FILE * fp = fopen(sDataPointsFileName.c_str(), "wb");
                if (fp == NULL) return false;

                int CR = R();
                fwrite(&CR, sizeof(int), 1, fp);
                fwrite(&cols, sizeof(int), 1, fp);

                T* ptr = data;
                int toWrite = rows;
                while (toWrite > 0)
                {
                    size_t write = fwrite(ptr, sizeof(T) * cols, toWrite, fp);
                    ptr += write * cols;
                    toWrite -= (int)write;
                }
                ptr = dataIncremental.data();
                toWrite = CR - rows;
                while (toWrite > 0)
                {
                    size_t write = fwrite(ptr, sizeof(T) * cols, toWrite, fp);
                    ptr += write * cols;
                    toWrite -= (int)write;
                }
                fclose(fp);

                std::cout << "Save Data (" << CR << ", " << cols << ") Finish!" << std::endl;
                return true;
            }

            bool Save(void **pDataPointsMemFile, int64_t &len)
            {
                size_t size = sizeof(int) + sizeof(int) + sizeof(T) * R() *cols;
                char *mem = (char*)malloc(size);
                if (mem == NULL) return false;

                int CR = R();

                auto header = (int*)mem;
                header[0] = CR;
                header[1] = cols;
                auto body = &mem[8];

                memcpy(body, data, sizeof(T) * cols * rows);
                body += sizeof(T) * cols * rows;
                memcpy(body, dataIncremental.data(), sizeof(T) * cols * (CR - rows));
                body += sizeof(T) * cols * (CR - rows);

                *pDataPointsMemFile = mem;
                len = size;

                return true;
            }

            bool Load(std::string sDataPointsFileName)
            {
                std::cout << "Load Data From " << sDataPointsFileName << std::endl;
                FILE * fp = fopen(sDataPointsFileName.c_str(), "rb");
                if (fp == NULL) return false;

                int R, C;
                fread(&R, sizeof(int), 1, fp);
                fread(&C, sizeof(int), 1, fp);

                Initialize(R, C);
                T* ptr = data;
                while (R > 0) {
                    size_t read = fread(ptr, sizeof(T) * C, R, fp);
                    ptr += read * C;
                    R -= (int)read;
                }
                fclose(fp);
                std::cout << "Load Data (" << rows << ", " << cols << ") Finish!" << std::endl;
                return true;
            }

            // Functions for loading models from memory mapped files
            bool Load(char* pDataPointsMemFile)
            {
                int R, C;
                R = *((int*)pDataPointsMemFile);
                pDataPointsMemFile += sizeof(int);

                C = *((int*)pDataPointsMemFile);
                pDataPointsMemFile += sizeof(int);

                Initialize(R, C, (T*)pDataPointsMemFile);
                return true;
            }

            bool Refine(const std::vector<int>& indices, std::string sDataPointsFileName)
            {
                std::cout << "Save Refine Data To " << sDataPointsFileName << std::endl;
                FILE * fp = fopen(sDataPointsFileName.c_str(), "wb");
                if (fp == NULL) return false;

                int R = (int)(indices.size());
                fwrite(&R, sizeof(int), 1, fp);
                fwrite(&cols, sizeof(int), 1, fp);

                // write point one by one in case for cache miss
                for (int i = 0; i < R; i++) {
                    if (indices[i] < rows)
                        fwrite(data + (size_t)indices[i] * cols, sizeof(T) * cols, 1, fp);
                    else
                        fwrite(dataIncremental.data() + (size_t)(indices[i] - rows) * cols, sizeof(T) * cols, 1, fp);
                }
                fclose(fp);

                std::cout << "Save Refine Data (" << R << ", " << cols << ") Finish!" << std::endl;
                return true;
            }
        };
    }
}

#endif // _SPTAG_COMMON_DATASET_H_
