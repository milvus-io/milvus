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
            std::string name = "Data";
            SizeType rows = 0;
            DimensionType cols = 1;
            bool ownData = false;
            T* data = nullptr;
            SizeType incRows = 0;
            std::vector<T*> incBlocks;
            static const SizeType rowsInBlock = 1024 * 1024;
        public:
            Dataset()
            {
                incBlocks.reserve(MaxSize / rowsInBlock + 1); 
            }
            Dataset(SizeType rows_, DimensionType cols_, T* data_ = nullptr, bool transferOnwership_ = true)
            {
                Initialize(rows_, cols_, data_, transferOnwership_);
                incBlocks.reserve(MaxSize / rowsInBlock + 1);
            }
            ~Dataset()
            {
                if (ownData) aligned_free(data);
                for (T* ptr : incBlocks) aligned_free(ptr);
                incBlocks.clear();
            }
            void Initialize(SizeType rows_, DimensionType cols_, T* data_ = nullptr, bool transferOnwership_ = true)
            {
                rows = rows_;
                cols = cols_;
                data = data_;
                if (data_ == nullptr || !transferOnwership_)
                {
                    ownData = true;
                    data = (T*)aligned_malloc(((size_t)rows) * cols * sizeof(T), ALIGN);
                    if (data_ != nullptr) memcpy(data, data_, ((size_t)rows) * cols * sizeof(T));
                    else std::memset(data, -1, ((size_t)rows) * cols * sizeof(T));
                }
            }
            void SetName(const std::string name_) { name = name_; }
            void SetR(SizeType R_) 
            {
                if (R_ >= rows)
                    incRows = R_ - rows;
                else 
                {
                    rows = R_;
                    incRows = 0;
                }
            }
            inline SizeType R() const { return rows + incRows; }
            inline DimensionType C() const { return cols; }
            inline std::uint64_t BufferSize() const { return sizeof(SizeType) + sizeof(DimensionType) + sizeof(T) * R() * C(); }

            inline const T* At(SizeType index) const
            {
                if (index >= rows) {
                    SizeType incIndex = index - rows;
                    return incBlocks[incIndex / rowsInBlock] + ((size_t)(incIndex % rowsInBlock)) * cols;
                }
                return data + ((size_t)index) * cols;
            }

            T* operator[](SizeType index)
            {
                return (T*)At(index);
            }
            
            const T* operator[](SizeType index) const
            {
                return At(index);
            }

            ErrorCode AddBatch(const T* pData, SizeType num)
            {
                if (R() > MaxSize - num) return ErrorCode::MemoryOverFlow;

                SizeType written = 0;
                while (written < num) {
                    SizeType curBlockIdx = (incRows + written) / rowsInBlock;
                    if (curBlockIdx >= (SizeType)incBlocks.size()) {
                        T* newBlock = (T*)aligned_malloc(((size_t)rowsInBlock) * cols * sizeof(T), ALIGN);
                        if (newBlock == nullptr) return ErrorCode::MemoryOverFlow;
                        incBlocks.push_back(newBlock);
                    }
                    SizeType curBlockPos = (incRows + written) % rowsInBlock;
                    SizeType toWrite = min(rowsInBlock - curBlockPos, num - written);
                    std::memcpy(incBlocks[curBlockIdx] + ((size_t)curBlockPos) * cols, pData + ((size_t)written) * cols, ((size_t)toWrite) * cols * sizeof(T));
                    written += toWrite;
                }
                incRows += written;
                return ErrorCode::Success;
            }

            ErrorCode AddBatch(SizeType num)
            {
                if (R() > MaxSize - num) return ErrorCode::MemoryOverFlow;

                SizeType written = 0;
                while (written < num) {
                    SizeType curBlockIdx = (incRows + written) / rowsInBlock;
                    if (curBlockIdx >= (SizeType)incBlocks.size()) {
                        T* newBlock = (T*)aligned_malloc(((size_t)rowsInBlock) * cols * sizeof(T), ALIGN);
                        if (newBlock == nullptr) return ErrorCode::MemoryOverFlow;
                        incBlocks.push_back(newBlock);
                    }
                    SizeType curBlockPos = (incRows + written) % rowsInBlock;
                    SizeType toWrite = min(rowsInBlock - curBlockPos, num - written);
                    std::memset(incBlocks[curBlockIdx] + ((size_t)curBlockPos) * cols, -1, ((size_t)toWrite) * cols * sizeof(T));
                    written += toWrite;
                }
                incRows += written;
                return ErrorCode::Success;
            }

            bool Save(std::ostream& p_outstream) const
            {
                SizeType CR = R();
                p_outstream.write((char*)&CR, sizeof(SizeType));
                p_outstream.write((char*)&cols, sizeof(DimensionType));
                p_outstream.write((char*)data, sizeof(T) * cols * rows);

                SizeType blocks = incRows / rowsInBlock;
                for (int i = 0; i < blocks; i++)
                    p_outstream.write((char*)incBlocks[i], sizeof(T) * cols * rowsInBlock);

                SizeType remain = incRows % rowsInBlock;
                if (remain > 0) p_outstream.write((char*)incBlocks[blocks], sizeof(T) * cols * remain);
                std::cout << "Save " << name << " (" << CR << ", " << cols << ") Finish!" << std::endl;
                return true;
            }

            bool Save(std::string sDataPointsFileName) const
            {
                std::cout << "Save " << name << " To " << sDataPointsFileName << std::endl;
                std::ofstream output(sDataPointsFileName, std::ios::binary);
                if (!output.is_open()) return false;
                Save(output);
                output.close();
                return true;
            }

            bool Load(std::string sDataPointsFileName)
            {
                std::cout << "Load " << name << " From " << sDataPointsFileName << std::endl;
                std::ifstream input(sDataPointsFileName, std::ios::binary);
                if (!input.is_open()) return false;

                input.read((char*)&rows, sizeof(SizeType));
                input.read((char*)&cols, sizeof(DimensionType));

                Initialize(rows, cols);
                input.read((char*)data, sizeof(T) * cols * rows);
                input.close();
                std::cout << "Load " << name << " (" << rows << ", " << cols << ") Finish!" << std::endl;
                return true;
            }

            // Functions for loading models from memory mapped files
            bool Load(char* pDataPointsMemFile)
            {
                SizeType R;
                DimensionType C;
                R = *((SizeType*)pDataPointsMemFile);
                pDataPointsMemFile += sizeof(SizeType);

                C = *((DimensionType*)pDataPointsMemFile);
                pDataPointsMemFile += sizeof(DimensionType);

                Initialize(R, C, (T*)pDataPointsMemFile, false);
                std::cout << "Load " << name << " (" << R << ", " << C << ") Finish!" << std::endl;
                return true;
            }

            bool Refine(const std::vector<SizeType>& indices, std::ostream& output)
            {
                SizeType R = (SizeType)(indices.size());
                output.write((char*)&R, sizeof(SizeType));
                output.write((char*)&cols, sizeof(DimensionType));

                for (SizeType i = 0; i < R; i++) {
                    output.write((char*)At(indices[i]), sizeof(T) * cols);
                }
                std::cout << "Save Refine " << name << " (" << R << ", " << cols << ") Finish!" << std::endl;
                return true;
            }

            bool Refine(const std::vector<SizeType>& indices, std::string sDataPointsFileName)
            {
                std::cout << "Save Refine " << name << " To " << sDataPointsFileName << std::endl;
                std::ofstream output(sDataPointsFileName, std::ios::binary);
                if (!output.is_open()) return false;
                Refine(indices, output);
                output.close();
                return true;
            }
        };
    }
}

#endif // _SPTAG_COMMON_DATASET_H_
