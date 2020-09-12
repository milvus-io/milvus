// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_COMMON_BKTREE_H_
#define _SPTAG_COMMON_BKTREE_H_

#include <iostream>
#include <stack>
#include <string>
#include <vector>

#include "../VectorIndex.h"

#include "CommonUtils.h"
#include "QueryResultSet.h"
#include "WorkSpace.h"

#pragma warning(disable:4996)  // 'fopen': This function or variable may be unsafe. Consider using fopen_s instead. To disable deprecation, use _CRT_SECURE_NO_WARNINGS. See online help for details.

namespace SPTAG
{
    namespace COMMON
    {
        // node type for storing BKT
        struct BKTNode
        {
            SizeType centerid;
            SizeType childStart;
            SizeType childEnd;

            BKTNode(SizeType cid = -1) : centerid(cid), childStart(-1), childEnd(-1) {}
        };

        template <typename T>
        struct KmeansArgs {
            int _K;
            DimensionType _D;
            int _T;
            T* centers;
            SizeType* counts;
            float* newCenters;
            SizeType* newCounts;
            int* label;
            SizeType* clusterIdx;
            float* clusterDist;
            T* newTCenters;

            KmeansArgs(int k, DimensionType dim, SizeType datasize, int threadnum) : _K(k), _D(dim), _T(threadnum) {
                centers = new T[k * dim];
                counts = new SizeType[k];
                newCenters = new float[threadnum * k * dim];
                newCounts = new SizeType[threadnum * k];
                label = new int[datasize];
                clusterIdx = new SizeType[threadnum * k];
                clusterDist = new float[threadnum * k];
                newTCenters = new T[k * dim];
            }

            ~KmeansArgs() {
                delete[] centers;
                delete[] counts;
                delete[] newCenters;
                delete[] newCounts;
                delete[] label;
                delete[] clusterIdx;
                delete[] clusterDist;
                delete[] newTCenters;
            }

            inline void ClearCounts() {
                memset(newCounts, 0, sizeof(SizeType) * _T * _K);
            }

            inline void ClearCenters() {
                memset(newCenters, 0, sizeof(float) * _T * _K * _D);
            }

            inline void ClearDists(float dist) {
                for (int i = 0; i < _T * _K; i++) {
                    clusterIdx[i] = -1;
                    clusterDist[i] = dist;
                }
            }

            void Shuffle(std::vector<SizeType>& indices, SizeType first, SizeType last) {
                SizeType* pos = new SizeType[_K];
                pos[0] = first;
                for (int k = 1; k < _K; k++) pos[k] = pos[k - 1] + newCounts[k - 1];

                for (int k = 0; k < _K; k++) {
                    if (newCounts[k] == 0) continue;
                    SizeType i = pos[k];
                    while (newCounts[k] > 0) {
                        SizeType swapid = pos[label[i]] + newCounts[label[i]] - 1;
                        newCounts[label[i]]--;
                        std::swap(indices[i], indices[swapid]);
                        std::swap(label[i], label[swapid]);
                    }
                    while (indices[i] != clusterIdx[k]) i++;
                    std::swap(indices[i], indices[pos[k] + counts[k] - 1]);
                }
                delete[] pos;
            }
        };

        class BKTree
        {
        public:
            BKTree(): m_iTreeNumber(1), m_iBKTKmeansK(32), m_iBKTLeafSize(8), m_iSamples(1000) {}
            
            BKTree(BKTree& other): m_iTreeNumber(other.m_iTreeNumber), 
                                   m_iBKTKmeansK(other.m_iBKTKmeansK), 
                                   m_iBKTLeafSize(other.m_iBKTLeafSize),
                                   m_iSamples(other.m_iSamples) {}
            ~BKTree() {}

            inline const BKTNode& operator[](SizeType index) const { return m_pTreeRoots[index]; }
            inline BKTNode& operator[](SizeType index) { return m_pTreeRoots[index]; }

            inline SizeType size() const { return (SizeType)m_pTreeRoots.size(); }

            inline const std::unordered_map<SizeType, SizeType>& GetSampleMap() const { return m_pSampleCenterMap; }

            template <typename T>
            void BuildTrees(VectorIndex* index, std::vector<SizeType>* indices = nullptr)
            {
                struct  BKTStackItem {
                    SizeType index, first, last;
                    BKTStackItem(SizeType index_, SizeType first_, SizeType last_) : index(index_), first(first_), last(last_) {}
                };
                std::stack<BKTStackItem> ss;

                std::vector<SizeType> localindices;
                if (indices == nullptr) {
                    localindices.resize(index->GetNumSamples());
                    for (SizeType i = 0; i < index->GetNumSamples(); i++) localindices[i] = i;
                }
                else {
                    localindices.assign(indices->begin(), indices->end());
                }
                KmeansArgs<T> args(m_iBKTKmeansK, index->GetFeatureDim(), (SizeType)localindices.size(), omp_get_num_threads());

                m_pSampleCenterMap.clear();
                for (char i = 0; i < m_iTreeNumber; i++)
                {
                    std::random_shuffle(localindices.begin(), localindices.end());

                    m_pTreeStart.push_back((SizeType)m_pTreeRoots.size());
                    m_pTreeRoots.push_back(BKTNode((SizeType)localindices.size()));
                    std::cout << "Start to build BKTree " << i + 1 << std::endl;

                    ss.push(BKTStackItem(m_pTreeStart[i], 0, (SizeType)localindices.size()));
                    while (!ss.empty()) {
                        BKTStackItem item = ss.top(); ss.pop();
                        SizeType newBKTid = (SizeType)m_pTreeRoots.size();
                        m_pTreeRoots[item.index].childStart = newBKTid;
                        if (item.last - item.first <= m_iBKTLeafSize) {
                            for (SizeType j = item.first; j < item.last; j++) {
                                m_pTreeRoots.push_back(BKTNode(localindices[j]));
                            }
                        }
                        else { // clustering the data into BKTKmeansK clusters
                            int numClusters = KmeansClustering(index, localindices, item.first, item.last, args);
                            if (numClusters <= 1) {
                                SizeType end = min(item.last + 1, (SizeType)localindices.size());
                                std::sort(localindices.begin() + item.first, localindices.begin() + end);
                                m_pTreeRoots[item.index].centerid = localindices[item.first];
                                m_pTreeRoots[item.index].childStart = -m_pTreeRoots[item.index].childStart;
                                for (SizeType j = item.first + 1; j < end; j++) {
                                    m_pTreeRoots.push_back(BKTNode(localindices[j]));
                                    m_pSampleCenterMap[localindices[j]] = m_pTreeRoots[item.index].centerid;
                                }
                                m_pSampleCenterMap[-1 - m_pTreeRoots[item.index].centerid] = item.index;
                            }
                            else {
                                for (int k = 0; k < m_iBKTKmeansK; k++) {
                                    if (args.counts[k] == 0) continue;
                                    m_pTreeRoots.push_back(BKTNode(localindices[item.first + args.counts[k] - 1]));
                                    if (args.counts[k] > 1) ss.push(BKTStackItem(newBKTid++, item.first, item.first + args.counts[k] - 1));
                                    item.first += args.counts[k];
                                }
                            }
                        }
                        m_pTreeRoots[item.index].childEnd = (SizeType)m_pTreeRoots.size();
                    }
                    std::cout << i + 1 << " BKTree built, " << m_pTreeRoots.size() - m_pTreeStart[i] << " " << localindices.size() << std::endl;
                }
            }

            inline std::uint64_t BufferSize() const
            {
                return sizeof(int) + sizeof(SizeType) * m_iTreeNumber +
                    sizeof(SizeType) + sizeof(BKTNode) * m_pTreeRoots.size();
            }

            bool SaveTrees(std::ostream& p_outstream) const
            {
                p_outstream.write((char*)&m_iTreeNumber, sizeof(int));
                p_outstream.write((char*)m_pTreeStart.data(), sizeof(SizeType) * m_iTreeNumber);
                SizeType treeNodeSize = (SizeType)m_pTreeRoots.size();
                p_outstream.write((char*)&treeNodeSize, sizeof(SizeType));
                p_outstream.write((char*)m_pTreeRoots.data(), sizeof(BKTNode) * treeNodeSize);
                std::cout << "Save BKT (" << m_iTreeNumber << "," << treeNodeSize << ") Finish!" << std::endl;
                return true;
            }

            bool SaveTrees(std::string sTreeFileName) const
            {
                std::cout << "Save BKT to " << sTreeFileName << std::endl;
                std::ofstream output(sTreeFileName, std::ios::binary);
                if (!output.is_open()) return false;
                SaveTrees(output);
                output.close();
                return true;
            }

            bool LoadTrees(char* pBKTMemFile)
            {
                m_iTreeNumber = *((int*)pBKTMemFile);
                pBKTMemFile += sizeof(int);
                m_pTreeStart.resize(m_iTreeNumber);
                memcpy(m_pTreeStart.data(), pBKTMemFile, sizeof(SizeType) * m_iTreeNumber);
                pBKTMemFile += sizeof(SizeType)*m_iTreeNumber;

                SizeType treeNodeSize = *((SizeType*)pBKTMemFile);
                pBKTMemFile += sizeof(SizeType);
                m_pTreeRoots.resize(treeNodeSize);
                memcpy(m_pTreeRoots.data(), pBKTMemFile, sizeof(BKTNode) * treeNodeSize);
                std::cout << "Load BKT (" << m_iTreeNumber << "," << treeNodeSize << ") Finish!" << std::endl;
                return true;
            }

            bool LoadTrees(std::string sTreeFileName)
            {
                std::cout << "Load BKT From " << sTreeFileName << std::endl;
                std::ifstream input(sTreeFileName, std::ios::binary);
                if (!input.is_open()) return false;

                input.read((char*)&m_iTreeNumber, sizeof(int));
                m_pTreeStart.resize(m_iTreeNumber);
                input.read((char*)m_pTreeStart.data(), sizeof(SizeType) * m_iTreeNumber);

                SizeType treeNodeSize;
                input.read((char*)&treeNodeSize, sizeof(SizeType));
                m_pTreeRoots.resize(treeNodeSize);
                input.read((char*)m_pTreeRoots.data(), sizeof(BKTNode) * treeNodeSize);
                input.close();
                std::cout << "Load BKT (" << m_iTreeNumber << "," << treeNodeSize << ") Finish!" << std::endl;
                return true;
            }

            template <typename T>
            void InitSearchTrees(const VectorIndex* p_index, const COMMON::QueryResultSet<T> &p_query, COMMON::WorkSpace &p_space) const
            {
                for (char i = 0; i < m_iTreeNumber; i++) {
                    const BKTNode& node = m_pTreeRoots[m_pTreeStart[i]];
                    if (node.childStart < 0) {
                        p_space.m_SPTQueue.insert(COMMON::HeapCell(m_pTreeStart[i], p_index->ComputeDistance((const void*)p_query.GetTarget(), p_index->GetSample(node.centerid))));
                    } 
                    else {
                        for (SizeType begin = node.childStart; begin < node.childEnd; begin++) {
                            SizeType index = m_pTreeRoots[begin].centerid;
                            p_space.m_SPTQueue.insert(COMMON::HeapCell(begin, p_index->ComputeDistance((const void*)p_query.GetTarget(), p_index->GetSample(index))));
                        }
                    } 
                }
            }

            template <typename T>
            void SearchTrees(const VectorIndex* p_index, const COMMON::QueryResultSet<T> &p_query, 
                COMMON::WorkSpace &p_space, const int p_limits) const
            {
                do
                {
                    COMMON::HeapCell bcell = p_space.m_SPTQueue.pop();
                    const BKTNode& tnode = m_pTreeRoots[bcell.node];
                    if (tnode.childStart < 0) {
                        if (!p_space.CheckAndSet(tnode.centerid)) {
                            p_space.m_iNumberOfCheckedLeaves++;
                            p_space.m_NGQueue.insert(COMMON::HeapCell(tnode.centerid, bcell.distance));
                        }
                        if (p_space.m_iNumberOfCheckedLeaves >= p_limits) break;
                    }
                    else {
                        if (!p_space.CheckAndSet(tnode.centerid)) {
                            p_space.m_NGQueue.insert(COMMON::HeapCell(tnode.centerid, bcell.distance));
                        }
                        for (SizeType begin = tnode.childStart; begin < tnode.childEnd; begin++) {
                            SizeType index = m_pTreeRoots[begin].centerid;
                            p_space.m_SPTQueue.insert(COMMON::HeapCell(begin, p_index->ComputeDistance((const void*)p_query.GetTarget(), p_index->GetSample(index))));
                        } 
                    }
                } while (!p_space.m_SPTQueue.empty());
            }

        private:

            template <typename T>
            float KmeansAssign(VectorIndex* p_index,
                               std::vector<SizeType>& indices,
                               const SizeType first, const SizeType last, KmeansArgs<T>& args, const bool updateCenters) const {
                float currDist = 0;
                int threads = omp_get_num_threads();
                float lambda = (updateCenters) ? COMMON::Utils::GetBase<T>() * COMMON::Utils::GetBase<T>() / (100.0f * (last - first)) : 0.0f;
                SizeType subsize = (last - first - 1) / threads + 1;

#pragma omp parallel for
                for (int tid = 0; tid < threads; tid++)
                {
                    SizeType istart = first + tid * subsize;
                    SizeType iend = min(first + (tid + 1) * subsize, last);
                    SizeType *inewCounts = args.newCounts + tid * m_iBKTKmeansK;
                    float *inewCenters = args.newCenters + tid * m_iBKTKmeansK * p_index->GetFeatureDim();
                    SizeType * iclusterIdx = args.clusterIdx + tid * m_iBKTKmeansK;
                    float * iclusterDist = args.clusterDist + tid * m_iBKTKmeansK;
                    float idist = 0;
                    for (SizeType i = istart; i < iend; i++) {
                        int clusterid = 0;
                        float smallestDist = MaxDist;
                        for (int k = 0; k < m_iBKTKmeansK; k++) {
                            float dist = p_index->ComputeDistance(p_index->GetSample(indices[i]), (const void*)(args.centers + k*p_index->GetFeatureDim())) + lambda*args.counts[k];
                            if (dist > -MaxDist && dist < smallestDist) {
                                clusterid = k; smallestDist = dist;
                            }
                        }
                        args.label[i] = clusterid;
                        inewCounts[clusterid]++;
                        idist += smallestDist;
                        if (updateCenters) {
                            const T* v = (const T*)p_index->GetSample(indices[i]);
                            float* center = inewCenters + clusterid*p_index->GetFeatureDim();
                            for (DimensionType j = 0; j < p_index->GetFeatureDim(); j++) center[j] += v[j];
                            if (smallestDist > iclusterDist[clusterid]) {
                                iclusterDist[clusterid] = smallestDist;
                                iclusterIdx[clusterid] = indices[i];
                            }
                        }
                        else {
                            if (smallestDist <= iclusterDist[clusterid]) {
                                iclusterDist[clusterid] = smallestDist;
                                iclusterIdx[clusterid] = indices[i];
                            }
                        }
                    }
                    COMMON::Utils::atomic_float_add(&currDist, idist);
                }

                for (int i = 1; i < threads; i++) {
                    for (int k = 0; k < m_iBKTKmeansK; k++)
                        args.newCounts[k] += args.newCounts[i*m_iBKTKmeansK + k];
                }

                if (updateCenters) {
                    for (int i = 1; i < threads; i++) {
                        float* currCenter = args.newCenters + i*m_iBKTKmeansK*p_index->GetFeatureDim();
                        for (size_t j = 0; j < ((size_t)m_iBKTKmeansK) * p_index->GetFeatureDim(); j++) args.newCenters[j] += currCenter[j];

                        for (int k = 0; k < m_iBKTKmeansK; k++) {
                            if (args.clusterIdx[i*m_iBKTKmeansK + k] != -1 && args.clusterDist[i*m_iBKTKmeansK + k] > args.clusterDist[k]) {
                                args.clusterDist[k] = args.clusterDist[i*m_iBKTKmeansK + k];
                                args.clusterIdx[k] = args.clusterIdx[i*m_iBKTKmeansK + k];
                            }
                        }
                    }

                    int maxcluster = -1;
                    SizeType maxCount = 0;
                    for (int k = 0; k < m_iBKTKmeansK; k++) {
                        if (args.newCounts[k] > maxCount && DistanceUtils::ComputeL2Distance((T*)p_index->GetSample(args.clusterIdx[k]), args.centers + k * p_index->GetFeatureDim(), p_index->GetFeatureDim()) > 1e-6)
                        {
                            maxcluster = k;
                            maxCount = args.newCounts[k];
                        }
                    }

                    if (maxcluster != -1 && (args.clusterIdx[maxcluster] < 0 || args.clusterIdx[maxcluster] >= p_index->GetNumSamples()))
                        std::cout << "first:" << first << " last:" << last << " maxcluster:" << maxcluster << "(" << args.newCounts[maxcluster] << ") Error dist:" << args.clusterDist[maxcluster] << std::endl;

                    for (int k = 0; k < m_iBKTKmeansK; k++) {
                        T* TCenter = args.newTCenters + k * p_index->GetFeatureDim();
                        if (args.newCounts[k] == 0) {
                            if (maxcluster != -1) {
                                //int nextid = Utils::rand_int(last, first);
                                //while (args.label[nextid] != maxcluster) nextid = Utils::rand_int(last, first);
                                SizeType nextid = args.clusterIdx[maxcluster];
                                std::memcpy(TCenter, p_index->GetSample(nextid), sizeof(T)*p_index->GetFeatureDim());
                            }
                            else {
                                std::memcpy(TCenter, args.centers + k * p_index->GetFeatureDim(), sizeof(T)*p_index->GetFeatureDim());
                            }
                        }
                        else {
                            float* currCenters = args.newCenters + k * p_index->GetFeatureDim();
                            for (DimensionType j = 0; j < p_index->GetFeatureDim(); j++) currCenters[j] /= args.newCounts[k];

                            if (p_index->GetDistCalcMethod() == DistCalcMethod::Cosine) {
                                COMMON::Utils::Normalize(currCenters, p_index->GetFeatureDim(), COMMON::Utils::GetBase<T>());
                            }
                            for (DimensionType j = 0; j < p_index->GetFeatureDim(); j++) TCenter[j] = (T)(currCenters[j]);
                        }
                    }
                }
                else {
                    for (int i = 1; i < threads; i++) {
                        for (int k = 0; k < m_iBKTKmeansK; k++) {
                            if (args.clusterIdx[i*m_iBKTKmeansK + k] != -1 && args.clusterDist[i*m_iBKTKmeansK + k] <= args.clusterDist[k]) {
                                args.clusterDist[k] = args.clusterDist[i*m_iBKTKmeansK + k];
                                args.clusterIdx[k] = args.clusterIdx[i*m_iBKTKmeansK + k];
                            }
                        }
                    }
                }
                return currDist;
            }

            template <typename T>
            int KmeansClustering(VectorIndex* p_index, 
                std::vector<SizeType>& indices, const SizeType first, const SizeType last, KmeansArgs<T>& args) const {
                int iterLimit = 100;

                SizeType batchEnd = min(first + m_iSamples, last);
                float currDiff, currDist, minClusterDist = MaxDist;
                for (int numKmeans = 0; numKmeans < 3; numKmeans++) {
                    for (int k = 0; k < m_iBKTKmeansK; k++) {
                        SizeType randid = COMMON::Utils::rand(last, first);
                        std::memcpy(args.centers + k*p_index->GetFeatureDim(), p_index->GetSample(indices[randid]), sizeof(T)*p_index->GetFeatureDim());
                    }
                    args.ClearCounts();
                    currDist = KmeansAssign(p_index, indices, first, batchEnd, args, false);
                    if (currDist < minClusterDist) {
                        minClusterDist = currDist;
                        memcpy(args.newTCenters, args.centers, sizeof(T)*m_iBKTKmeansK*p_index->GetFeatureDim());
                        memcpy(args.counts, args.newCounts, sizeof(SizeType) * m_iBKTKmeansK);
                    }
                }

                minClusterDist = MaxDist;
                int noImprovement = 0;
                for (int iter = 0; iter < iterLimit; iter++) {
                    std::memcpy(args.centers, args.newTCenters, sizeof(T)*m_iBKTKmeansK*p_index->GetFeatureDim());
                    std::random_shuffle(indices.begin() + first, indices.begin() + last);

                    args.ClearCenters();
                    args.ClearCounts();
                    args.ClearDists(-MaxDist);
                    currDist = KmeansAssign(p_index, indices, first, batchEnd, args, true);
                    memcpy(args.counts, args.newCounts, sizeof(SizeType) * m_iBKTKmeansK);

                    currDiff = 0;
                    for (int k = 0; k < m_iBKTKmeansK; k++) {
                        currDiff += p_index->ComputeDistance((const void*)(args.centers + k*p_index->GetFeatureDim()), (const void*)(args.newTCenters + k*p_index->GetFeatureDim()));
                    }

                    if (currDist < minClusterDist) {
                        noImprovement = 0;
                        minClusterDist = currDist;
                    }
                    else {
                        noImprovement++;
                    }
                    if (currDiff < 1e-3 || noImprovement >= 5) break;
                }

                args.ClearCounts();
                args.ClearDists(MaxDist);
                currDist = KmeansAssign(p_index, indices, first, last, args, false);
                memcpy(args.counts, args.newCounts, sizeof(SizeType) * m_iBKTKmeansK);

                int numClusters = 0;
                for (int i = 0; i < m_iBKTKmeansK; i++) if (args.counts[i] > 0) numClusters++;

                if (numClusters <= 1) {
                    //if (last - first > 1) std::cout << "large cluster:" << last - first << " dist:" << currDist << std::endl;
                    return numClusters;
                }
                args.Shuffle(indices, first, last);
                return numClusters;
            }

        private:
            std::vector<SizeType> m_pTreeStart;
            std::vector<BKTNode> m_pTreeRoots;
            std::unordered_map<SizeType, SizeType> m_pSampleCenterMap;

        public:
            int m_iTreeNumber, m_iBKTKmeansK, m_iBKTLeafSize, m_iSamples;
        };
    }
}
#endif
