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
            int centerid;
            int childStart;
            int childEnd;

            BKTNode(int cid = -1) : centerid(cid), childStart(-1), childEnd(-1) {}
        };

        template <typename T>
        struct KmeansArgs {
            int _K;
            int _D;
            int _T;
            T* centers;
            int* counts;
            float* newCenters;
            int* newCounts;
            char* label;
            int* clusterIdx;
            float* clusterDist;
            T* newTCenters;

            KmeansArgs(int k, int dim, int datasize, int threadnum) : _K(k), _D(dim), _T(threadnum) {
                centers = new T[k * dim];
                counts = new int[k];
                newCenters = new float[threadnum * k * dim];
                newCounts = new int[threadnum * k];
                label = new char[datasize];
                clusterIdx = new int[threadnum * k];
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
                memset(newCounts, 0, sizeof(int) * _T * _K);
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

            void Shuffle(std::vector<int>& indices, int first, int last) {
                int* pos = new int[_K];
                pos[0] = first;
                for (int k = 1; k < _K; k++) pos[k] = pos[k - 1] + newCounts[k - 1];

                for (int k = 0; k < _K; k++) {
                    if (newCounts[k] == 0) continue;
                    int i = pos[k];
                    while (newCounts[k] > 0) {
                        int swapid = pos[(int)(label[i])] + newCounts[(int)(label[i])] - 1;
                        newCounts[(int)(label[i])]--;
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

            inline const BKTNode& operator[](int index) const { return m_pTreeRoots[index]; }
            inline BKTNode& operator[](int index) { return m_pTreeRoots[index]; }

            inline int size() const { return (int)m_pTreeRoots.size(); }

            inline const std::unordered_map<int, int>& GetSampleMap() const { return m_pSampleCenterMap; }

            template <typename T>
            void BuildTrees(VectorIndex* index, std::vector<int>* indices = nullptr)
            {
                struct  BKTStackItem {
                    int index, first, last;
                    BKTStackItem(int index_, int first_, int last_) : index(index_), first(first_), last(last_) {}
                };
                std::stack<BKTStackItem> ss;

                std::vector<int> localindices;
                if (indices == nullptr) {
                    localindices.resize(index->GetNumSamples());
                    for (int i = 0; i < index->GetNumSamples(); i++) localindices[i] = i;
                }
                else {
                    localindices.assign(indices->begin(), indices->end());
                }
                KmeansArgs<T> args(m_iBKTKmeansK, index->GetFeatureDim(), (int)localindices.size(), omp_get_num_threads());

                m_pSampleCenterMap.clear();
                for (char i = 0; i < m_iTreeNumber; i++)
                {
                    std::random_shuffle(localindices.begin(), localindices.end());

                    m_pTreeStart.push_back((int)m_pTreeRoots.size());
                    m_pTreeRoots.push_back(BKTNode((int)localindices.size()));
                    std::cout << "Start to build BKTree " << i + 1 << std::endl;

                    ss.push(BKTStackItem(m_pTreeStart[i], 0, (int)localindices.size()));
                    while (!ss.empty()) {
                        BKTStackItem item = ss.top(); ss.pop();
                        int newBKTid = (int)m_pTreeRoots.size();
                        m_pTreeRoots[item.index].childStart = newBKTid;
                        if (item.last - item.first <= m_iBKTLeafSize) {
                            for (int j = item.first; j < item.last; j++) {
                                m_pTreeRoots.push_back(BKTNode(localindices[j]));
                            }
                        }
                        else { // clustering the data into BKTKmeansK clusters
                            int numClusters = KmeansClustering(index, localindices, item.first, item.last, args);
                            if (numClusters <= 1) {
                                int end = min(item.last + 1, (int)localindices.size());
                                std::sort(localindices.begin() + item.first, localindices.begin() + end);
                                m_pTreeRoots[item.index].centerid = localindices[item.first];
                                m_pTreeRoots[item.index].childStart = -m_pTreeRoots[item.index].childStart;
                                for (int j = item.first + 1; j < end; j++) {
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
                        m_pTreeRoots[item.index].childEnd = (int)m_pTreeRoots.size();
                    }
                    std::cout << i + 1 << " BKTree built, " << m_pTreeRoots.size() - m_pTreeStart[i] << " " << localindices.size() << std::endl;
                }
            }

            bool SaveTrees(void **pKDTMemFile, int64_t &len) const
            {
                int treeNodeSize = (int)m_pTreeRoots.size();

                size_t size = sizeof(int) +
                    sizeof(int) * m_iTreeNumber +
                    sizeof(int) +
                    sizeof(BKTNode) * treeNodeSize;
                char *mem = (char*)malloc(size);
                if (mem == NULL) return false;

                auto ptr = mem;
                *(int*)ptr = m_iTreeNumber;
                ptr += sizeof(int);

                memcpy(ptr, m_pTreeStart.data(), sizeof(int) * m_iTreeNumber);
                ptr += sizeof(int) * m_iTreeNumber;

                *(int*)ptr = treeNodeSize;
                ptr += sizeof(int);

                memcpy(ptr, m_pTreeRoots.data(), sizeof(BKTNode) * treeNodeSize);
                *pKDTMemFile = mem;
                len = size;

                return true;
            }

            bool SaveTrees(std::string sTreeFileName) const
            {
                std::cout << "Save BKT to " << sTreeFileName << std::endl;
                FILE *fp = fopen(sTreeFileName.c_str(), "wb");
                if (fp == NULL) return false;

                fwrite(&m_iTreeNumber, sizeof(int), 1, fp);
                fwrite(m_pTreeStart.data(), sizeof(int), m_iTreeNumber, fp);
                int treeNodeSize = (int)m_pTreeRoots.size();
                fwrite(&treeNodeSize, sizeof(int), 1, fp);
                fwrite(m_pTreeRoots.data(), sizeof(BKTNode), treeNodeSize, fp);
                fclose(fp);
                std::cout << "Save BKT (" << m_iTreeNumber << "," << treeNodeSize << ") Finish!" << std::endl;
                return true;
            }

            bool LoadTrees(char* pBKTMemFile)
            {
                m_iTreeNumber = *((int*)pBKTMemFile);
                pBKTMemFile += sizeof(int);
                m_pTreeStart.resize(m_iTreeNumber);
                memcpy(m_pTreeStart.data(), pBKTMemFile, sizeof(int) * m_iTreeNumber);
                pBKTMemFile += sizeof(int)*m_iTreeNumber;

                int treeNodeSize = *((int*)pBKTMemFile);
                pBKTMemFile += sizeof(int);
                m_pTreeRoots.resize(treeNodeSize);
                memcpy(m_pTreeRoots.data(), pBKTMemFile, sizeof(BKTNode) * treeNodeSize);
                return true;
            }

            bool LoadTrees(std::string sTreeFileName)
            {
                std::cout << "Load BKT From " << sTreeFileName << std::endl;
                FILE *fp = fopen(sTreeFileName.c_str(), "rb");
                if (fp == NULL) return false;

                fread(&m_iTreeNumber, sizeof(int), 1, fp);
                m_pTreeStart.resize(m_iTreeNumber);
                fread(m_pTreeStart.data(), sizeof(int), m_iTreeNumber, fp);

                int treeNodeSize;
                fread(&treeNodeSize, sizeof(int), 1, fp);
                m_pTreeRoots.resize(treeNodeSize);
                fread(m_pTreeRoots.data(), sizeof(BKTNode), treeNodeSize, fp);
                fclose(fp);
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
                        for (int begin = node.childStart; begin < node.childEnd; begin++) {
                            int index = m_pTreeRoots[begin].centerid;
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
                        for (int begin = tnode.childStart; begin < tnode.childEnd; begin++) {
                            int index = m_pTreeRoots[begin].centerid;
                            p_space.m_SPTQueue.insert(COMMON::HeapCell(begin, p_index->ComputeDistance((const void*)p_query.GetTarget(), p_index->GetSample(index))));
                        } 
                    }
                } while (!p_space.m_SPTQueue.empty());
            }

        private:

            template <typename T>
            float KmeansAssign(VectorIndex* p_index,
                               std::vector<int>& indices, 
                               const int first, const int last, KmeansArgs<T>& args, const bool updateCenters) const {
                float currDist = 0;
                int threads = omp_get_num_threads();
                float lambda = (updateCenters) ? COMMON::Utils::GetBase<T>() * COMMON::Utils::GetBase<T>() / (100.0f * (last - first)) : 0.0f;
                int subsize = (last - first - 1) / threads + 1;

#pragma omp parallel for
                for (int tid = 0; tid < threads; tid++)
                {
                    int istart = first + tid * subsize;
                    int iend = min(first + (tid + 1) * subsize, last);
                    int *inewCounts = args.newCounts + tid * m_iBKTKmeansK;
                    float *inewCenters = args.newCenters + tid * m_iBKTKmeansK * p_index->GetFeatureDim();
                    int * iclusterIdx = args.clusterIdx + tid * m_iBKTKmeansK;
                    float * iclusterDist = args.clusterDist + tid * m_iBKTKmeansK;
                    float idist = 0;
                    for (int i = istart; i < iend; i++) {
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
                            for (int j = 0; j < p_index->GetFeatureDim(); j++) center[j] += v[j];
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
                        for (int j = 0; j < m_iBKTKmeansK * p_index->GetFeatureDim(); j++) args.newCenters[j] += currCenter[j];
                    }

                    int maxcluster = 0;
                    for (int k = 1; k < m_iBKTKmeansK; k++) if (args.newCounts[maxcluster] < args.newCounts[k]) maxcluster = k;

                    int maxid = maxcluster;
                    for (int tid = 1; tid < threads; tid++) {
                        if (args.clusterDist[maxid] < args.clusterDist[tid * m_iBKTKmeansK + maxcluster]) maxid = tid * m_iBKTKmeansK + maxcluster;
                    }
                    if (args.clusterIdx[maxid] < 0 || args.clusterIdx[maxid] >= p_index->GetNumSamples())
                        std::cout << "first:" << first << " last:" << last << " maxcluster:" << maxcluster << "(" << args.newCounts[maxcluster] << ") Error maxid:" << maxid << " dist:" << args.clusterDist[maxid] << std::endl;
                    maxid = args.clusterIdx[maxid];

                    for (int k = 0; k < m_iBKTKmeansK; k++) {
                        T* TCenter = args.newTCenters + k * p_index->GetFeatureDim();
                        if (args.newCounts[k] == 0) {
                            //int nextid = Utils::rand_int(last, first);
                            //while (args.label[nextid] != maxcluster) nextid = Utils::rand_int(last, first);
                            int nextid = maxid;
                            std::memcpy(TCenter, p_index->GetSample(nextid), sizeof(T)*p_index->GetFeatureDim());
                        }
                        else {
                            float* currCenters = args.newCenters + k * p_index->GetFeatureDim();
                            for (int j = 0; j < p_index->GetFeatureDim(); j++) currCenters[j] /= args.newCounts[k];

                            if (p_index->GetDistCalcMethod() == DistCalcMethod::Cosine) {
                                COMMON::Utils::Normalize(currCenters, p_index->GetFeatureDim(), COMMON::Utils::GetBase<T>());
                            }
                            for (int j = 0; j < p_index->GetFeatureDim(); j++) TCenter[j] = (T)(currCenters[j]);
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
                std::vector<int>& indices, const int first, const int last, KmeansArgs<T>& args) const {
                int iterLimit = 100;

                int batchEnd = min(first + m_iSamples, last);
                float currDiff, currDist, minClusterDist = MaxDist;
                for (int numKmeans = 0; numKmeans < 3; numKmeans++) {
                    for (int k = 0; k < m_iBKTKmeansK; k++) {
                        int randid = COMMON::Utils::rand_int(last, first);
                        std::memcpy(args.centers + k*p_index->GetFeatureDim(), p_index->GetSample(indices[randid]), sizeof(T)*p_index->GetFeatureDim());
                    }
                    args.ClearCounts();
                    currDist = KmeansAssign(p_index, indices, first, batchEnd, args, false);
                    if (currDist < minClusterDist) {
                        minClusterDist = currDist;
                        memcpy(args.newTCenters, args.centers, sizeof(T)*m_iBKTKmeansK*p_index->GetFeatureDim());
                        memcpy(args.counts, args.newCounts, sizeof(int) * m_iBKTKmeansK);
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
                    memcpy(args.counts, args.newCounts, sizeof(int)*m_iBKTKmeansK);

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
                memcpy(args.counts, args.newCounts, sizeof(int)*m_iBKTKmeansK);

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
            std::vector<int> m_pTreeStart;
            std::vector<BKTNode> m_pTreeRoots;
            std::unordered_map<int, int> m_pSampleCenterMap;

        public:
            int m_iTreeNumber, m_iBKTKmeansK, m_iBKTLeafSize, m_iSamples;
        };
    }
}
#endif
