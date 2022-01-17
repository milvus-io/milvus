//
// Copyright (C) 2015-2020 Yahoo Japan Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#pragma once

#include "NGT/Index.h"
#include "defines.h"

using namespace std;

#if defined(NGT_AVX_DISABLED)
#define NGT_CLUSTER_NO_AVX
#else
#if defined(__AVX2__)
#define NGT_CLUSTER_AVX2
#else
#define NGT_CLUSTER_NO_AVX
#endif
#endif

#if defined(NGT_CLUSTER_NO_AVX)
// #warning "*** SIMD is *NOT* available! ***"
#else
#include <immintrin.h>
#endif

#include <omp.h>
#include <random>

namespace NGT {

class Clustering {
 public:
    enum InitializationMode {
        InitializationModeHead = 0,
        InitializationModeRandom = 1,
        InitializationModeKmeansPlusPlus = 2
    };

    enum ClusteringType {
        ClusteringTypeKmeansWithNGT = 0,
        ClusteringTypeKmeansWithoutNGT = 1,
        ClusteringTypeKmeansWithIteration = 2,
        ClusteringTypeKmeansWithNGTForCentroids = 3
    };

    class Entry {
     public:
        Entry() : vectorID(0), centroidID(0), distance(0.0) {
        }
        Entry(size_t vid, size_t cid, double d) : vectorID(vid), centroidID(cid), distance(d) {
        }
        bool
        operator<(const Entry& e) const {
            return distance > e.distance;
        }
        uint32_t vectorID;
        uint32_t centroidID;
        double distance;
    };

    class DescendingEntry {
     public:
        DescendingEntry(size_t vid, double d) : vectorID(vid), distance(d) {
        }
        bool
        operator<(const DescendingEntry& e) const {
            return distance < e.distance;
        }
        size_t vectorID;
        double distance;
    };

    class Cluster {
     public:
        Cluster(std::vector<float>& c) : centroid(c), radius(0.0) {
        }
        Cluster(const Cluster& c) {
            *this = c;
        }
        Cluster&
        operator=(const Cluster& c) {
            members = c.members;
            centroid = c.centroid;
            radius = c.radius;
            return *this;
        }

        std::vector<Entry> members;
        std::vector<float> centroid;
        double radius;
    };

    Clustering(InitializationMode im = InitializationModeHead, ClusteringType ct = ClusteringTypeKmeansWithNGT,
               size_t mi = 100)
        : clusteringType(ct), initializationMode(im), maximumIteration(mi) {
        initialize();
    }

    void
    initialize() {
        epsilonFrom = 0.12;
        epsilonTo = epsilonFrom;
        epsilonStep = 0.04;
        resultSizeCoefficient = 5;
    }

    static void
    convert(std::vector<std::string>& strings, std::vector<float>& vector) {
        vector.clear();
        for (auto it = strings.begin(); it != strings.end(); ++it) {
            vector.push_back(stod(*it));
        }
    }

    static void
    extractVector(const std::string& str, std::vector<float>& vec) {
        std::vector<std::string> tokens;
        NGT::Common::tokenize(str, tokens, " \t");
        convert(tokens, vec);
    }

    static void
    loadVectors(const std::string& file, std::vector<std::vector<float> >& vectors) {
        std::ifstream is(file);
        if (!is) {
            throw std::runtime_error("loadVectors::Cannot open " + file);
        }
        std::string line;
        while (getline(is, line)) {
            std::vector<float> v;
            extractVector(line, v);
            vectors.push_back(v);
        }
    }

    static void
    saveVectors(const std::string& file, std::vector<std::vector<float> >& vectors) {
        std::ofstream os(file);
        for (auto vit = vectors.begin(); vit != vectors.end(); ++vit) {
            std::vector<float>& v = *vit;
            for (auto it = v.begin(); it != v.end(); ++it) {
                os << std::setprecision(9) << (*it);
                if (it + 1 != v.end()) {
                    os << "\t";
                }
            }
            os << std::endl;
        }
    }

    static void
    saveVector(const std::string& file, std::vector<size_t>& vectors) {
        std::ofstream os(file);
        for (auto vit = vectors.begin(); vit != vectors.end(); ++vit) {
            os << *vit << std::endl;
        }
    }

    static void
    loadClusters(const std::string& file, std::vector<Cluster>& clusters, size_t numberOfClusters = 0) {
        std::ifstream is(file);
        if (!is) {
            throw std::runtime_error("loadClusters::Cannot open " + file);
        }
        std::string line;
        while (getline(is, line)) {
            std::vector<float> v;
            extractVector(line, v);
            clusters.push_back(v);
            if ((numberOfClusters != 0) && (clusters.size() >= numberOfClusters)) {
                break;
            }
        }
        if ((numberOfClusters != 0) && (clusters.size() < numberOfClusters)) {
//            std::cerr << "initial cluster data are not enough. " << clusters.size() << ":" << numberOfClusters
//                      << std::endl;
            if (NGT_LOG_DEBUG_)
                (*NGT_LOG_DEBUG_)("initial cluster data are not enough. " + std::to_string(clusters.size()) + ":" + std::to_string(numberOfClusters));
            exit(1);
        }
    }
#if !defined(NGT_CLUSTER_NO_AVX)
    static double
    sumOfSquares(float* a, float* b, size_t size) {
        __m256 sum = _mm256_setzero_ps();
        float* last = a + size;
        float* lastgroup = last - 7;
        while (a < lastgroup) {
            __m256 v = _mm256_sub_ps(_mm256_loadu_ps(a), _mm256_loadu_ps(b));
            sum = _mm256_add_ps(sum, _mm256_mul_ps(v, v));
            a += 8;
            b += 8;
        }
        __attribute__((aligned(32))) float f[8];
        _mm256_store_ps(f, sum);
        double s = f[0] + f[1] + f[2] + f[3] + f[4] + f[5] + f[6] + f[7];
        while (a < last) {
            double d = *a++ - *b++;
            s += d * d;
        }
        return s;
    }
#else   // !defined(NGT_AVX_DISABLED) && defined(__AVX__)
    static double
    sumOfSquares(float* a, float* b, size_t size) {
        double csum = 0.0;
        float* x = a;
        float* y = b;
        for (size_t i = 0; i < size; i++) {
            double d = (double)*x++ - (double)*y++;
            csum += d * d;
        }
        return csum;
    }
#endif  // !defined(NGT_AVX_DISABLED) && defined(__AVX__)

    static double
    distanceL2(std::vector<float>& vector1, std::vector<float>& vector2) {
        return sqrt(sumOfSquares(&vector1[0], &vector2[0], vector1.size()));
    }

    static double
    distanceL2(std::vector<std::vector<float> >& vector1, std::vector<std::vector<float> >& vector2) {
        assert(vector1.size() == vector2.size());
        double distance = 0.0;
        for (size_t i = 0; i < vector1.size(); i++) {
            distance += distanceL2(vector1[i], vector2[i]);
        }
        distance /= (double)vector1.size();
        return distance;
    }

    static double
    meanSumOfSquares(std::vector<float>& vector1, std::vector<float>& vector2) {
        return sumOfSquares(&vector1[0], &vector2[0], vector1.size()) / (double)vector1.size();
    }

    static void
    subtract(std::vector<float>& a, std::vector<float>& b) {
        assert(a.size() == b.size());
        auto bit = b.begin();
        for (auto ait = a.begin(); ait != a.end(); ++ait, ++bit) {
            *ait = *ait - *bit;
        }
    }

    static void
    getInitialCentroidsFromHead(std::vector<std::vector<float> >& vectors, std::vector<Cluster>& clusters,
                                size_t size) {
        size = size > vectors.size() ? vectors.size() : size;
        clusters.clear();
        for (size_t i = 0; i < size; i++) {
            clusters.push_back(Cluster(vectors[i]));
        }
    }

    static void
    getInitialCentroidsRandomly(std::vector<std::vector<float> >& vectors, std::vector<Cluster>& clusters, size_t size,
                                size_t seed) {
        clusters.clear();
        std::random_device rnd;
        if (seed == 0) {
            seed = rnd();
        }
        std::mt19937 mt(seed);

        for (size_t i = 0; i < size; i++) {
            size_t idx = mt() * vectors.size() / mt.max();
            if (idx >= size) {
                i--;
                continue;
            }
            clusters.push_back(Cluster(vectors[idx]));
        }
        assert(clusters.size() == size);
    }

    static void
    getInitialCentroidsKmeansPlusPlus(std::vector<std::vector<float> >& vectors, std::vector<Cluster>& clusters,
                                      size_t size) {
        size = size > vectors.size() ? vectors.size() : size;
        clusters.clear();
        std::random_device rnd;
        std::mt19937 mt(rnd());
        size_t idx = (long long)mt() * (long long)vectors.size() / (long long)mt.max();
        clusters.push_back(Cluster(vectors[idx]));

        NGT::Timer timer;
        for (size_t k = 1; k < size; k++) {
            double sum = 0;
            std::priority_queue<DescendingEntry> sortedObjects;
            // get d^2 and sort
#pragma omp parallel for
            for (size_t vi = 0; vi < vectors.size(); vi++) {
                auto vit = vectors.begin() + vi;
                double mind = DBL_MAX;
                for (auto cit = clusters.begin(); cit != clusters.end(); ++cit) {
                    double d = distanceL2(*vit, (*cit).centroid);
                    d *= d;
                    if (d < mind) {
                        mind = d;
                    }
                }
#pragma omp critical
                {
                    sortedObjects.push(DescendingEntry(distance(vectors.begin(), vit), mind));
                    sum += mind;
                }
            }
            double l = (double)mt() / (double)mt.max() * sum;
            while (!sortedObjects.empty()) {
                sum -= sortedObjects.top().distance;
                if (l >= sum) {
                    clusters.push_back(Cluster(vectors[sortedObjects.top().vectorID]));
                    break;
                }
                sortedObjects.pop();
            }
        }
    }

    static void
    assign(std::vector<std::vector<float> >& vectors, std::vector<Cluster>& clusters,
           size_t clusterSize = std::numeric_limits<size_t>::max()) {
        // compute distances to the nearest clusters, and construct heap by the distances.
        NGT::Timer timer;
        timer.start();

        std::vector<Entry> sortedObjects(vectors.size());
#pragma omp parallel for
        for (size_t vi = 0; vi < vectors.size(); vi++) {
            auto vit = vectors.begin() + vi;
            {
                double mind = DBL_MAX;
                size_t mincidx = -1;
                for (auto cit = clusters.begin(); cit != clusters.end(); ++cit) {
                    double d = distanceL2(*vit, (*cit).centroid);
                    if (d < mind) {
                        mind = d;
                        mincidx = distance(clusters.begin(), cit);
                    }
                }
                sortedObjects[vi] = Entry(vi, mincidx, mind);
            }
        }
        std::sort(sortedObjects.begin(), sortedObjects.end());

        // clear
        for (auto cit = clusters.begin(); cit != clusters.end(); ++cit) {
            (*cit).members.clear();
        }

        // distribute objects to the nearest clusters in the same size constraint.
        for (auto soi = sortedObjects.rbegin(); soi != sortedObjects.rend();) {
            Entry& entry = *soi;
            if (entry.centroidID >= clusters.size()) {
//                std::cerr << "Something wrong. " << entry.centroidID << ":" << clusters.size() << std::endl;
                if (NGT_LOG_DEBUG_)
                    (*NGT_LOG_DEBUG_)("Something wrong. " + std::to_string(entry.centroidID) + ":" + std::to_string(clusters.size()));
                soi++;
                continue;
            }
            if (clusters[entry.centroidID].members.size() < clusterSize) {
                clusters[entry.centroidID].members.push_back(entry);
                soi++;
            } else {
                double mind = DBL_MAX;
                size_t mincidx = -1;
                for (auto cit = clusters.begin(); cit != clusters.end(); ++cit) {
                    if ((*cit).members.size() >= clusterSize) {
                        continue;
                    }
                    double d = distanceL2(vectors[entry.vectorID], (*cit).centroid);
                    if (d < mind) {
                        mind = d;
                        mincidx = distance(clusters.begin(), cit);
                    }
                }
                entry = Entry(entry.vectorID, mincidx, mind);
                int pt = distance(sortedObjects.rbegin(), soi);
                std::sort(sortedObjects.begin(), soi.base());
                soi = sortedObjects.rbegin() + pt;
                assert(pt == distance(sortedObjects.rbegin(), soi));
            }
        }

        moveFartherObjectsToEmptyClusters(clusters);
    }

    static void
    moveFartherObjectsToEmptyClusters(std::vector<Cluster>& clusters) {
        size_t emptyClusterCount = 0;
        for (auto cit = clusters.begin(); cit != clusters.end(); ++cit) {
            if ((*cit).members.size() == 0) {
                emptyClusterCount++;
                double max = 0.0;
                auto maxit = clusters.begin();
                for (auto scit = clusters.begin(); scit != clusters.end(); ++scit) {
                    if ((*scit).members.size() >= 2 && (*scit).members.back().distance > max) {
                        maxit = scit;
                        max = (*scit).members.back().distance;
                    }
                }
                (*cit).members.push_back((*maxit).members.back());
                (*cit).members.back().centroidID = distance(clusters.begin(), cit);
                (*maxit).members.pop_back();
            }
        }
        emptyClusterCount = 0;
        for (auto cit = clusters.begin(); cit != clusters.end(); ++cit) {
            if ((*cit).members.size() == 0) {
                emptyClusterCount++;
            }
        }
    }

    static void
    assignWithNGT(NGT::Index& index, std::vector<std::vector<float> >& vectors, std::vector<Cluster>& clusters,
                  float& radius, size_t& resultSize, float epsilon = 0.12, size_t notRetrievedObjectCount = 0) {
        size_t dataSize = vectors.size();
        assert(index.getObjectRepositorySize() - 1 == vectors.size());
        vector<vector<Entry> > results(clusters.size());
#pragma omp parallel for
        for (size_t ci = 0; ci < clusters.size(); ci++) {
            auto cit = clusters.begin() + ci;
            NGT::ObjectDistances objects;  // result set
            NGT::Object* query = 0;
            query = index.allocateObject((*cit).centroid);
            // set search prameters.
            NGT::SearchContainer sc(*query);  // search parametera container.
            sc.setResults(&objects);          // set the result set.
            sc.setEpsilon(epsilon);           // set exploration coefficient.
            if (radius > 0.0) {
                sc.setRadius(radius);
                sc.setSize(dataSize / 2);
            } else {
                sc.setSize(resultSize);  // the number of resultant objects.
            }
            index.search(sc);
            results[ci].reserve(objects.size());
            for (size_t idx = 0; idx < objects.size(); idx++) {
                size_t oidx = objects[idx].id - 1;
                results[ci].push_back(Entry(oidx, ci, objects[idx].distance));
            }

            index.deleteObject(query);
        }
        size_t resultCount = 0;
        for (auto ri = results.begin(); ri != results.end(); ++ri) {
            resultCount += (*ri).size();
        }
        vector<Entry> sortedResults;
        sortedResults.reserve(resultCount);
        for (auto ri = results.begin(); ri != results.end(); ++ri) {
            auto end = (*ri).begin();
            for (; end != (*ri).end(); ++end) {
            }
            std::copy((*ri).begin(), end, std::back_inserter(sortedResults));
        }

        vector<bool> processedObjects(dataSize, false);
        for (auto i = sortedResults.begin(); i != sortedResults.end(); ++i) {
            processedObjects[(*i).vectorID] = true;
        }

        notRetrievedObjectCount = 0;
        vector<uint32_t> notRetrievedObjectIDs;
        for (size_t idx = 0; idx < dataSize; idx++) {
            if (!processedObjects[idx]) {
                notRetrievedObjectCount++;
                notRetrievedObjectIDs.push_back(idx);
            }
        }

        sort(sortedResults.begin(), sortedResults.end());

        for (auto cit = clusters.begin(); cit != clusters.end(); ++cit) {
            (*cit).members.clear();
        }

        for (auto i = sortedResults.rbegin(); i != sortedResults.rend(); ++i) {
            size_t objectID = (*i).vectorID;
            size_t clusterID = (*i).centroidID;
            if (processedObjects[objectID]) {
                processedObjects[objectID] = false;
                clusters[clusterID].members.push_back(*i);
                clusters[clusterID].members.back().centroidID = clusterID;
                radius = (*i).distance;
            }
        }

        vector<Entry> notRetrievedObjects(notRetrievedObjectIDs.size());

#pragma omp parallel for
        for (size_t vi = 0; vi < notRetrievedObjectIDs.size(); vi++) {
            auto vit = notRetrievedObjectIDs.begin() + vi;
            {
                double mind = DBL_MAX;
                size_t mincidx = -1;
                for (auto cit = clusters.begin(); cit != clusters.end(); ++cit) {
                    double d = distanceL2(vectors[*vit], (*cit).centroid);
                    if (d < mind) {
                        mind = d;
                        mincidx = distance(clusters.begin(), cit);
                    }
                }
                notRetrievedObjects[vi] = Entry(*vit, mincidx, mind);  // Entry(vectorID, centroidID, distance)
            }
        }

        sort(notRetrievedObjects.begin(), notRetrievedObjects.end());

        for (auto nroit = notRetrievedObjects.begin(); nroit != notRetrievedObjects.end(); ++nroit) {
            clusters[(*nroit).centroidID].members.push_back(*nroit);
        }

        moveFartherObjectsToEmptyClusters(clusters);
    }

    static double
    calculateCentroid(std::vector<std::vector<float> >& vectors, std::vector<Cluster>& clusters) {
        double distance = 0;
        size_t memberCount = 0;
        for (auto it = clusters.begin(); it != clusters.end(); ++it) {
            memberCount += (*it).members.size();
            if ((*it).members.size() != 0) {
                std::vector<float> mean(vectors[0].size(), 0.0);
                for (auto memit = (*it).members.begin(); memit != (*it).members.end(); ++memit) {
                    auto mit = mean.begin();
                    auto& v = vectors[(*memit).vectorID];
                    for (auto vit = v.begin(); vit != v.end(); ++vit, ++mit) {
                        *mit += *vit;
                    }
                }
                for (auto mit = mean.begin(); mit != mean.end(); ++mit) {
                    *mit /= (*it).members.size();
                }
                distance += distanceL2((*it).centroid, mean);
                (*it).centroid = mean;
            } else {
//                cerr << "Clustering: Fatal Error. No member!" << endl;
                if (NGT_LOG_DEBUG_)
                    (*NGT_LOG_DEBUG_)("Clustering: Fatal Error. No member!");
                abort();
            }
        }
        return distance;
    }

    static void
    saveClusters(const std::string& file, std::vector<Cluster>& clusters) {
        std::ofstream os(file);
        for (auto cit = clusters.begin(); cit != clusters.end(); ++cit) {
            std::vector<float>& v = (*cit).centroid;
            for (auto it = v.begin(); it != v.end(); ++it) {
                os << std::setprecision(9) << (*it);
                if (it + 1 != v.end()) {
                    os << "\t";
                }
            }
            os << std::endl;
        }
    }

    double
    kmeansWithoutNGT(std::vector<std::vector<float> >& vectors, size_t numberOfClusters,
                     std::vector<Cluster>& clusters) {
        size_t clusterSize = std::numeric_limits<size_t>::max();
        if (clusterSizeConstraint) {
            clusterSize = ceil((double)vectors.size() / (double)numberOfClusters);
        }

        double diff = 0;
        for (size_t i = 0; i < maximumIteration; i++) {
//            std::cerr << "iteration=" << i << std::endl;
            if (NGT_LOG_DEBUG_)
                (*NGT_LOG_DEBUG_)("iteration=" + std::to_string(i));
            assign(vectors, clusters, clusterSize);
            // centroid is recomputed.
            // diff is distance between the current centroids and the previous centroids.
            diff = calculateCentroid(vectors, clusters);
            if (diff == 0) {
                break;
            }
        }
        return diff == 0;
    }

    double
    kmeansWithNGT(NGT::Index& index, std::vector<std::vector<float> >& vectors, size_t numberOfClusters,
                  std::vector<Cluster>& clusters, float epsilon) {
        diffHistory.clear();
        NGT::Timer timer;
        timer.start();
        float radius;
        double diff = 0.0;
        size_t resultSize;
        resultSize = resultSizeCoefficient * vectors.size() / clusters.size();
        for (size_t i = 0; i < maximumIteration; i++) {
            size_t notRetrievedObjectCount = 0;
            radius = -1.0;
            assignWithNGT(index, vectors, clusters, radius, resultSize, epsilon, notRetrievedObjectCount);
            // centroid is recomputed.
            // diff is distance between the current centroids and the previous centroids.
            std::vector<Cluster> prevClusters = clusters;
            diff = calculateCentroid(vectors, clusters);
            timer.stop();
//            std::cerr << "iteration=" << i << " time=" << timer << " diff=" << diff << std::endl;
            if (NGT_LOG_DEBUG_)
                (*NGT_LOG_DEBUG_)("iteration=" + std::to_string(i) + " time=" + std::to_string(timer.time)+ " diff=" + std::to_string(diff));
            timer.start();
            diffHistory.push_back(diff);

            if (diff == 0) {
                break;
            }
        }
        return diff;
    }

    double
    kmeansWithNGT(std::vector<std::vector<float> >& vectors, size_t numberOfClusters, std::vector<Cluster>& clusters) {
        pid_t pid = getpid();
        std::stringstream str;
        str << "cluster-ngt." << pid;
        string database = str.str();
        string dataFile;
        size_t dataSize = 0;
        size_t dim = clusters.front().centroid.size();
        NGT::Property property;
        property.dimension = dim;
        property.graphType = NGT::Property::GraphType::GraphTypeANNG;
        property.objectType = NGT::Index::Property::ObjectType::Float;
        property.distanceType = NGT::Index::Property::DistanceType::DistanceTypeL2;

        NGT::Index::createGraphAndTree(database, property, dataFile, dataSize);

        float* data = new float[vectors.size() * dim];
        float* ptr = data;
        dataSize = vectors.size();
        for (auto vi = vectors.begin(); vi != vectors.end(); ++vi) {
            memcpy(ptr, &((*vi)[0]), dim * sizeof(float));
            ptr += dim;
        }
        size_t threadSize = 20;
        NGT::Index::append(database, data, dataSize, threadSize);
        delete[] data;

        NGT::Index index(database);

        return kmeansWithNGT(index, vectors, numberOfClusters, clusters, epsilonFrom);
    }

    double
    kmeansWithNGT(NGT::Index& index, size_t numberOfClusters, std::vector<Cluster>& clusters) {
        NGT::GraphIndex& graph = static_cast<NGT::GraphIndex&>(index.getIndex());
        NGT::ObjectSpace& os = graph.getObjectSpace();
        size_t size = os.getRepository().size();
        std::vector<std::vector<float> > vectors(size - 1);
        for (size_t idx = 1; idx < size; idx++) {
            try {
                os.getObject(idx, vectors[idx - 1]);
            } catch (...) {
//                cerr << "Cannot get object " << idx << endl;
                if (NGT_LOG_DEBUG_)
                    (*NGT_LOG_DEBUG_)("Cannot get object " + std::to_string(idx));
            }
        }
//        cerr << "# of data for clustering=" << vectors.size() << endl;
        if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)("# of data for clustering=" + std::to_string(vectors.size()));
        double diff = DBL_MAX;
        clusters.clear();
        setupInitialClusters(vectors, numberOfClusters, clusters);
        for (float epsilon = epsilonFrom; epsilon <= epsilonTo; epsilon += epsilonStep) {
//            cerr << "epsilon=" << epsilon << endl;
            if (NGT_LOG_DEBUG_)
                (*NGT_LOG_DEBUG_)("epsilon=" + std::to_string(epsilon));
            diff = kmeansWithNGT(index, vectors, numberOfClusters, clusters, epsilon);
            if (diff == 0.0) {
                return diff;
            }
        }
        return diff;
    }

    double
    kmeansWithNGT(NGT::Index& index, size_t numberOfClusters, NGT::Index& outIndex) {
        std::vector<Cluster> clusters;
        double diff = kmeansWithNGT(index, numberOfClusters, clusters);
        for (auto i = clusters.begin(); i != clusters.end(); ++i) {
            outIndex.insert((*i).centroid);
        }
        outIndex.createIndex(16);
        return diff;
    }

    double
    kmeansWithNGT(NGT::Index& index, size_t numberOfClusters) {
        NGT::Property prop;
        index.getProperty(prop);
        string path = index.getPath();
        index.save();
        index.close();
        string outIndexName = path;
        string inIndexName = path + ".tmp";
        std::rename(outIndexName.c_str(), inIndexName.c_str());
        NGT::Index::createGraphAndTree(outIndexName, prop);
        index.open(outIndexName);
        NGT::Index inIndex(inIndexName);
        double diff = kmeansWithNGT(inIndex, numberOfClusters, index);
        inIndex.close();
        NGT::Index::destroy(inIndexName);
        return diff;
    }

    double
    kmeansWithNGT(string& indexName, size_t numberOfClusters) {
        NGT::Index inIndex(indexName);
        double diff = kmeansWithNGT(inIndex, numberOfClusters);
        inIndex.save();
        inIndex.close();
        return diff;
    }

    static double
    calculateMSE(std::vector<std::vector<float> >& vectors, std::vector<Cluster>& clusters) {
        double mse = 0.0;
        size_t count = 0;
        for (auto cit = clusters.begin(); cit != clusters.end(); ++cit) {
            count += (*cit).members.size();
            for (auto mit = (*cit).members.begin(); mit != (*cit).members.end(); ++mit) {
                mse += meanSumOfSquares((*cit).centroid, vectors[(*mit).vectorID]);
            }
        }
        assert(vectors.size() == count);
        return mse / (double)vectors.size();
    }

    static double
    calculateML2(std::vector<std::vector<float> >& vectors, std::vector<Cluster>& clusters) {
        double d = 0.0;
        size_t count = 0;
        for (auto cit = clusters.begin(); cit != clusters.end(); ++cit) {
            count += (*cit).members.size();
            double localD = 0.0;
            for (auto mit = (*cit).members.begin(); mit != (*cit).members.end(); ++mit) {
                double distance = distanceL2((*cit).centroid, vectors[(*mit).vectorID]);
                d += distance;
                localD += distance;
            }
        }
        if (vectors.size() != count) {
//            std::cerr << "Warning! vectors.size() != count" << std::endl;
            if (NGT_LOG_DEBUG_)
                (*NGT_LOG_DEBUG_)("Warning! vectors.size() != count");
        }

        return d / (double)vectors.size();
    }

    static double
    calculateML2FromSpecifiedCentroids(std::vector<std::vector<float> >& vectors, std::vector<Cluster>& clusters,
                                       std::vector<size_t>& centroidIds) {
        double d = 0.0;
        size_t count = 0;
        for (auto it = centroidIds.begin(); it != centroidIds.end(); ++it) {
            Cluster& cluster = clusters[(*it)];
            count += cluster.members.size();
            for (auto mit = cluster.members.begin(); mit != cluster.members.end(); ++mit) {
                d += distanceL2(cluster.centroid, vectors[(*mit).vectorID]);
            }
        }
        return d / (double)vectors.size();
    }

    void
    setupInitialClusters(std::vector<std::vector<float> >& vectors, size_t numberOfClusters,
                         std::vector<Cluster>& clusters) {
        if (clusters.empty()) {
            switch (initializationMode) {
                case InitializationModeHead: {
                    getInitialCentroidsFromHead(vectors, clusters, numberOfClusters);
                    break;
                }
                case InitializationModeRandom: {
                    getInitialCentroidsRandomly(vectors, clusters, numberOfClusters, 0);
                    break;
                }
                case InitializationModeKmeansPlusPlus: {
                    getInitialCentroidsKmeansPlusPlus(vectors, clusters, numberOfClusters);
                    break;
                }
                default:
//                    std::cerr << "proper initMode is not specified." << std::endl;
                    if (NGT_LOG_DEBUG_)
                        (*NGT_LOG_DEBUG_)("proper initMode is not specified.");
                    exit(1);
            }
        }
    }

    bool
    kmeans(std::vector<std::vector<float> >& vectors, size_t numberOfClusters, std::vector<Cluster>& clusters) {
        setupInitialClusters(vectors, numberOfClusters, clusters);

        switch (clusteringType) {
            case ClusteringTypeKmeansWithoutNGT:
                return kmeansWithoutNGT(vectors, numberOfClusters, clusters);
                break;
            case ClusteringTypeKmeansWithNGT:
                return kmeansWithNGT(vectors, numberOfClusters, clusters);
                break;
            default:
//                cerr << "kmeans::fatal error!. invalid clustering type. " << clusteringType << endl;
                if (NGT_LOG_DEBUG_)
                    (*NGT_LOG_DEBUG_)("kmeans::fatal error!. invalid clustering type. " + std::to_string(clusteringType));
                abort();
                break;
        }
    }

    static void
    evaluate(std::vector<std::vector<float> >& vectors, std::vector<Cluster>& clusters, char mode,
             std::vector<size_t> centroidIds = std::vector<size_t>()) {
        size_t clusterSize = std::numeric_limits<size_t>::max();
        assign(vectors, clusters, clusterSize);

//        std::cout << "The number of vectors=" << vectors.size() << std::endl;
//        std::cout << "The number of centroids=" << clusters.size() << std::endl;
        if (centroidIds.size() == 0) {
            switch (mode) {
                case 'e':
//                    std::cout << "MSE=" << calculateMSE(vectors, clusters) << std::endl;
                    break;
                case '2':
                default:
//                    std::cout << "ML2=" << calculateML2(vectors, clusters) << std::endl;
                    break;
            }
        } else {
            switch (mode) {
                case 'e':
                    break;
                case '2':
                default:
//                    std::cout << "ML2=" << calculateML2FromSpecifiedCentroids(vectors, clusters, centroidIds)
//                              << std::endl;
                    break;
            }
        }
    }

    ClusteringType clusteringType;
    InitializationMode initializationMode;
    size_t numberOfClusters;
    bool clusterSizeConstraint;
    size_t maximumIteration;
    float epsilonFrom;
    float epsilonTo;
    float epsilonStep;
    size_t resultSizeCoefficient;
    vector<double> diffHistory;
};

}  // namespace NGT
