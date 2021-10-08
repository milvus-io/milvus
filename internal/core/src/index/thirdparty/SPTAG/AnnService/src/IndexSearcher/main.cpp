// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Helper/SimpleIniReader.h"
#include "inc/Helper/CommonHelper.h"
#include "inc/Core/Common.h"
#include "inc/Core/MetadataSet.h"
#include "inc/Core/VectorIndex.h"
#include "inc/Core/SearchQuery.h"
#include "inc/Core/Common/WorkSpace.h"
#include "inc/Core/Common/DataUtils.h"
#include <iomanip>
#include <set>

using namespace SPTAG;

template <typename T>
float CalcRecall(std::vector<QueryResult> &results, const std::vector<std::set<SizeType>> &truth, SizeType NumQuerys, int K, std::ofstream& log)
{
    float meanrecall = 0, minrecall = MaxDist, maxrecall = 0, stdrecall = 0;
    std::vector<float> thisrecall(NumQuerys, 0);
    for (SizeType i = 0; i < NumQuerys; i++)
    {
        for (SizeType id : truth[i])
        {
            for (int j = 0; j < K; j++)
            {
                if (results[i].GetResult(j)->VID == id)
                {
                    thisrecall[i] += 1;
                    break;
                }
            }
        }
        thisrecall[i] /= K;
        meanrecall += thisrecall[i];
        if (thisrecall[i] < minrecall) minrecall = thisrecall[i];
        if (thisrecall[i] > maxrecall) maxrecall = thisrecall[i];
    }
    meanrecall /= NumQuerys;
    for (SizeType i = 0; i < NumQuerys; i++)
    {
        stdrecall += (thisrecall[i] - meanrecall) * (thisrecall[i] - meanrecall);
    }
    stdrecall = std::sqrt(stdrecall / NumQuerys);
    log << meanrecall << " " << stdrecall << " " << minrecall << " " << maxrecall << std::endl;
    return meanrecall;
}

void LoadTruth(std::ifstream& fp, std::vector<std::set<SizeType>>& truth, SizeType NumQuerys, int K)
{
    SizeType get;
    std::string line;
    for (SizeType i = 0; i < NumQuerys; ++i)
    {
        truth[i].clear();
        for (int j = 0; j < K; ++j)
        {
            fp >> get;
            truth[i].insert(get);
        }
        std::getline(fp, line);
    }
}

template <typename T>
int Process(Helper::IniReader& reader, VectorIndex& index)
{
    std::string queryFile = reader.GetParameter("Index", "QueryFile", std::string("querys.bin"));
    std::string truthFile = reader.GetParameter("Index", "TruthFile", std::string("truth.txt"));
    std::string outputFile = reader.GetParameter("Index", "ResultFile", std::string(""));

    SizeType numBatchQuerys = reader.GetParameter("Index", "NumBatchQuerys", (SizeType)10000);
    SizeType numDebugQuerys = reader.GetParameter("Index", "NumDebugQuerys", (SizeType)-1);
    int K = reader.GetParameter("Index", "K", 32);

    std::vector<std::string> maxCheck = Helper::StrUtils::SplitString(reader.GetParameter("Index", "MaxCheck", std::string("2048")), "#");

    std::ifstream inStream(queryFile);
    std::ifstream ftruth(truthFile);
    std::ofstream fp;
    if (!inStream.is_open())
    {
        std::cout << "ERROR: Cannot Load Query file " << queryFile << "!" << std::endl;
        return -1;
    }
    if (outputFile != "")
    {
        fp.open(outputFile);
        if (!fp.is_open())
        {
            std::cout << "ERROR: Cannot open " << outputFile << " for write!" << std::endl;
        }
    }

    std::ofstream log(index.GetIndexName() + "_" + std::to_string(K) + ".txt");
    if (!log.is_open())
    {
        std::cout << "ERROR: Cannot open logging file!" << std::endl;
        return -1;
    }

    SizeType numQuerys = (numDebugQuerys >= 0) ? numDebugQuerys : numBatchQuerys;

    std::vector<std::vector<T>> Query(numQuerys, std::vector<T>(index.GetFeatureDim(), 0)); 
    std::vector<std::set<SizeType>> truth(numQuerys);
    std::vector<QueryResult> results(numQuerys, QueryResult(NULL, K, 0));

    clock_t * latencies = new clock_t[numQuerys + 1];

    int base = 1;
    if (index.GetDistCalcMethod() == DistCalcMethod::Cosine) {
        base = COMMON::Utils::GetBase<T>();
    }
    int basesquare = base * base;

    DimensionType dims = index.GetFeatureDim();
    std::vector<std::string> QStrings;
    while (!inStream.eof())
    {
        QStrings.clear();
        COMMON::Utils::PrepareQuerys(inStream, QStrings, Query, numQuerys, dims, index.GetDistCalcMethod(), base);
        if (numQuerys == 0) break;

        for (SizeType i = 0; i < numQuerys; i++) results[i].SetTarget(Query[i].data());
        if (ftruth.is_open()) LoadTruth(ftruth, truth, numQuerys, K);

        std::cout << "    \t[avg]      \t[99%] \t[95%] \t[recall] \t[mem]" << std::endl;

        SizeType subSize = (numQuerys - 1) / omp_get_num_threads() + 1;
        for (std::string& mc : maxCheck)
        {
            index.SetParameter("MaxCheck", mc.c_str());
            for (SizeType i = 0; i < numQuerys; i++) results[i].Reset();

#pragma omp parallel for
            for (int tid = 0; tid < omp_get_num_threads(); tid++)
            {
                SizeType start = tid * subSize;
                SizeType end = min((tid + 1) * subSize, numQuerys);
                for (SizeType i = start; i < end; i++)
                {
                    latencies[i] = clock();
                    index.SearchIndex(results[i]);
                }
            }

            latencies[numQuerys] = clock();

            float timeMean = 0, timeMin = MaxDist, timeMax = 0, timeStd = 0;
            for (SizeType i = 0; i < numQuerys; i++)
            {
                if (latencies[i + 1] >= latencies[i])
                    latencies[i] = latencies[i + 1] - latencies[i];
                else
                    latencies[i] = latencies[numQuerys] - latencies[i];
                timeMean += latencies[i];
                if (latencies[i] > timeMax) timeMax = (float)latencies[i];
                if (latencies[i] < timeMin) timeMin = (float)latencies[i];
            }
            timeMean /= numQuerys;
            for (SizeType i = 0; i < numQuerys; i++) timeStd += ((float)latencies[i] - timeMean) * ((float)latencies[i] - timeMean);
            timeStd = std::sqrt(timeStd / numQuerys);
            log << timeMean << " " << timeStd << " " << timeMin << " " << timeMax << " ";

            std::sort(latencies, latencies + numQuerys, [](clock_t x, clock_t y)
            {
                return x < y;
            });
            float l99 = float(latencies[SizeType(numQuerys * 0.99)]) / CLOCKS_PER_SEC;
            float l95 = float(latencies[SizeType(numQuerys * 0.95)]) / CLOCKS_PER_SEC;

            float recall = 0;
            if (ftruth.is_open())
            {
                recall = CalcRecall<T>(results, truth, numQuerys, K, log);
            }

#ifndef _MSC_VER
            struct rusage rusage;
            getrusage(RUSAGE_SELF, &rusage);
            unsigned long long peakWSS = rusage.ru_maxrss * 1024 / 1000000000;
#else
            PROCESS_MEMORY_COUNTERS pmc;
            GetProcessMemoryInfo(GetCurrentProcess(), &pmc, sizeof(pmc));
            unsigned long long peakWSS = pmc.PeakWorkingSetSize / 1000000000;
#endif
            std::cout << mc << "\t" << std::fixed << std::setprecision(6) << (timeMean / CLOCKS_PER_SEC) << "\t" << std::setprecision(4) << l99 << "\t" << l95 << "\t" << recall << "\t\t" << peakWSS << "GB" << std::endl;

        }
        
        if (fp.is_open())
        {
            fp << std::setprecision(3) << std::fixed;
            for (SizeType i = 0; i < numQuerys; i++)
            {
                fp << QStrings[i] << ":";
                for (int j = 0; j < K; j++)
                {
                    if (results[i].GetResult(j)->VID < 0) {
                        fp << results[i].GetResult(j)->Dist << "@" << results[i].GetResult(j)->VID << std::endl;
                    }
                    else {
                        ByteArray vm = index.GetMetadata(results[i].GetResult(j)->VID);
                        fp << (results[i].GetResult(j)->Dist / basesquare) << "@";
                        fp.write((const char*)vm.Data(), vm.Length());
                    }
                    fp << "|";
                }
                fp << std::endl;
            }
        }
        
        if (numQuerys < numBatchQuerys || numDebugQuerys >= 0) break;
    }
    std::cout << "Output results finish!" << std::endl;

    inStream.close();
    fp.close();
    log.close();
    ftruth.close();
    delete[] latencies;

    QStrings.clear();
    results.clear();

    return 0;
}

int main(int argc, char** argv)
{
    if (argc < 2)
    {
        std::cerr << "IndexSearcher.exe folder" << std::endl;
        return -1;
    }

    std::shared_ptr<SPTAG::VectorIndex> vecIndex;
    auto ret = SPTAG::VectorIndex::LoadIndex(argv[1], vecIndex);
    if (SPTAG::ErrorCode::Success != ret || nullptr == vecIndex)
    {
        std::cerr << "Cannot open configure file!" << std::endl;
        return -1;
    }

    Helper::IniReader iniReader;
    for (int i = 1; i < argc; i++)
    {
        std::string param(argv[i]);
        size_t idx = param.find("=");
        if (idx == std::string::npos) continue;

        std::string paramName = param.substr(0, idx);
        std::string paramVal = param.substr(idx + 1);
        std::string sectionName;
        idx = paramName.find(".");
        if (idx != std::string::npos) {
            sectionName = paramName.substr(0, idx);
            paramName = paramName.substr(idx + 1);
        }
        iniReader.SetParameter(sectionName, paramName, paramVal);
        std::cout << "Set [" << sectionName << "]" << paramName << " = " << paramVal << std::endl;
    }

    switch (vecIndex->GetVectorValueType())
    {
#define DefineVectorValueType(Name, Type) \
    case VectorValueType::Name: \
        Process<Type>(iniReader, *(vecIndex.get())); \
        break; \

#include "inc/Core/DefinitionList.h"
#undef DefineVectorValueType

    default: break;
    }
    return 0;
}
