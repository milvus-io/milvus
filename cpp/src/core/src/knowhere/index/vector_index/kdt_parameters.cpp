
#include <mutex>
#include "knowhere/index/vector_index/kdt_parameters.h"


namespace zilliz {
namespace knowhere {

const std::vector<KDTParameter> &
KDTParameterManagement::GetKDTParameters() {
    return kdt_parameters_;
}

KDTParameterManagement::KDTParameterManagement() {
    kdt_parameters_ = std::vector<KDTParameter>{
        {"KDTNumber", "1"},
        {"NumTopDimensionKDTSplit", "5"},
        {"NumSamplesKDTSplitConsideration", "100"},

        {"TPTNumber", "32"},
        {"TPTLeafSize", "2000"},
        {"NumTopDimensionTPTSplit", "5"},

        {"NeighborhoodSize", "32"},
        {"GraphNeighborhoodScale", "2"},
        {"GraphCEFScale", "2"},
        {"RefineIterations", "0"},
        {"CEF", "1000"},
        {"MaxCheckForRefineGraph", "10000"},

        {"NumberOfThreads", "1"},

        {"MaxCheck", "8192"},
        {"ThresholdOfNumberOfContinuousNoBetterPropagation", "3"},
        {"NumberOfInitialDynamicPivots", "50"},
        {"NumberOfOtherDynamicPivots", "4"},
    };
}

} // namespace knowhere
} // namespace zilliz
