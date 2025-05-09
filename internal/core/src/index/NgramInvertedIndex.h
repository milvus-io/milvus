#pragma once
#include <string>
#include <boost/filesystem.hpp>
#include <optional>

#include "exec/expression/Expr.h"
#include "index/InvertedIndexTantivy.h"
#include "segcore/SegmentInterface.h"

namespace milvus::index {
class NgramInvertedIndex : public InvertedIndexTantivy<std::string> {
 public:
    explicit NgramInvertedIndex(const storage::FileManagerContext& ctx,
                                uintptr_t min_gram,
                                uintptr_t max_gram);

    ~NgramInvertedIndex() override{};

 public:
    //  IndexStatsPtr
    //  Upload(const Config& config) override;

    void
    Load(milvus::tracer::TraceContext ctx, const Config& config) override;

    void
    BuildWithFieldData(const std::vector<FieldDataPtr>& datas) override;

    std::optional<TargetBitmap>
    InnerMatchQuery(const std::string& literal, exec::SegmentExpr* segment);

 private:
    uintptr_t min_gram_{0};
    uintptr_t max_gram_{0};
    int64_t field_id_{0};
};
}  // namespace milvus::index