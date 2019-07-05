#include "VectorSource.h"
#include "ExecutionEngine.h"
#include "EngineFactory.h"
#include "Log.h"
#include "metrics/Metrics.h"

namespace zilliz {
namespace milvus {
namespace engine {


VectorSource::VectorSource(const size_t &n,
                           const float *vectors) :
                           n_(n),
                           vectors_(vectors),
                           id_generator_(new SimpleIDGenerator()) {
    current_num_vectors_added = 0;
}

Status VectorSource::Add(const ExecutionEnginePtr& execution_engine,
                         const meta::TableFileSchema& table_file_schema,
                         const size_t& num_vectors_to_add,
                         size_t& num_vectors_added) {

    auto start_time = METRICS_NOW_TIME;

    num_vectors_added = current_num_vectors_added + num_vectors_to_add <= n_ ? num_vectors_to_add : n_ - current_num_vectors_added;
    IDNumbers vector_ids_to_add;
    id_generator_->GetNextIDNumbers(num_vectors_added, vector_ids_to_add);
    Status status = execution_engine->AddWithIds(num_vectors_added, vectors_ + current_num_vectors_added, vector_ids_to_add.data());
    if (status.ok()) {
        current_num_vectors_added += num_vectors_added;
        vector_ids_.insert(vector_ids_.end(), vector_ids_to_add.begin(), vector_ids_to_add.end());
    }
    else {
        ENGINE_LOG_ERROR << "VectorSource::Add failed: " + status.ToString();
    }

    auto end_time = METRICS_NOW_TIME;
    auto total_time = METRICS_MICROSECONDS(start_time, end_time);
    server::Metrics::GetInstance().AddVectorsPerSecondGaugeSet(static_cast<int>(n_), static_cast<int>(table_file_schema.dimension_), total_time);

    return status;
}

size_t VectorSource::GetNumVectorsAdded() {
    return current_num_vectors_added;
}

bool VectorSource::AllAdded() {
    return (current_num_vectors_added == n_);
}

IDNumbers VectorSource::GetVectorIds() {
    return vector_ids_;
}

} // namespace engine
} // namespace milvus
} // namespace zilliz