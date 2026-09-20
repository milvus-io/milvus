#include "segcore/storagev1translator/InterimSealedIndexTranslator.h"

#include <algorithm>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/Chunk.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/OffsetMapping.h"
#include "fmt/core.h"
#include "index/Families.h"
#include "index/IndexTypeAdapter.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "index/contracts/build/IReaderConvertible.h"
#include "index/vector/VectorMemBuilder.h"
#include "index/vector/VectorTypeUtils.h"
#include "knowhere/operands.h"
#include "mmap/ChunkedColumnInterface.h"
#include "segcore/Utils.h"

namespace milvus::segcore::storagev1translator {
namespace {

template <typename T>
std::span<const typename index::VectorBuildInput<T>::value_type>
ChunkValues(const cachinglayer::PinWrapper<Chunk*>& pin,
            int64_t rows,
            int64_t dim) {
    using ValueType = typename index::VectorBuildInput<T>::value_type;
    AssertInfo(rows >= 0, "interim vector chunk row count is negative");
    const auto* chunk = pin.get();
    AssertInfo(chunk != nullptr, "interim vector chunk is null");
    AssertInfo(rows == 0 || chunk->Data() != nullptr,
               "non-empty interim vector chunk has no data");
    if constexpr (std::is_same_v<T, sparse_u32_f32>) {
        return {reinterpret_cast<const ValueType*>(chunk->Data()),
                static_cast<size_t>(rows)};
    } else {
        AssertInfo(dim > 0, "dense interim vector dimension is invalid");
        AssertInfo(rows <= std::numeric_limits<int64_t>::max() / dim,
                   "interim vector chunk value count overflows int64");
        return {reinterpret_cast<const ValueType*>(chunk->Data()),
                static_cast<size_t>(rows * dim)};
    }
}

template <typename T>
std::unique_ptr<index::IIndexReaderBase>
BuildInterimReader(
    const std::shared_ptr<
        std::vector<cachinglayer::PinWrapper<Chunk*>>>& pinned_chunks,
    int64_t logical_rows,
    int64_t physical_rows,
    int64_t dim,
    ValidityView validity,
    const std::vector<int64_t>& physical_rows_per_chunk,
    DataType value_type,
    const IndexType& index_type,
    const MetricType& metric_type,
    IndexVersion index_version,
    const Config& build_config,
    std::optional<knowhere::ViewDataOp> view_data) {
    AssertInfo(pinned_chunks->size() == physical_rows_per_chunk.size(),
               "interim vector chunk metadata size mismatch");
    using ValueType = typename index::VectorBuildInput<T>::value_type;
    std::vector<std::span<const ValueType>> chunks;
    chunks.reserve(pinned_chunks->size());
    for (size_t chunk_id = 0; chunk_id < pinned_chunks->size(); ++chunk_id) {
        chunks.emplace_back(ChunkValues<T>((*pinned_chunks)[chunk_id],
                                           physical_rows_per_chunk[chunk_id],
                                           dim));
    }

    index::VectorMemBuilder<T> builder =
        view_data.has_value() ? index::VectorMemBuilder<T>(DataType::NONE,
                                                           index_type,
                                                           metric_type,
                                                           index_version,
                                                           dim,
                                                           build_config,
                                                           std::move(*view_data),
                                                           false)
                              : index::VectorMemBuilder<T>(DataType::NONE,
                                                           index_type,
                                                           metric_type,
                                                           index_version,
                                                           dim,
                                                           build_config,
                                                           false);
    index::InterimVectorBuildInput<T> input{
        .physical_chunks = chunks,
        .logical_rows = logical_rows,
        .physical_rows = physical_rows,
        .dim = dim,
        .parent_validity = validity,
    };
    auto artifact = std::move(builder).Build(input);
    auto reader = index::IReaderConvertible::FromArtifact(std::move(artifact));
    AssertInfo(reader->ValueType() == value_type,
               "interim vector reader type {} disagrees with field type {}",
               reader->ValueType(),
               value_type);
    return reader;
}

}  // namespace

InterimSealedIndexTranslator::InterimSealedIndexTranslator(
    std::shared_ptr<ChunkedColumnInterface> vec_data,
    int64_t segment_id,
    int64_t field_id,
    knowhere::IndexType index_type,
    knowhere::MetricType metric_type,
    IndexVersion index_version,
    knowhere::Json build_config,
    int64_t dim,
    bool is_sparse,
    DataType vec_data_type,
    const std::string& warmup_policy)
    : vec_data_(vec_data),
      segment_id_(segment_id),
      index_type_(index_type),
      metric_type_(metric_type),
      index_version_(index_version),
      build_config_(build_config),
      dim_(dim),
      is_sparse_(is_sparse),
      vec_data_type_(vec_data_type),
      index_key_(fmt::format("seg_{}_ii_{}", segment_id, field_id)),
      meta_(milvus::cachinglayer::StorageType::MEMORY,
            milvus::cachinglayer::CellIdMappingMode::ALWAYS_ZERO,
            milvus::segcore::getCellDataType(
                /* is_vector */ true,
                /* is_index */ true),
            milvus::segcore::getCacheWarmupPolicy(warmup_policy,
                                                  /* is_vector */ true,
                                                  /* is_index */ true),
            /* support_eviction */ false) {
    AssertInfo(vec_data_ != nullptr, "interim vector column is null");
    AssertInfo(vec_data_->NumRows() <=
                   static_cast<size_t>(std::numeric_limits<int64_t>::max()),
               "interim vector logical row count exceeds int64");
    AssertInfo(is_sparse_ ==
                   IsSparseFloatVectorDataType(vec_data_type_),
               "interim sparse flag disagrees with vector field type {}",
               vec_data_type_);
    AssertInfo(is_sparse_ || vec_data_type_ == DataType::VECTOR_FLOAT ||
                   vec_data_type_ == DataType::VECTOR_FLOAT16 ||
                   vec_data_type_ == DataType::VECTOR_BFLOAT16,
               "unsupported interim vector field type {}",
               vec_data_type_);

    build_config_[index::METRIC_TYPE] = metric_type_;
    build_config_[DIM_KEY] = dim_;
    build_config_[INDEX_NUM_ROWS_KEY] =
        static_cast<int64_t>(vec_data_->NumRows());
    auto adapted = index::AdaptIndexType({
        .index_type = index_type_,
        .field_type = vec_data_type_,
        .element_type = DataType::NONE,
        // #52361: the interim index is built at the configured target engine
        // version, so the adapter must classify it at the same version.
        .index_engine_version = index_version_,
        .params = std::move(build_config_),
        .is_nested = false,
        .is_text_match = false,
    });
    AssertInfo(adapted.family == index::families::kVectorMem,
               "interim vector index {} is not an in-memory family",
               index_type_);
    build_config_ = std::move(adapted.params);
    auto family = std::move(adapted.family);
    const auto loader = index::LoaderRegistry::Instance().Lookup(family);
    AssertInfo(static_cast<bool>(loader),
               "no index loader is registered for family {}",
               family);
    SetReaderContract(std::move(family),
                      vec_data_type_,
                      loader.derive_caps(build_config_));
}

size_t
InterimSealedIndexTranslator::num_cells() const {
    return 1;
}

milvus::cachinglayer::cid_t
InterimSealedIndexTranslator::cell_id_of(
    milvus::cachinglayer::uid_t uid) const {
    return 0;
}

std::pair<milvus::cachinglayer::ResourceUsage,
          milvus::cachinglayer::ResourceUsage>
InterimSealedIndexTranslator::estimated_byte_size_of_cell(
    milvus::cachinglayer::cid_t cid) const {
    int64_t size = vec_data_->DataByteSize();
    int64_t row_count = vec_data_->NumRows();
    // TODO: hack, move these estimate logic to knowhere
    // ignore the size of centroids
    if (index_type_ == knowhere::IndexEnum::INDEX_FAISS_SCANN_DVR) {
        int64_t vec_size =
            int64_t(index::GetValueFromConfig<int>(
                        build_config_, knowhere::indexparam::SUB_DIM)
                        .value() /
                    8 * dim_);
        if (build_config_[knowhere::indexparam::REFINE_TYPE] ==
            knowhere::RefineType::UINT8_QUANT) {
            vec_size += dim_ * 1;
        } else if (build_config_[knowhere::indexparam::REFINE_TYPE] ==
                       knowhere::RefineType::FLOAT16_QUANT ||
                   build_config_[knowhere::indexparam::REFINE_TYPE] ==
                       knowhere::RefineType::BFLOAT16_QUANT) {
            vec_size += dim_ * 2;
        }  // else knowhere::RefineType::DATA_VIEW, no extra size
        return {{vec_size * row_count, 0},
                {static_cast<int64_t>(vec_size * row_count + size * 0.5), 0}};
    } else if (index_type_ == knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC) {
        // fp16/bf16 all use float32 to build index
        int64_t fp32_size = row_count * sizeof(float) * dim_;
        return {{fp32_size, 0}, {static_cast<int64_t>(fp32_size * 0.5), 0}};
    } else {
        // SPARSE_WAND_CC and SPARSE_INVERTED_INDEX_CC basically has the same size as the
        // raw data.
        return {{size, 0}, {static_cast<int64_t>(size * 2.0), 0}};
    }
}

const std::string&
InterimSealedIndexTranslator::key() const {
    return index_key_;
}

std::vector<std::pair<milvus::cachinglayer::cid_t,
                      std::unique_ptr<milvus::index::IIndexReaderBase>>>
InterimSealedIndexTranslator::get_cells(
    milvus::OpContext* ctx,
    const std::vector<milvus::cachinglayer::cid_t>& cids) {
    // Check for cancellation before building interim index
    CheckCancellation(
        ctx, segment_id_, "InterimSealedIndexTranslator::get_cells()");

    const auto num_chunks = vec_data_->num_chunks();
    AssertInfo(num_chunks >= 0,
               "interim vector column has a negative chunk count");
    if (vec_data_->IsNullable()) {
        vec_data_->BuildValidRowIds(ctx);
    }
    const auto& offset_mapping = vec_data_->GetOffsetMapping();
    const bool nullable = offset_mapping.IsEnabled();
    AssertInfo(vec_data_->NumRows() <=
                   static_cast<size_t>(std::numeric_limits<int64_t>::max()),
               "interim vector logical row count exceeds int64");
    const auto logical_rows = static_cast<int64_t>(vec_data_->NumRows());
    const auto physical_rows =
        nullable ? offset_mapping.GetValidCount() : logical_rows;

    auto pinned_chunks = std::make_shared<
        std::vector<cachinglayer::PinWrapper<Chunk*>>>(
        vec_data_->GetAllChunks(ctx));
    AssertInfo(pinned_chunks->size() == static_cast<size_t>(num_chunks),
               "interim vector column returned {} pins for {} chunks",
               pinned_chunks->size(),
               num_chunks);
    std::vector<int64_t> physical_rows_per_chunk;
    physical_rows_per_chunk.reserve(num_chunks);
    auto rows_until_chunk = std::make_shared<std::vector<int64_t>>();
    rows_until_chunk->reserve(num_chunks + 1);
    rows_until_chunk->push_back(0);
    for (int64_t chunk_id = 0; chunk_id < num_chunks; ++chunk_id) {
        const auto chunk_rows =
            nullable ? vec_data_->GetValidCountInChunk(chunk_id)
                     : vec_data_->chunk_row_nums(chunk_id);
        AssertInfo(chunk_rows >= 0 &&
                       chunk_rows <= std::numeric_limits<int64_t>::max() -
                                         rows_until_chunk->back(),
                   "interim vector chunk row count overflows int64");
        physical_rows_per_chunk.push_back(chunk_rows);
        rows_until_chunk->push_back(rows_until_chunk->back() + chunk_rows);
    }
    AssertInfo(rows_until_chunk->back() == physical_rows,
               "interim vector chunks contain {} physical rows, expected {}",
               rows_until_chunk->back(),
               physical_rows);

    ValidityView validity;
    if (nullable) {
        validity =
            ValidityView::FromExpanded(vec_data_->GetValidData().data());
    }

    std::unique_ptr<index::IIndexReaderBase> reader;
    if (is_sparse_) {
        reader = BuildInterimReader<sparse_u32_f32>(pinned_chunks,
                                                    logical_rows,
                                                    physical_rows,
                                                    dim_,
                                                    validity,
                                                    physical_rows_per_chunk,
                                                    vec_data_type_,
                                                    index_type_,
                                                    metric_type_,
                                                    index_version_,
                                                    build_config_,
                                                    std::nullopt);
    } else {
        knowhere::ViewDataOp view_data =
            [pinned_chunks, rows_until_chunk, num_chunks](size_t id) {
                AssertInfo(
                    id <= static_cast<size_t>(
                              std::numeric_limits<int64_t>::max()),
                    "compact interim vector offset {} exceeds int64",
                    id);
                auto compact_offset = static_cast<int64_t>(id);
                AssertInfo(compact_offset < rows_until_chunk->back(),
                           "compact interim vector offset {} is out of range",
                           id);
                auto it = std::upper_bound(rows_until_chunk->begin(),
                                           rows_until_chunk->end(),
                                           compact_offset);
                AssertInfo(it != rows_until_chunk->begin(),
                           "compact interim vector offset {} is out of range",
                           id);
                const auto chunk_id =
                    std::distance(rows_until_chunk->begin(), it) - 1;
                AssertInfo(chunk_id < num_chunks,
                           "compact interim vector offset {} is out of range",
                           id);
                compact_offset -= (*rows_until_chunk)[chunk_id];
                const auto* chunk = (*pinned_chunks)[chunk_id].get();
                AssertInfo(chunk != nullptr,
                           "interim vector chunk {} is null",
                           chunk_id);
                return static_cast<const void*>(
                    chunk->ValueAt(compact_offset));
            };

        reader = index::DispatchPhysicalVectorDataType(
            vec_data_type_,
            [&]<typename T>() -> std::unique_ptr<index::IIndexReaderBase> {
                if constexpr (std::is_same_v<T, float> ||
                              std::is_same_v<T, float16> ||
                              std::is_same_v<T, bfloat16>) {
                    return BuildInterimReader<T>(pinned_chunks,
                                                 logical_rows,
                                                 physical_rows,
                                                 dim_,
                                                 validity,
                                                 physical_rows_per_chunk,
                                                 vec_data_type_,
                                                 index_type_,
                                                 metric_type_,
                                                 index_version_,
                                                 build_config_,
                                                 std::move(view_data));
                } else {
                    ThrowInfo(DataTypeInvalid,
                              "unsupported interim vector field type {}",
                              vec_data_type_);
                }
            },
            [&]() -> std::unique_ptr<index::IIndexReaderBase> {
                ThrowInfo(DataTypeInvalid,
                          "unsupported interim vector field type {}",
                          vec_data_type_);
            });
    }

    AssertInfo(reader != nullptr, "interim vector builder returned no reader");
    std::vector<std::pair<cid_t, std::unique_ptr<index::IIndexReaderBase>>>
        result;
    result.emplace_back(std::make_pair(0, std::move(reader)));
    return result;
}

milvus::cachinglayer::Meta*
InterimSealedIndexTranslator::meta() {
    return &meta_;
}

}  // namespace milvus::segcore::storagev1translator
