#include "knowhere/version.h"
#include "knowhere/dataset.h"
#include "knowhere/index_node.h"
#include "knowhere/index/index_factory.h"
#include "knowhere/index/index_node.h"
#include "knowhere/index/index_static.h"
#include "knowhere/log.h"
#include "knowhere/cluster/cluster_factory.h"
#include "knowhere/comp/brute_force.h"
#include "knowhere/comp/knowhere_check.h"
#include "knowhere/comp/materialized_view.h"
#include "knowhere/comp/time_recorder.h"
#include "knowhere/operands.h"
#include "knowhere/prometheus_client.h"
#include "knowhere/sparse_utils.h"
#include "knowhere/utils.h"
#include "knowhere/comp/knowhere_config.h"

#include <cstdint>
#include <type_traits>

int
main() {
    static_assert(std::is_same_v<knowhere::int8, int8_t>,
                  "knowhere::int8 must match upstream int8_t alias");

    LOG_KNOWHERE_INFO_ << "compile-surface";
    knowhere::expected<knowhere::Resource> resource_expected;
    knowhere::Status error_status = knowhere::Status::not_implemented;
    knowhere::expected<knowhere::DataSetPtr> error_expected(error_status);
    auto ds = knowhere::GenDataSet(1, 4, nullptr);
    ds->SetIsSparse(true);
    auto dataset_sparse = ds->IsSparse();
    (void)ds;
    (void)dataset_sparse;
    auto version = knowhere::Version::GetCurrentVersion().VersionNumber();
    auto min_version = knowhere::Version::GetMinimalVersion().VersionNumber();
    auto max_version = knowhere::Version::GetMaximumVersion().VersionNumber();
    auto status_string = knowhere::Status2String(knowhere::Status::success);
    (void)version;
    (void)min_version;
    (void)max_version;
    (void)status_string;
    (void)resource_expected;
    (void)error_expected;

    knowhere::fp16 fp16_value{};
    knowhere::bf16 bf16_value{};
    knowhere::bin1 bin_value{};
    knowhere::sparse_u32_f32 sparse_value{};
    knowhere::sparse_u32_f32::ValueType sparse_scalar = 1.0F;
    (void)fp16_value;
    (void)bf16_value;
    (void)bin_value;
    (void)sparse_value;
    (void)sparse_scalar;

    auto metric_match = knowhere::IsMetricType("l2", knowhere::metric::L2);
    auto flat_index = knowhere::IsFlatIndex(knowhere::IndexEnum::INDEX_FAISS_IDMAP);
    auto hnsw_is_flat = knowhere::IsFlatIndex(knowhere::IndexEnum::INDEX_HNSW);
    auto ivfsq = knowhere::IndexEnum::INDEX_FAISS_IVFSQ8;
    knowhere::IndexVersion index_version = 1;
    auto max_sim = knowhere::metric::MAX_SIM;
    auto max_sim_cosine = knowhere::metric::MAX_SIM_COSINE;
    auto max_sim_ip = knowhere::metric::MAX_SIM_IP;
    auto max_sim_l2 = knowhere::metric::MAX_SIM_L2;
    auto hamming = knowhere::metric::HAMMING;
    auto jaccard = knowhere::metric::JACCARD;
    auto superstructure = knowhere::metric::SUPERSTRUCTURE;
    auto substructure = knowhere::metric::SUBSTRUCTURE;
    auto mhjaccard = knowhere::metric::MHJACCARD;
    auto max_sim_hamming = knowhere::metric::MAX_SIM_HAMMING;
    auto max_sim_jaccard = knowhere::metric::MAX_SIM_JACCARD;
    auto bm25_k1 = knowhere::meta::BM25_K1;
    auto bm25_b = knowhere::meta::BM25_B;
    auto bm25_avgdl = knowhere::meta::BM25_AVGDL;
    auto input_beg_id = knowhere::meta::INPUT_BEG_ID;
    auto refine_type = knowhere::RefineType::DATA_VIEW;
    auto vec_type = knowhere::VecType::FLOAT_VECTOR;
    knowhere::MaterializedViewSearchInfo materialized_view_search_info;
    materialized_view_search_info.field_id_to_touched_categories_cnt[7] = 2;
    materialized_view_search_info.is_pure_and = false;
    materialized_view_search_info.has_not = true;
    nlohmann::json materialized_view_json;
    materialized_view_json[knowhere::meta::MATERIALIZED_VIEW_SEARCH_INFO] =
        materialized_view_search_info;
    auto materialized_view_roundtrip =
        materialized_view_json[knowhere::meta::MATERIALIZED_VIEW_SEARCH_INFO]
            .get<knowhere::MaterializedViewSearchInfo>();
    auto index_type_ok = knowhere::KnowhereCheck::IndexTypeAndDataTypeCheck(
        knowhere::IndexEnum::INDEX_HNSW, vec_type, false);
    auto index_static_config =
        knowhere::IndexStaticFaced<knowhere::fp32>::CreateConfig(
            knowhere::IndexEnum::INDEX_HNSW, 1);
    std::string config_check_message;
    auto index_static_config_status =
        knowhere::IndexStaticFaced<knowhere::fp32>::ConfigCheck(
            knowhere::IndexEnum::INDEX_HNSW,
            1,
            knowhere::Json::object(),
            config_check_message);
    auto index_features = knowhere::IndexFactory::Instance().GetIndexFeatures();
    constexpr uint64_t kBinaryFlag = 1ULL << 0;
    constexpr uint64_t kFloat32Flag = 1ULL << 1;
    constexpr uint64_t kSparseFloat32Flag = 1ULL << 4;
    constexpr uint64_t kInt8Flag = 1ULL << 5;
    auto hnsw_features = index_features.find(knowhere::IndexEnum::INDEX_HNSW);
    if (hnsw_features == index_features.end() ||
        (hnsw_features->second & (kFloat32Flag | kInt8Flag)) !=
            (kFloat32Flag | kInt8Flag)) {
        return 11;
    }
    auto binary_features =
        index_features.find(knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT);
    if (binary_features == index_features.end() ||
        (binary_features->second & kBinaryFlag) != kBinaryFlag) {
        return 12;
    }
    auto sparse_features =
        index_features.find(knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX);
    if (sparse_features == index_features.end() ||
        (sparse_features->second & kSparseFloat32Flag) != kSparseFloat32Flag) {
        return 13;
    }
    auto deduplicate_features = index_features.find("MINHASH_LSH");
    if (deduplicate_features == index_features.end() ||
        (deduplicate_features->second & kBinaryFlag) != kBinaryFlag) {
        return 14;
    }
    std::vector<int64_t> brute_force_ids(1, -1);
    std::vector<float> brute_force_distances(1, 0.0F);
    knowhere::sparse::label_t sparse_label = -1;
    auto brute_force_status = knowhere::BruteForce::SearchWithBuf<float>(
        ds,
        ds,
        brute_force_ids.data(),
        brute_force_distances.data(),
        knowhere::Json::object(),
        knowhere::BitsetView{});
    auto brute_force_sparse_status =
        knowhere::BruteForce::SearchSparseWithBuf(
            ds,
            ds,
            &sparse_label,
            brute_force_distances.data(),
            knowhere::Json::object(),
            knowhere::BitsetView{});
    auto brute_force_range = knowhere::BruteForce::RangeSearch<float>(
        ds, ds, knowhere::Json::object(), knowhere::BitsetView{});
    auto brute_force_iterators = knowhere::BruteForce::AnnIterator<float>(
        ds, ds, knowhere::Json::object(), knowhere::BitsetView{});
    auto mmap_ok = knowhere::KnowhereCheck::SupportMmapIndexTypeCheck(
        knowhere::IndexEnum::INDEX_HNSW);
    auto cluster = knowhere::ClusterFactory::Instance().Create<float>(
        knowhere::KMEANS_CLUSTER);
    knowhere::Index<knowhere::IndexNode> unsupported_index(
        std::make_shared<knowhere::UnsupportedIndexNode>("unsupported"));
    knowhere::TimeRecorder recorder("compile-surface", 0);
    ds->SetTensorBeginId(9);
    auto tensor_begin_id = ds->GetTensorBeginId();
    knowhere::sparse::SparseRow<knowhere::sparse_u32_f32::ValueType> sparse_row(2);
    auto sparse_element_size =
        knowhere::sparse::SparseRow<knowhere::sparse_u32_f32::ValueType>::element_size();
    auto sparse_data = sparse_row.data();
    auto sparse_bytes_view = static_cast<const uint8_t*>(sparse_row.data());
    auto sparse_size = sparse_row.size();
    auto sparse_dim = sparse_row.dim();
    auto sparse_bytes = sparse_row.data_byte_size();
    auto sparse_empty = sparse_row.empty();
    auto prometheus_metrics = knowhere::prometheusClient->GetMetrics();
    auto& prometheus_registry = knowhere::prometheusClient->GetRegistry();
    auto build_with_ptr =
        unsupported_index.Build(ds, knowhere::Json::object(), true);
    auto build_with_null =
        unsupported_index.Build(nullptr, knowhere::Json::object());
    auto add_with_ptr =
        unsupported_index.Add(ds, knowhere::Json::object(), true);
    auto search_with_ptr =
        unsupported_index.Search(
            ds, knowhere::Json::object(), knowhere::BitsetView{}, nullptr);
    auto range_search_with_ptr =
        unsupported_index.RangeSearch(
            ds, knowhere::Json::object(), knowhere::BitsetView{}, nullptr);
    auto ann_iterator_with_ptr = unsupported_index.AnnIterator(
        ds, knowhere::Json::object(), knowhere::BitsetView{}, true, nullptr);
    auto get_vector_by_ids = unsupported_index.GetVectorByIds(ds, nullptr);
    auto deserialize_from_file = unsupported_index.DeserializeFromFile(
        "/tmp/nonexistent-index", knowhere::Json::object());
    auto load_index_with_stream = unsupported_index.LoadIndexWithStream();
    auto additional_scalar_supported =
        unsupported_index.IsAdditionalScalarSupported(false);
    auto index_meta = unsupported_index.GetIndexMeta(knowhere::Json::object());
    auto index_type = unsupported_index.Type();
    recorder.RecordSection("after-sparse");
    recorder.ElapseFromBegin("done");
    (void)metric_match;
    (void)flat_index;
    (void)hnsw_is_flat;
    (void)ivfsq;
    (void)index_version;
    (void)max_sim;
    (void)max_sim_cosine;
    (void)max_sim_ip;
    (void)max_sim_l2;
    (void)hamming;
    (void)jaccard;
    (void)superstructure;
    (void)substructure;
    (void)mhjaccard;
    (void)max_sim_hamming;
    (void)max_sim_jaccard;
    (void)bm25_k1;
    (void)bm25_b;
    (void)bm25_avgdl;
    (void)input_beg_id;
    (void)refine_type;
    (void)materialized_view_roundtrip;
    (void)index_type_ok;
    (void)index_static_config;
    (void)index_static_config_status;
    (void)index_features;
    (void)brute_force_status;
    (void)brute_force_sparse_status;
    (void)brute_force_range;
    (void)brute_force_iterators;
    (void)mmap_ok;
    (void)cluster;
    (void)unsupported_index;
    (void)recorder;
    (void)tensor_begin_id;
    (void)sparse_element_size;
    (void)sparse_data;
    (void)sparse_bytes_view;
    (void)sparse_size;
    (void)sparse_dim;
    (void)sparse_bytes;
    (void)sparse_empty;
    (void)prometheus_metrics;
    (void)prometheus_registry;
    (void)build_with_ptr;
    (void)build_with_null;
    (void)add_with_ptr;
    (void)search_with_ptr;
    (void)range_search_with_ptr;
    (void)ann_iterator_with_ptr;
    (void)get_vector_by_ids;
    (void)deserialize_from_file;
    (void)load_index_with_stream;
    (void)additional_scalar_supported;
    (void)index_meta;
    (void)index_type;

    knowhere::KnowhereConfig::SetBlasThreshold(1024);
    knowhere::KnowhereConfig::SetEarlyStopThreshold(0.0);
    knowhere::KnowhereConfig::ShowVersion();
    auto simd_result = knowhere::KnowhereConfig::SetSimdType(
        knowhere::KnowhereConfig::SimdType::AUTO);
    knowhere::KnowhereConfig::EnablePatchForComputeFP32AsBF16();
    knowhere::KnowhereConfig::SetBuildThreadPoolSize(1);
    knowhere::KnowhereConfig::SetSearchThreadPoolSize(1);
    auto aio_ok = knowhere::KnowhereConfig::SetAioContextPool(1);
    knowhere::KnowhereConfig::SetFetchThreadPoolSize(1);
    knowhere::KnowhereConfig::SetRaftMemPool();
    knowhere::KnowhereConfig::SetRaftMemPool(1, 2);
    (void)simd_result;
    (void)aio_ok;
    return 0;
}
