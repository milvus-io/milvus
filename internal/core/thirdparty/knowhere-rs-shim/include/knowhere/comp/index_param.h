#pragma once

#include <cstdint>
#include <string>

namespace knowhere {

using IndexType = std::string;
using MetricType = std::string;
using IndexVersion = int32_t;

enum class RefineType {
    DATA_VIEW = 0,
    BFLOAT16_QUANT = 1,
    FLOAT16_QUANT = 2,
    UINT8_QUANT = 3,
};

struct fp32 {};
struct fp16 {};
struct bf16 {};
struct bin1 {};
struct sparse_u32_f32 {
    using ValueType = float;
};
using int8 = int8_t;

namespace IndexEnum {
inline constexpr const char* INVALID = "";
inline constexpr const char* INDEX_HNSW = "HNSW";
inline constexpr const char* INDEX_DISKANN = "DISKANN";
inline constexpr const char* INDEX_FAISS_IDMAP = "FLAT";
inline constexpr const char* INDEX_FAISS_IVFFLAT = "IVF_FLAT";
inline constexpr const char* INDEX_FAISS_IVFFLAT_CC = "IVF_FLAT_CC";
inline constexpr const char* INDEX_FAISS_IVFPQ = "IVF_PQ";
inline constexpr const char* INDEX_FAISS_IVFSQ8 = "IVF_SQ8";
inline constexpr const char* INDEX_FAISS_IVFSQ = "IVF_SQ8";
inline constexpr const char* INDEX_FAISS_BIN_IDMAP = "BIN_FLAT";
inline constexpr const char* INDEX_FAISS_BIN_IVFFLAT = "BIN_IVF_FLAT";
inline constexpr const char* INDEX_FAISS_SCANN_DVR = "SCANN";
inline constexpr const char* INDEX_SPARSE_INVERTED_INDEX = "SPARSE_INVERTED_INDEX";
inline constexpr const char* INDEX_SPARSE_INVERTED_INDEX_CC = "SPARSE_INVERTED_INDEX_CC";
inline constexpr const char* INDEX_SPARSE_WAND = "SPARSE_WAND";
inline constexpr const char* INDEX_SPARSE_WAND_CC = "SPARSE_WAND_CC";
}  // namespace IndexEnum

namespace metric {
inline constexpr const char* L2 = "L2";
inline constexpr const char* IP = "IP";
inline constexpr const char* COSINE = "COSINE";
inline constexpr const char* BM25 = "BM25";
inline constexpr const char* HAMMING = "HAMMING";
inline constexpr const char* JACCARD = "JACCARD";
inline constexpr const char* SUPERSTRUCTURE = "SUPERSTRUCTURE";
inline constexpr const char* SUBSTRUCTURE = "SUBSTRUCTURE";
inline constexpr const char* MHJACCARD = "MHJACCARD";
inline constexpr const char* MAX_SIM = "MAX_SIM";
inline constexpr const char* MAX_SIM_COSINE = "MAX_SIM_COSINE";
inline constexpr const char* MAX_SIM_IP = "MAX_SIM_IP";
inline constexpr const char* MAX_SIM_L2 = "MAX_SIM_L2";
inline constexpr const char* MAX_SIM_HAMMING = "MAX_SIM_HAMMING";
inline constexpr const char* MAX_SIM_JACCARD = "MAX_SIM_JACCARD";
}  // namespace metric

namespace meta {
inline constexpr const char* BM = "bm25_k1_b";
inline constexpr const char* DIM = "dim";
inline constexpr const char* DISTANCE = "distance";
inline constexpr const char* EMB_LIST_META = "emb_list_meta";
inline constexpr const char* EMB_LIST_OFFSET = "emb_list_offset";
inline constexpr const char* IDS = "ids";
inline constexpr const char* INDEX_TYPE = "index_type";
inline constexpr const char* INPUT_BEG_ID = "input_begin_id";
inline constexpr const char* JSON_ID_SET = "json_id_set";
inline constexpr const char* JSON_INFO = "json_info";
inline constexpr const char* LIMS = "lims";
inline constexpr const char* MATERIALIZED_VIEW_SEARCH_INFO = "materialized_view_search_info";
inline constexpr const char* BM25_K1 = "bm25_k1";
inline constexpr const char* BM25_B = "bm25_b";
inline constexpr const char* BM25_AVGDL = "bm25_avgdl";
inline constexpr const char* METRIC_TYPE = "metric_type";
inline constexpr const char* NUM_BUILD_THREAD = "num_build_thread";
inline constexpr const char* RADIUS = "radius";
inline constexpr const char* RANGE_FILTER = "range_filter";
inline constexpr const char* RANGE_SEARCH_K = "range_search_k";
inline constexpr const char* RETAIN_ITERATOR_ORDER = "retain_iterator_order";
inline constexpr const char* ROWS = "rows";
inline constexpr const char* SCALAR_INFO = "scalar_info";
inline constexpr const char* SPAN_ID = "span_id";
inline constexpr const char* TENSOR = "tensor";
inline constexpr const char* TOPK = "k";
inline constexpr const char* TRACE_FLAGS = "trace_flags";
inline constexpr const char* TRACE_ID = "trace_id";
}  // namespace meta

namespace indexparam {
inline constexpr const char* EF = "ef";
inline constexpr const char* EFCONSTRUCTION = "efConstruction";
inline constexpr const char* INVERTED_INDEX_ALGO = "inverted_index_algo";
inline constexpr const char* M = "M";
inline constexpr const char* NLIST = "nlist";
inline constexpr const char* NPROBE = "nprobe";
inline constexpr const char* REFINE_RATIO = "refine_ratio";
inline constexpr const char* REFINE_TYPE = "refine_type";
inline constexpr const char* REFINE_WITH_QUANT = "refine_with_quant";
inline constexpr const char* SSIZE = "ssize";
inline constexpr const char* SUB_DIM = "sub_dim";
}  // namespace indexparam

}  // namespace knowhere
