#include <iostream>
#include <string>

#include "knowhere/comp/index_param.h"
#include "knowhere/config.h"

namespace {

bool
Contains(const std::string& haystack, const std::string& needle) {
    return haystack.find(needle) != std::string::npos;
}

int
ExpectInvalidSearch(const knowhere::Config& config,
                    const std::string& expected_message,
                    int exit_code) {
    std::string message;
    const auto status = knowhere::ValidateHnswSearchConfig(config, message);
    if (status != knowhere::Status::invalid_args ||
        !Contains(message, expected_message)) {
        std::cerr << "unexpected search config result: status="
                  << static_cast<int>(status) << " message=" << message
                  << "\n";
        return exit_code;
    }
    return 0;
}

int
ExpectValidSearch(const knowhere::Config& config, int exit_code) {
    std::string message;
    const auto status = knowhere::ValidateHnswSearchConfig(config, message);
    if (status != knowhere::Status::success) {
        std::cerr << "unexpected valid search config failure: status="
                  << static_cast<int>(status) << " message=" << message
                  << "\n";
        return exit_code;
    }
    return 0;
}

int
ExpectInvalid(const std::string& index_name,
              const knowhere::Config& config,
              const std::string& expected_message,
              int exit_code) {
    std::string message;
    const auto status = knowhere::IndexStaticFaced<float>::ConfigCheck(
        index_name, 0, config, message);
    if (status != knowhere::Status::invalid_args ||
        !Contains(message, expected_message)) {
        std::cerr << "unexpected config check result for " << index_name
                  << ": status=" << static_cast<int>(status)
                  << " message=" << message << "\n";
        return exit_code;
    }
    return 0;
}

}  // namespace

int
main() {
    knowhere::Config ivf_invalid_nlist;
    ivf_invalid_nlist[knowhere::indexparam::NLIST] = "0";
    if (const auto rc = ExpectInvalid(knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
                                      ivf_invalid_nlist,
                                      "Out of range in json: param 'nlist'",
                                      1);
        rc != 0) {
        return rc;
    }

    knowhere::Config ivfpq_invalid_nbits;
    ivfpq_invalid_nbits[knowhere::indexparam::NLIST] = "128";
    ivfpq_invalid_nbits[knowhere::indexparam::NBITS] = "0";
    if (const auto rc = ExpectInvalid(knowhere::IndexEnum::INDEX_FAISS_IVFPQ,
                                      ivfpq_invalid_nbits,
                                      "Out of range in json: param 'nbits'",
                                      2);
        rc != 0) {
        return rc;
    }

    knowhere::Config ivfpq_invalid_m;
    ivfpq_invalid_m[knowhere::meta::DIM] = "128";
    ivfpq_invalid_m[knowhere::indexparam::IVFM] = "7";
    if (const auto rc = ExpectInvalid(
            knowhere::IndexEnum::INDEX_FAISS_IVFPQ,
            ivfpq_invalid_m,
            "The dimension of a vector (dim) should be a multiple of the number of subquantizers (m)",
            3);
        rc != 0) {
        return rc;
    }

    knowhere::Config scann_invalid_nlist;
    scann_invalid_nlist[knowhere::indexparam::NLIST] = "0";
    if (const auto rc = ExpectInvalid(
            knowhere::IndexEnum::INDEX_FAISS_SCANN_DVR,
            scann_invalid_nlist,
            "Out of range in json: param 'nlist' (0) should be in range [1, 65536]",
            12);
        rc != 0) {
        return rc;
    }

    knowhere::Config scann_invalid_dim;
    scann_invalid_dim[knowhere::meta::DIM] = "127";
    if (const auto rc = ExpectInvalid(
            knowhere::IndexEnum::INDEX_FAISS_SCANN_DVR,
            scann_invalid_dim,
            "The dimension of a vector (dim) should be a multiple of sub_dim. Dimension:127, sub_dim:2: invalid parameter",
            13);
        rc != 0) {
        return rc;
    }

    knowhere::Config hnsw_sq_invalid_sq_type;
    hnsw_sq_invalid_sq_type["sq_type"] = "FP32";
    if (const auto rc = ExpectInvalid(
            knowhere::IndexEnum::INDEX_HNSW_SQ,
            hnsw_sq_invalid_sq_type,
            "invalid scalar quantizer type: invalid parameter",
            14);
        rc != 0) {
        return rc;
    }

    knowhere::Config hnsw_sq_invalid_refine;
    hnsw_sq_invalid_refine["refine"] = 1;
    if (const auto rc = ExpectInvalid(knowhere::IndexEnum::INDEX_HNSW_SQ,
                                      hnsw_sq_invalid_refine,
                                      "should be a boolean: invalid parameter",
                                      15);
        rc != 0) {
        return rc;
    }

    knowhere::Config hnsw_sq_invalid_refine_type;
    hnsw_sq_invalid_refine_type[knowhere::indexparam::REFINE_TYPE] = "INT8";
    if (const auto rc = ExpectInvalid(
            knowhere::IndexEnum::INDEX_HNSW_SQ,
            hnsw_sq_invalid_refine_type,
            "invalid refine type : INT8, optional types are [sq4u, sq6, sq8, fp16, bf16, fp32, flat]: invalid parameter",
            16);
        rc != 0) {
        return rc;
    }

    knowhere::Config ivf_rabitq_invalid_nlist;
    ivf_rabitq_invalid_nlist[knowhere::indexparam::NLIST] = -1;
    if (const auto rc = ExpectInvalid(
            knowhere::IndexEnum::INDEX_FAISS_IVF_RABITQ,
            ivf_rabitq_invalid_nlist,
            "param 'nlist' (-1) should be in range [1, 65536]",
            17);
        rc != 0) {
        return rc;
    }

    knowhere::Config ivf_rabitq_invalid_nlist_type;
    ivf_rabitq_invalid_nlist_type[knowhere::indexparam::NLIST] = 128.0;
    if (const auto rc = ExpectInvalid(
            knowhere::IndexEnum::INDEX_FAISS_IVF_RABITQ,
            ivf_rabitq_invalid_nlist_type,
            "wrong data type in json",
            18);
        rc != 0) {
        return rc;
    }

    knowhere::Config ivf_rabitq_invalid_refine;
    ivf_rabitq_invalid_refine["refine"] = 1;
    if (const auto rc = ExpectInvalid(
            knowhere::IndexEnum::INDEX_FAISS_IVF_RABITQ,
            ivf_rabitq_invalid_refine,
            "Type conflict in json: param 'refine' (\"1\") should be a boolean",
            19);
        rc != 0) {
        return rc;
    }

    knowhere::Config ivf_rabitq_invalid_refine_type;
    ivf_rabitq_invalid_refine_type[knowhere::indexparam::REFINE_TYPE] = "PQ";
    if (const auto rc = ExpectInvalid(
            knowhere::IndexEnum::INDEX_FAISS_IVF_RABITQ,
            ivf_rabitq_invalid_refine_type,
            "invalid refine type : PQ, optional types are [sq6, sq8, fp16, bf16, fp32, flat]",
            20);
        rc != 0) {
        return rc;
    }

    knowhere::Config ivf_rabitq_invalid_nprobe;
    ivf_rabitq_invalid_nprobe["nprobe"] = -1;
    if (const auto rc = ExpectInvalid(
            knowhere::IndexEnum::INDEX_FAISS_IVF_RABITQ,
            ivf_rabitq_invalid_nprobe,
            "Out of range in json: param 'nprobe' (-1) should be in range [1, 65536]",
            20);
        rc != 0) {
        return rc;
    }

    knowhere::Config ivf_rabitq_invalid_rbq_bits_query;
    ivf_rabitq_invalid_rbq_bits_query["rbq_bits_query"] = 6.0;
    if (const auto rc = ExpectInvalid(
            knowhere::IndexEnum::INDEX_FAISS_IVF_RABITQ,
            ivf_rabitq_invalid_rbq_bits_query,
            "Type conflict in json: param 'rbq_bits_query' (6.0) should be integer",
            20);
        rc != 0) {
        return rc;
    }

    knowhere::Config ivf_rabitq_invalid_refine_k;
    ivf_rabitq_invalid_refine_k[knowhere::indexparam::REFINE_K] = true;
    if (const auto rc = ExpectInvalid(
            knowhere::IndexEnum::INDEX_FAISS_IVF_RABITQ,
            ivf_rabitq_invalid_refine_k,
            "Type conflict in json: param 'refine_k' (true) should be a number",
            20);
        rc != 0) {
        return rc;
    }

    knowhere::Config ivf_rabitq_valid_search_strings;
    ivf_rabitq_valid_search_strings["nprobe"] = "32";
    ivf_rabitq_valid_search_strings["rbq_bits_query"] = "6";
    ivf_rabitq_valid_search_strings[knowhere::indexparam::REFINE_K] = "2.0";
    std::string ivf_rabitq_search_message;
    if (const auto status = knowhere::IndexStaticFaced<float>::ConfigCheck(
            knowhere::IndexEnum::INDEX_FAISS_IVF_RABITQ,
            0,
            ivf_rabitq_valid_search_strings,
            ivf_rabitq_search_message);
        status != knowhere::Status::success) {
        std::cerr << "unexpected IVF_RABITQ valid search config failure: status="
                  << static_cast<int>(status)
                  << " message=" << ivf_rabitq_search_message << "\n";
        return 20;
    }

    knowhere::Config diskann_invalid_search_list_small;
    diskann_invalid_search_list_small["search_list_size"] = 1;
    diskann_invalid_search_list_small[knowhere::meta::TOPK] = 10;
    if (const auto rc = ExpectInvalid(
            knowhere::IndexEnum::INDEX_DISKANN,
            diskann_invalid_search_list_small,
            "search_list_size(1) should be larger than k(10)",
            21);
        rc != 0) {
        return rc;
    }

    knowhere::Config diskann_invalid_search_list_negative;
    diskann_invalid_search_list_negative["search_list_size"] = -1;
    if (const auto rc = ExpectInvalid(
            knowhere::IndexEnum::INDEX_DISKANN,
            diskann_invalid_search_list_negative,
            "param 'search_list_size' (-1) should be in range [1, 2147483647]",
            22);
        rc != 0) {
        return rc;
    }

    knowhere::Config diskann_invalid_search_list_float;
    diskann_invalid_search_list_float["search_list_size"] = 100.0;
    if (const auto rc = ExpectInvalid(
            knowhere::IndexEnum::INDEX_DISKANN,
            diskann_invalid_search_list_float,
            "Type conflict in json: param 'search_list_size' (100.0) should be integer",
            23);
        rc != 0) {
        return rc;
    }

    knowhere::Config diskann_invalid_search_list_bool;
    diskann_invalid_search_list_bool["search_list_size"] = true;
    if (const auto rc = ExpectInvalid(
            knowhere::IndexEnum::INDEX_DISKANN,
            diskann_invalid_search_list_bool,
            "Type conflict in json: param 'search_list_size' (true) should be integer",
            24);
        rc != 0) {
        return rc;
    }

    knowhere::Config diskann_invalid_search_list_null;
    diskann_invalid_search_list_null["search_list_size"] = nullptr;
    if (const auto rc = ExpectInvalid(
            knowhere::IndexEnum::INDEX_DISKANN,
            diskann_invalid_search_list_null,
            "Type conflict in json: param 'search_list_size' (null) should be integer",
            25);
        rc != 0) {
        return rc;
    }

    knowhere::Config hnsw_sq_invalid_refine_k_zero;
    hnsw_sq_invalid_refine_k_zero[knowhere::indexparam::REFINE_K] = 0;
    if (const auto rc = ExpectInvalidSearch(hnsw_sq_invalid_refine_k_zero,
                                            "Out of range in json",
                                            26);
        rc != 0) {
        return rc;
    }

    knowhere::Config hnsw_sq_invalid_refine_k_bool;
    hnsw_sq_invalid_refine_k_bool[knowhere::indexparam::REFINE_K] = true;
    if (const auto rc = ExpectInvalidSearch(
            hnsw_sq_invalid_refine_k_bool,
            "Type conflict in json: param 'refine_k' (true) should be a number",
            27);
        rc != 0) {
        return rc;
    }

    knowhere::Config hnsw_sq_invalid_refine_k_null;
    hnsw_sq_invalid_refine_k_null[knowhere::indexparam::REFINE_K] = nullptr;
    if (const auto rc = ExpectInvalidSearch(hnsw_sq_invalid_refine_k_null,
                                            "Type conflict in json",
                                            28);
        rc != 0) {
        return rc;
    }

    knowhere::Config hnsw_sq_invalid_refine_k_list;
    hnsw_sq_invalid_refine_k_list[knowhere::indexparam::REFINE_K] =
        knowhere::Json::array({15});
    if (const auto rc = ExpectInvalidSearch(hnsw_sq_invalid_refine_k_list,
                                            "Type conflict in json",
                                            29);
        rc != 0) {
        return rc;
    }

    knowhere::Config hnsw_valid_ef_equal_topk;
    hnsw_valid_ef_equal_topk[knowhere::indexparam::EF] = 10;
    hnsw_valid_ef_equal_topk[knowhere::meta::TOPK] = 10;
    if (const auto rc = ExpectValidSearch(hnsw_valid_ef_equal_topk, 30);
        rc != 0) {
        return rc;
    }

    knowhere::Config hnsw_invalid_m;
    hnsw_invalid_m[knowhere::indexparam::M] = "-1";
    if (const auto rc = ExpectInvalid(knowhere::IndexEnum::INDEX_HNSW,
                                      hnsw_invalid_m,
                                      "param 'M' (-1) should be in range [2, 2048]",
                                      4);
        rc != 0) {
        return rc;
    }

    knowhere::Config hnsw_invalid_m_float;
    hnsw_invalid_m_float[knowhere::indexparam::M] = 16.0;
    if (const auto rc = ExpectInvalid(knowhere::IndexEnum::INDEX_HNSW,
                                      hnsw_invalid_m_float,
                                      "wrong data type in json",
                                      5);
        rc != 0) {
        return rc;
    }

    knowhere::Config hnsw_invalid_m_decimal_string;
    hnsw_invalid_m_decimal_string[knowhere::indexparam::M] = "16.0";
    if (const auto rc = ExpectInvalid(knowhere::IndexEnum::INDEX_HNSW,
                                      hnsw_invalid_m_decimal_string,
                                      "wrong data type in json",
                                      7);
        rc != 0) {
        return rc;
    }

    knowhere::Config hnsw_invalid_m_bool;
    hnsw_invalid_m_bool[knowhere::indexparam::M] = true;
    if (const auto rc = ExpectInvalid(
            knowhere::IndexEnum::INDEX_HNSW,
            hnsw_invalid_m_bool,
            "invalid integer value, key: 'M', value: 'True': invalid parameter",
            8);
        rc != 0) {
        return rc;
    }

    knowhere::Config hnsw_invalid_ef;
    hnsw_invalid_ef[knowhere::indexparam::EFCONSTRUCTION] = "-1";
    if (const auto rc = ExpectInvalid(knowhere::IndexEnum::INDEX_HNSW,
                                      hnsw_invalid_ef,
                                      "param 'efConstruction' (-1) should be in range [1, 2147483647]",
                                      9);
        rc != 0) {
        return rc;
    }

    knowhere::Config hnsw_invalid_ef_decimal_string;
    hnsw_invalid_ef_decimal_string[knowhere::indexparam::EFCONSTRUCTION] = "100.0";
    if (const auto rc = ExpectInvalid(
            knowhere::IndexEnum::INDEX_HNSW,
            hnsw_invalid_ef_decimal_string,
            "wrong data type in json",
            10);
        rc != 0) {
        return rc;
    }

    knowhere::Config hnsw_invalid_ef_bool;
    hnsw_invalid_ef_bool[knowhere::indexparam::EFCONSTRUCTION] = true;
    if (const auto rc = ExpectInvalid(
            knowhere::IndexEnum::INDEX_HNSW,
            hnsw_invalid_ef_bool,
            "invalid integer value, key: 'efConstruction', value: 'True': invalid parameter",
            11);
        rc != 0) {
        return rc;
    }

    return 0;
}
