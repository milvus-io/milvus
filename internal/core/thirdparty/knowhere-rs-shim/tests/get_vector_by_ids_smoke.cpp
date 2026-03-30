#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <vector>

#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "knowhere/index/index_factory.h"
#include "knowhere/version.h"

namespace {

knowhere::DataSetPtr
MakeIdsDataset(const std::vector<int64_t>& ids) {
    auto dataset = std::make_shared<knowhere::DataSet>();
    dataset->SetRows(static_cast<int64_t>(ids.size()));
    dataset->SetDim(1);
    dataset->SetIds(ids.data());
    dataset->SetIsOwner(false);
    return dataset;
}

int
CheckHnswGetVectorByIds() {
    const std::vector<float> base = {
        1.0f,  2.0f,  3.0f,  4.0f,
        5.0f,  6.0f,  7.0f,  8.0f,
        9.0f,  10.0f, 11.0f, 12.0f,
    };
    const std::vector<int64_t> base_ids = {10, 20, 30};
    const std::vector<int64_t> requested_ids = {20, 10};
    const std::vector<float> expected = {
        5.0f, 6.0f, 7.0f, 8.0f,
        1.0f, 2.0f, 3.0f, 4.0f,
    };

    auto base_ds = knowhere::GenDataSet(3, 4, base.data());
    base_ds->SetIds(base_ids.data());

    knowhere::Config build_cfg;
    build_cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::L2;
    build_cfg[knowhere::meta::DIM] = 4;
    build_cfg[knowhere::indexparam::M] = 16;
    build_cfg[knowhere::indexparam::EFCONSTRUCTION] = 64;
    build_cfg[knowhere::indexparam::EF] = 32;

    auto created = knowhere::IndexFactory::Instance().Create<float>(
        knowhere::IndexEnum::INDEX_HNSW,
        knowhere::Version::GetCurrentVersion());
    if (!created.has_value()) {
        std::cerr << "failed to create hnsw index\n";
        return 1;
    }

    auto index = created.value();
    if (index.Build(*base_ds, build_cfg) != knowhere::Status::success) {
        std::cerr << "failed to build hnsw index\n";
        return 2;
    }

    auto requested = MakeIdsDataset(requested_ids);
    auto vectors = index.GetVectorByIds(requested);
    if (!vectors.has_value() || vectors.value() == nullptr ||
        vectors.value()->GetTensor() == nullptr) {
        std::cerr << "failed to get vectors from hnsw index\n";
        return 3;
    }

    const auto* tensor =
        static_cast<const float*>(vectors.value()->GetTensor());
    if (vectors.value()->GetRows() != 2 || vectors.value()->GetDim() != 4 ||
        vectors.value()->GetIds() == nullptr ||
        std::memcmp(tensor,
                    expected.data(),
                    expected.size() * sizeof(float)) != 0) {
        std::cerr << "unexpected hnsw get_vector_by_ids payload\n";
        return 4;
    }

    knowhere::BinarySet binary_set;
    if (index.Serialize(binary_set) != knowhere::Status::success) {
        std::cerr << "failed to serialize hnsw index\n";
        return 5;
    }

    auto restored_created = knowhere::IndexFactory::Instance().Create<float>(
        knowhere::IndexEnum::INDEX_HNSW,
        knowhere::Version::GetCurrentVersion());
    if (!restored_created.has_value()) {
        std::cerr << "failed to create restored hnsw index\n";
        return 6;
    }
    auto restored = restored_created.value();
    if (restored.Deserialize(binary_set, build_cfg) != knowhere::Status::success) {
        std::cerr << "failed to deserialize hnsw index\n";
        return 7;
    }

    auto restored_vectors = restored.GetVectorByIds(requested);
    if (!restored_vectors.has_value() || restored_vectors.value() == nullptr ||
        restored_vectors.value()->GetTensor() == nullptr ||
        std::memcmp(restored_vectors.value()->GetTensor(),
                    expected.data(),
                    expected.size() * sizeof(float)) != 0) {
        std::cerr << "unexpected restored hnsw get_vector_by_ids payload\n";
        return 8;
    }

    return 0;
}

int
CheckFp16RawGetVectorByIds() {
    const std::vector<uint16_t> base = {
        0x3C00u, 0x4000u,
        0x4200u, 0x4400u,
        0x4500u, 0x4600u,
    };
    const std::vector<int64_t> base_ids = {3, 7, 11};
    const std::vector<int64_t> requested_ids = {11, 3};
    const std::vector<uint16_t> expected = {
        0x4500u, 0x4600u,
        0x3C00u, 0x4000u,
    };

    auto base_ds = knowhere::GenDataSet(3, 2, base.data());
    base_ds->SetIds(base_ids.data());

    knowhere::Config build_cfg;
    build_cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::COSINE;
    build_cfg[knowhere::meta::DIM] = 2;

    auto created = knowhere::IndexFactory::Instance().Create<knowhere::fp16>(
        knowhere::IndexEnum::INDEX_HNSW,
        knowhere::Version::GetCurrentVersion());
    if (!created.has_value()) {
        std::cerr << "failed to create fp16 raw-data index\n";
        return 9;
    }

    auto index = created.value();
    if (index.Build(*base_ds, build_cfg) != knowhere::Status::success) {
        std::cerr << "failed to build fp16 raw-data index\n";
        return 10;
    }

    auto requested = MakeIdsDataset(requested_ids);
    auto vectors = index.GetVectorByIds(requested);
    if (!vectors.has_value() || vectors.value() == nullptr ||
        vectors.value()->GetTensor() == nullptr) {
        std::cerr << "failed to get vectors from fp16 raw-data index\n";
        return 11;
    }

    if (vectors.value()->GetRows() != 2 || vectors.value()->GetDim() != 2 ||
        vectors.value()->GetIds() == nullptr ||
        std::memcmp(vectors.value()->GetTensor(),
                    expected.data(),
                    expected.size() * sizeof(uint16_t)) != 0) {
        std::cerr << "unexpected fp16 raw-data get_vector_by_ids payload\n";
        return 12;
    }

    knowhere::BinarySet binary_set;
    if (index.Serialize(binary_set) != knowhere::Status::success) {
        std::cerr << "failed to serialize fp16 raw-data index\n";
        return 13;
    }

    auto restored_created = knowhere::IndexFactory::Instance().Create<knowhere::fp16>(
        knowhere::IndexEnum::INDEX_HNSW,
        knowhere::Version::GetCurrentVersion());
    if (!restored_created.has_value()) {
        std::cerr << "failed to create restored fp16 raw-data index\n";
        return 14;
    }
    auto restored = restored_created.value();
    if (restored.Deserialize(binary_set, build_cfg) != knowhere::Status::success) {
        std::cerr << "failed to deserialize fp16 raw-data index\n";
        return 15;
    }

    auto restored_vectors = restored.GetVectorByIds(requested);
    if (!restored_vectors.has_value() || restored_vectors.value() == nullptr ||
        restored_vectors.value()->GetTensor() == nullptr ||
        std::memcmp(restored_vectors.value()->GetTensor(),
                    expected.data(),
                    expected.size() * sizeof(uint16_t)) != 0) {
        std::cerr << "unexpected restored fp16 raw-data payload\n";
        return 16;
    }

    return 0;
}

}  // namespace

int
main() {
    if (const auto rc = CheckHnswGetVectorByIds(); rc != 0) {
        return rc;
    }
    if (const auto rc = CheckFp16RawGetVectorByIds(); rc != 0) {
        return rc;
    }
    return 0;
}
