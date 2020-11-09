#include "Collection.h"
#include "pb/common.pb.h"
#include "pb/schema.pb.h"
#include "pb/etcd_meta.pb.h"
#include <google/protobuf/text_format.h>
#include <knowhere/index/vector_index/adapter/VectorAdapter.h>
#include <cstring>

namespace milvus::segcore {

Collection::Collection(const std::string& collection_proto)
    : collection_proto_(collection_proto) {
    parse();
    index_ = nullptr;
}
#if 0
void
Collection::AddIndex(const grpc::IndexParam& index_param) {
    auto& index_name = index_param.index_name();
    auto& field_name = index_param.field_name();

    Assert(!index_name.empty());
    Assert(!field_name.empty());

    auto index_type = knowhere::IndexEnum::INDEX_FAISS_IVFPQ;
    auto index_mode = knowhere::IndexMode::MODE_CPU;
    knowhere::Config index_conf;

    bool found_index_type = false;
    bool found_index_mode = false;
    bool found_index_conf = false;

    auto extra_params = index_param.extra_params();
    for (auto& extra_param : extra_params) {
        if (extra_param.key() == "index_type") {
            index_type = extra_param.value().data();
            found_index_type = true;
            continue;
        }
        if (extra_param.key() == "index_mode") {
            auto index_mode_int = stoi(extra_param.value());
            if (index_mode_int == 0) {
                found_index_mode = true;
                continue;
            } else if (index_mode_int == 1) {
                index_mode = knowhere::IndexMode::MODE_GPU;
                found_index_mode = true;
                continue;
            } else {
                throw std::runtime_error("Illegal index mode, only 0 or 1 is supported.");
            }
        }
        if (extra_param.key() == "params") {
            index_conf = nlohmann::json::parse(extra_param.value());
            found_index_conf = true;
            continue;
        }
    }

    if (!found_index_type) {
        std::cout << "WARN: Not specify index type, use default index type: INDEX_FAISS_IVFPQ" << std::endl;
    }
    if (!found_index_mode) {
        std::cout << "WARN: Not specify index mode, use default index mode: MODE_CPU" << std::endl;
    }
    if (!found_index_conf) {
        int dim = 0;

        for (auto& field : schema_->get_fields()) {
            if (field.get_data_type() == DataType::VECTOR_FLOAT) {
                dim = field.get_dim();
            }
        }
        Assert(dim != 0);

        index_conf = milvus::knowhere::Config{
            {knowhere::meta::DIM, dim},         {knowhere::IndexParams::nlist, 100},
            {knowhere::IndexParams::nprobe, 4}, {knowhere::IndexParams::m, 4},
            {knowhere::IndexParams::nbits, 8},  {knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
            {knowhere::meta::DEVICEID, 0},
        };
        std::cout << "WARN: Not specify index config, use default index config" << std::endl;
    }

    index_->AddEntry(index_name, field_name, index_type, index_mode, index_conf);
}

void
Collection::CreateIndex(std::string& index_config) {
    if (index_config.empty()) {
        index_ = nullptr;
        std::cout << "null index config when create index" << std::endl;
        return;
    }

    milvus::proto::etcd::CollectionMeta collection_meta;
    auto suc = google::protobuf::TextFormat::ParseFromString(index_config, &collection_meta);

    if (!suc) {
        std::cerr << "unmarshal index string failed" << std::endl;
    }

    index_ = std::make_shared<IndexMeta>(schema_);

    // for (const auto& index : collection_meta.indexes()) {
    //     std::cout << "add index, index name =" << index.index_name() << ", field_name = " << index.field_name()
    //               << std::endl;
    //     AddIndex(index);
    // }
}
#endif

void
Collection::parse() {
    if (collection_proto_.empty()) {
        std::cout << "WARN: Use default schema" << std::endl;
        auto schema = std::make_shared<Schema>();
        schema->AddField("fakevec", DataType::VECTOR_FLOAT, 16);
        schema->AddField("age", DataType::INT32);
        schema_ = schema;
        return;
    }

    milvus::proto::etcd::CollectionMeta collection_meta;
    auto suc = google::protobuf::TextFormat::ParseFromString(collection_proto_, &collection_meta);

    if (!suc) {
        std::cerr << "unmarshal schema string failed" << std::endl;
    }

    collection_name_ = collection_meta.schema().name();
    // TODO: delete print
    std::cout << "create collection " << collection_meta.schema().name() << std::endl;

    auto schema = std::make_shared<Schema>();
    for (const milvus::proto::schema::FieldSchema& child : collection_meta.schema().fields()) {
        const auto& type_params = child.type_params();
        int dim = 16;
        for (const auto& type_param : type_params) {
            if (type_param.key() == "dim") {
                dim = strtoll(type_param.value().c_str(), nullptr, 10);
            }
        }
        std::cout << "add Field, name :" << child.name() << ", datatype :" << child.data_type() << ", dim :" << dim
                  << std::endl;
        schema->AddField(std::string_view(child.name()), DataType(child.data_type()), dim);
    }
    /*
    schema->AddField("fakevec", DataType::VECTOR_FLOAT, 16);
    schema->AddField("age", DataType::INT32);
    */
    schema_ = schema;
}

}  // namespace milvus::segcore
