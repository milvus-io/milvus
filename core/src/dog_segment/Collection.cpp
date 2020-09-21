#include "Collection.h"
#include "pb/master.pb.h"
//using Collection = masterpb::Collection;
#include <google/protobuf/text_format.h>
namespace milvus::dog_segment {

Collection::Collection(std::string &collection_name, std::string &schema):
      collection_name_(collection_name), schema_json_(schema) {
    parse();
}

void
Collection::set_index() {}

void
Collection::parse() {
    if(schema_json_ == "") {
        auto schema = std::make_shared<Schema>();
        schema->AddField("fakevec", DataType::VECTOR_FLOAT, 16);
        schema->AddField("age", DataType::INT32);
        schema_ = schema;
        return;
    }

    masterpb::Collection collection;
    auto suc = google::protobuf::TextFormat::ParseFromString(schema_json_, &collection);


    if (!suc) {
      std::cerr << "unmarshal failed" << std::endl;
    }
    auto schema = std::make_shared<Schema>();
    for (const milvus::grpc::FieldMeta & child: collection.schema().field_metas()){
            std::cout<<"add Field, name :" << child.field_name() << ", datatype :" << child.type() << ", dim :" << int(child.dim()) << std::endl;
            schema->AddField(std::string_view(child.field_name()), DataType {child.type()}, int(child.dim()));
    }
    /*
    schema->AddField("fakevec", DataType::VECTOR_FLOAT, 16);
    schema->AddField("age", DataType::INT32);
    */
    schema_ = schema;
    
}

}

