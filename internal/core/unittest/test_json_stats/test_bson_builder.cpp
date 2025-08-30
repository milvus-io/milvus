#include <gtest/gtest.h>
#include "common/bson_view.h"
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/types.hpp>
#include <bsoncxx/types/value.hpp>
#include <bsoncxx/types/bson_value/view.hpp>
#include <bsoncxx/types/bson_value/value.hpp>

#include "index/json_stats/bson_builder.h"

namespace milvus::index {

class DomNodeTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
    }
};

TEST_F(DomNodeTest, DefaultConstructorTest) {
    DomNode node;
    EXPECT_EQ(node.type, DomNode::Type::DOCUMENT);
    EXPECT_TRUE(node.document_children.empty());
    EXPECT_FALSE(node.bson_value.has_value());
}

TEST_F(DomNodeTest, TypeConstructorTest) {
    // Test DOCUMENT type
    DomNode doc_node(DomNode::Type::DOCUMENT);
    EXPECT_EQ(doc_node.type, DomNode::Type::DOCUMENT);
    EXPECT_TRUE(doc_node.document_children.empty());
    EXPECT_FALSE(doc_node.bson_value.has_value());

    // Test VALUE type
    DomNode value_node(DomNode::Type::VALUE);
    EXPECT_EQ(value_node.type, DomNode::Type::VALUE);
    EXPECT_TRUE(value_node.document_children.empty());
    EXPECT_FALSE(value_node.bson_value.has_value());
}

TEST_F(DomNodeTest, ValueConstructorTest) {
    // Test with boolean value
    bsoncxx::types::b_bool bool_val{true};
    DomNode bool_node{bsoncxx::types::bson_value::value(bool_val)};
    EXPECT_EQ(bool_node.type, DomNode::Type::VALUE);
    EXPECT_TRUE(bool_node.bson_value.has_value());
    EXPECT_TRUE(bool_node.bson_value.value().view().get_bool());

    // Test with int32 value
    bsoncxx::types::b_int32 int_val{42};
    DomNode int_node{bsoncxx::types::bson_value::value(int_val)};
    EXPECT_EQ(int_node.type, DomNode::Type::VALUE);
    EXPECT_TRUE(int_node.bson_value.has_value());
    EXPECT_EQ(int_node.bson_value.value().view().get_int32(), 42);

    // Test with string value
    bsoncxx::types::b_string str_val{"test"};
    DomNode str_node{bsoncxx::types::bson_value::value(str_val)};
    EXPECT_EQ(str_node.type, DomNode::Type::VALUE);
    EXPECT_TRUE(str_node.bson_value.has_value());
    EXPECT_STREQ(str_node.bson_value.value().view().get_string().value.data(),
                 "test");
}

TEST_F(DomNodeTest, DocumentChildrenTest) {
    DomNode root;

    // Add child nodes
    root.document_children["child1"] = DomNode(DomNode::Type::VALUE);
    root.document_children["child2"] = DomNode(DomNode::Type::DOCUMENT);

    EXPECT_EQ(root.document_children.size(), 2);
    EXPECT_EQ(root.document_children["child1"].type, DomNode::Type::VALUE);
    EXPECT_EQ(root.document_children["child2"].type, DomNode::Type::DOCUMENT);

    // Test nested document
    root.document_children["child2"].document_children["nested"] = DomNode(
        bsoncxx::types::bson_value::value(bsoncxx::types::b_int32{123}));

    EXPECT_EQ(root.document_children["child2"].document_children["nested"].type,
              DomNode::Type::VALUE);
    EXPECT_EQ(root.document_children["child2"]
                  .document_children["nested"]
                  .bson_value.value()
                  .view()
                  .get_int32(),
              123);
}

class BsonBuilderTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        builder_ = std::make_unique<BsonBuilder>();
    }

    std::unique_ptr<BsonBuilder> builder_;
};

TEST_F(BsonBuilderTest, CreateValueNodeTest) {
    BsonBuilder builder;

    // Test NONE type
    auto none_node = builder.CreateValueNode("", JSONType::NONE);
    EXPECT_EQ(none_node.type, DomNode::Type::VALUE);
    EXPECT_TRUE(none_node.bson_value.value().view().type() ==
                bsoncxx::type::k_null);

    // Test BOOL type
    auto true_node = builder.CreateValueNode("true", JSONType::BOOL);
    EXPECT_EQ(true_node.type, DomNode::Type::VALUE);
    EXPECT_TRUE(true_node.bson_value.value().view().type() ==
                bsoncxx::type::k_bool);
    EXPECT_TRUE(true_node.bson_value.value().view().get_bool().value);

    auto false_node = builder.CreateValueNode("false", JSONType::BOOL);
    EXPECT_EQ(false_node.type, DomNode::Type::VALUE);
    EXPECT_TRUE(false_node.bson_value.value().view().type() ==
                bsoncxx::type::k_bool);
    EXPECT_FALSE(false_node.bson_value.value().view().get_bool().value);

    // Test INT64 type
    auto int64_node =
        builder.CreateValueNode("9223372036854775807", JSONType::INT64);
    EXPECT_EQ(int64_node.type, DomNode::Type::VALUE);
    EXPECT_TRUE(int64_node.bson_value.value().view().type() ==
                bsoncxx::type::k_int64);
    EXPECT_EQ(int64_node.bson_value.value().view().get_int64().value,
              9223372036854775807LL);

    // Test DOUBLE type
    auto double_node = builder.CreateValueNode("3.14159", JSONType::DOUBLE);
    EXPECT_EQ(double_node.type, DomNode::Type::VALUE);
    EXPECT_TRUE(double_node.bson_value.value().view().type() ==
                bsoncxx::type::k_double);
    EXPECT_DOUBLE_EQ(double_node.bson_value.value().view().get_double().value,
                     3.14159);

    // Test STRING type
    auto string_node = builder.CreateValueNode("hello world", JSONType::STRING);
    EXPECT_EQ(string_node.type, DomNode::Type::VALUE);
    EXPECT_TRUE(string_node.bson_value.value().view().type() ==
                bsoncxx::type::k_string);
    EXPECT_EQ(string_node.bson_value.value().view().get_string().value,
              "hello world");

    // Test ARRAY type
    auto array_node = builder.CreateValueNode("[1, 2, 3]", JSONType::ARRAY);
    EXPECT_EQ(array_node.type, DomNode::Type::VALUE);
    EXPECT_TRUE(array_node.bson_value.value().view().type() ==
                bsoncxx::type::k_array);
    auto array_view = array_node.bson_value.value().view().get_array().value;
    EXPECT_EQ(std::distance(array_view.begin(), array_view.end()), 3);
    EXPECT_EQ(array_view[0].get_int64().value, 1);
    EXPECT_EQ(array_view[1].get_int64().value, 2);
    EXPECT_EQ(array_view[2].get_int64().value, 3);

    // Test invalid type
    EXPECT_THROW(builder.CreateValueNode("value", static_cast<JSONType>(999)),
                 std::runtime_error);
}

TEST_F(BsonBuilderTest, AppendToDomTest) {
    BsonBuilder builder;
    DomNode root(DomNode::Type::DOCUMENT);

    // Test single level append
    builder.AppendToDom(root, {"key1"}, "value1", JSONType::STRING);
    EXPECT_TRUE(root.document_children.find("key1") !=
                root.document_children.end());
    EXPECT_EQ(root.document_children["key1"].type, DomNode::Type::VALUE);
    EXPECT_EQ(root.document_children["key1"]
                  .bson_value.value()
                  .view()
                  .get_string()
                  .value,
              "value1");

    // Test nested document append
    builder.AppendToDom(
        root, {"level1", "level2", "key2"}, "42", JSONType::INT64);
    EXPECT_TRUE(root.document_children.find("level1") !=
                root.document_children.end());
    EXPECT_EQ(root.document_children["level1"].type, DomNode::Type::DOCUMENT);
    EXPECT_TRUE(
        root.document_children["level1"].document_children.find("level2") !=
        root.document_children["level1"].document_children.end());
    EXPECT_EQ(root.document_children["level1"].document_children["level2"].type,
              DomNode::Type::DOCUMENT);
    EXPECT_EQ(root.document_children["level1"]
                  .document_children["level2"]
                  .document_children["key2"]
                  .type,
              DomNode::Type::VALUE);
    EXPECT_EQ(root.document_children["level1"]
                  .document_children["level2"]
                  .document_children["key2"]
                  .bson_value.value()
                  .view()
                  .get_int64()
                  .value,
              42);

    // Test overwriting existing value with document
    builder.AppendToDom(root, {"key1", "nested"}, "value3", JSONType::STRING);
    EXPECT_EQ(root.document_children["key1"].type, DomNode::Type::DOCUMENT);
    EXPECT_EQ(root.document_children["key1"].document_children["nested"].type,
              DomNode::Type::VALUE);
    EXPECT_EQ(root.document_children["key1"]
                  .document_children["nested"]
                  .bson_value.value()
                  .view()
                  .get_string()
                  .value,
              "value3");
}

TEST_F(BsonBuilderTest, ConvertDomToBsonTest) {
    BsonBuilder builder;
    DomNode root(DomNode::Type::DOCUMENT);

    // Build a complex document structure
    builder.AppendToDom(root, {"string_field"}, "hello", JSONType::STRING);
    builder.AppendToDom(root, {"int_field"}, "42", JSONType::INT64);
    builder.AppendToDom(root, {"double_field"}, "3.14", JSONType::DOUBLE);
    builder.AppendToDom(root, {"bool_field"}, "true", JSONType::BOOL);
    builder.AppendToDom(
        root, {"nested", "field"}, "nested_value", JSONType::STRING);
    builder.AppendToDom(root, {"array_field"}, "[1, 2, 3]", JSONType::ARRAY);

    // Convert to BSON
    bsoncxx::builder::basic::document bson_doc;
    builder.ConvertDomToBson(root, bson_doc);
    auto bson_view = bson_doc.view();

    // Verify the converted BSON document
    EXPECT_EQ(bson_view["string_field"].get_string().value, "hello");
    EXPECT_EQ(bson_view["int_field"].get_int64().value, 42);
    EXPECT_DOUBLE_EQ(bson_view["double_field"].get_double().value, 3.14);
    EXPECT_TRUE(bson_view["bool_field"].get_bool().value);
    EXPECT_EQ(bson_view["nested"]["field"].get_string().value, "nested_value");

    auto array_view = bson_view["array_field"].get_array().value;
    EXPECT_EQ(std::distance(array_view.begin(), array_view.end()), 3);
    EXPECT_EQ(array_view[0].get_int64().value, 1);
    EXPECT_EQ(array_view[1].get_int64().value, 2);
    EXPECT_EQ(array_view[2].get_int64().value, 3);
}

TEST_F(BsonBuilderTest, ComplexDocumentTest) {
    BsonBuilder builder;
    DomNode root(DomNode::Type::DOCUMENT);

    // Create a complex document with multiple levels and types
    builder.AppendToDom(root, {"user", "name"}, "John", JSONType::STRING);
    builder.AppendToDom(root, {"user", "age"}, "30", JSONType::INT64);
    builder.AppendToDom(
        root, {"user", "scores"}, "[85, 90, 95]", JSONType::ARRAY);
    builder.AppendToDom(
        root, {"user", "address", "city"}, "New York", JSONType::STRING);
    builder.AppendToDom(
        root, {"user", "address", "zip"}, "10001", JSONType::STRING);
    builder.AppendToDom(root, {"user", "active"}, "true", JSONType::BOOL);
    builder.AppendToDom(root, {"metadata", "version"}, "1.0", JSONType::STRING);
    builder.AppendToDom(root, {"metadata", "count"}, "1000", JSONType::INT64);

    // Convert to BSON
    bsoncxx::builder::basic::document bson_doc;
    builder.ConvertDomToBson(root, bson_doc);
    auto bson_view = bson_doc.view();

    // Verify the complex document structure
    EXPECT_EQ(bson_view["user"]["name"].get_string().value, "John");
    EXPECT_EQ(bson_view["user"]["age"].get_int64().value, 30);
    EXPECT_TRUE(bson_view["user"]["active"].get_bool().value);
    EXPECT_EQ(bson_view["user"]["address"]["city"].get_string().value,
              "New York");
    EXPECT_EQ(bson_view["user"]["address"]["zip"].get_string().value, "10001");
    EXPECT_EQ(bson_view["metadata"]["version"].get_string().value, "1.0");
    EXPECT_EQ(bson_view["metadata"]["count"].get_int64().value, 1000);

    // Verify array
    auto scores_array = bson_view["user"]["scores"].get_array().value;
    EXPECT_EQ(std::distance(scores_array.begin(), scores_array.end()), 3);
    EXPECT_EQ(scores_array[0].get_int64().value, 85);
    EXPECT_EQ(scores_array[1].get_int64().value, 90);
    EXPECT_EQ(scores_array[2].get_int64().value, 95);
}

TEST_F(BsonBuilderTest, ExtractBsonKeyOffsetsTest) {
    {
        auto doc = bsoncxx::from_json(R"({ "age": 30 })");
        auto offsets = BsonBuilder::ExtractBsonKeyOffsets(doc.view());
        EXPECT_EQ(offsets.size(), 1);
        EXPECT_EQ(offsets[0].first, "/age");
        EXPECT_EQ(offsets[0].second, 4);
        BsonView view(doc.view().data(), doc.view().length());
        auto res = view.ParseAsValueAtOffset<int64_t>(4);
        EXPECT_EQ(res.value(), 30);
    }

    {
        auto doc = bsoncxx::from_json(R"({ "age": "30"})");
        auto offsets = BsonBuilder::ExtractBsonKeyOffsets(doc.view());
        EXPECT_EQ(offsets.size(), 1);
        EXPECT_EQ(offsets[0].first, "/age");
        EXPECT_EQ(offsets[0].second, 4);
        auto bson_view = BsonView(doc.view().data(), doc.view().length());
        auto res = bson_view.ParseAsValueAtOffset<std::string_view>(4);
        EXPECT_STREQ(res.value().data(), "30");
    }

    {
        auto doc = bsoncxx::from_json(
            R"({ "age": 30, "name": "Alice", "active": true })");
        auto offsets = BsonBuilder::ExtractBsonKeyOffsets(doc.view());
        EXPECT_EQ(offsets.size(), 3);
        EXPECT_EQ(offsets[0].first, "/age");
        EXPECT_EQ(offsets[1].first, "/name");
        EXPECT_EQ(offsets[2].first, "/active");
        auto view = BsonView(doc.view().data(), doc.view().length());
        auto res1 = view.ParseAsValueAtOffset<int64_t>(offsets[0].second);
        EXPECT_EQ(res1.value(), 30);
        auto res2 =
            view.ParseAsValueAtOffset<std::string_view>(offsets[1].second);
        EXPECT_STREQ(res2.value().data(), "Alice");
        auto res3 = view.ParseAsValueAtOffset<bool>(offsets[2].second);
        EXPECT_EQ(res3.value(), true);
    }
}

TEST_F(BsonBuilderTest, ExtractBsonKeyOffsetsTestAllTypes) {
    // Create a complex BSON document with nested structures
    bsoncxx::builder::basic::document doc;

    // Add simple fields
    doc.append(bsoncxx::builder::basic::kvp("string_field", "value1"));
    doc.append(bsoncxx::builder::basic::kvp("int_field", 42));
    doc.append(bsoncxx::builder::basic::kvp("double_field", 3.14));
    doc.append(bsoncxx::builder::basic::kvp("bool_field", true));

    // Add nested document
    bsoncxx::builder::basic::document nested_doc;
    nested_doc.append(
        bsoncxx::builder::basic::kvp("nested_string", "nested_value"));
    nested_doc.append(bsoncxx::builder::basic::kvp("nested_int", 123));
    doc.append(bsoncxx::builder::basic::kvp("nested_doc", nested_doc));

    // Add array
    bsoncxx::builder::basic::array arr;
    arr.append("array_item1");
    arr.append(456);
    arr.append(false);
    doc.append(bsoncxx::builder::basic::kvp("array_field", arr));

    // Add nested array with document
    bsoncxx::builder::basic::array nested_arr;
    bsoncxx::builder::basic::document arr_doc;
    arr_doc.append(
        bsoncxx::builder::basic::kvp("arr_doc_field", "arr_doc_value"));
    nested_arr.append(arr_doc);
    doc.append(bsoncxx::builder::basic::kvp("nested_array", nested_arr));

    // Extract offsets
    auto offsets = BsonBuilder::ExtractBsonKeyOffsets(doc.view());

    // Verify results
    EXPECT_FALSE(offsets.empty());

    // Create a map for easier lookup
    std::map<std::string, size_t> offset_map;
    for (const auto& [key, offset] : offsets) {
        offset_map[key] = offset;
    }

    // Verify all expected fields are present
    EXPECT_TRUE(offset_map.find("/string_field") != offset_map.end());
    EXPECT_TRUE(offset_map.find("/int_field") != offset_map.end());
    EXPECT_TRUE(offset_map.find("/double_field") != offset_map.end());
    EXPECT_TRUE(offset_map.find("/bool_field") != offset_map.end());
    EXPECT_TRUE(offset_map.find("/nested_doc") != offset_map.end());
    EXPECT_TRUE(offset_map.find("/nested_doc/nested_string") !=
                offset_map.end());
    EXPECT_TRUE(offset_map.find("/nested_doc/nested_int") != offset_map.end());
    EXPECT_TRUE(offset_map.find("/array_field") != offset_map.end());
    EXPECT_TRUE(offset_map.find("/nested_array") != offset_map.end());

    // Verify offsets are within document bounds
    size_t doc_size = doc.view().length();
    for (const auto& [key, offset] : offsets) {
        EXPECT_LT(offset, doc_size)
            << "Offset for key " << key << " is out of bounds";
    }

    // Verify offsets are unique
    std::set<size_t> unique_offsets;
    for (const auto& [key, offset] : offsets) {
        EXPECT_TRUE(unique_offsets.insert(offset).second)
            << "Duplicate offset found for key " << key;
    }

    // Verify the total number of fields
    // This number should match the total number of fields we added
    // 1. string_field
    // 2. int_field
    // 3. double_field
    // 4. bool_field
    // 5. nested_doc
    // 6. nested_doc/nested_string
    // 7. nested_doc/nested_int
    // 8. array_field
    // 9. nested_array
    // 10. nested_array/0/arr_doc_field
    EXPECT_EQ(offsets.size(), 9);
}

TEST_F(BsonBuilderTest, ExtractOffsetsRecursiveTest) {
    // Test empty document
    bsoncxx::builder::basic::document empty_doc;
    std::vector<std::pair<std::string, size_t>> empty_result;
    builder_->ExtractOffsetsRecursive(
        empty_doc.view().data(), empty_doc.view().data(), "", empty_result);
    EXPECT_TRUE(empty_result.empty());

    // Test simple document with one field
    bsoncxx::builder::basic::document simple_doc;
    simple_doc.append(bsoncxx::builder::basic::kvp("field", "value"));
    std::vector<std::pair<std::string, size_t>> simple_result;
    builder_->ExtractOffsetsRecursive(
        simple_doc.view().data(), simple_doc.view().data(), "", simple_result);
    EXPECT_EQ(simple_result.size(), 1);
    EXPECT_EQ(simple_result[0].first, "/field");
}

TEST_F(BsonBuilderTest, ExtractOffsetsRecursiveWithNestedPathTest) {
    bsoncxx::builder::basic::document doc;
    bsoncxx::builder::basic::document nested_doc;
    nested_doc.append(bsoncxx::builder::basic::kvp("nested_field", "value"));
    doc.append(bsoncxx::builder::basic::kvp("parent", nested_doc));

    std::vector<std::pair<std::string, size_t>> result;
    builder_->ExtractOffsetsRecursive(
        doc.view().data(), doc.view().data(), "prefix", result);

    EXPECT_EQ(result.size(), 2);
    EXPECT_EQ(result[0].first, "prefix/parent");
    EXPECT_EQ(result[1].first, "prefix/parent/nested_field");
}

TEST_F(BsonBuilderTest, ExtractOffsetsRecursiveWithArrayElementsTest) {
    bsoncxx::builder::basic::document doc;
    bsoncxx::builder::basic::array arr;

    // Add array with mixed types
    arr.append("string_item");
    arr.append(42);
    arr.append(true);

    // Add nested document in array
    bsoncxx::builder::basic::document nested_doc;
    nested_doc.append(bsoncxx::builder::basic::kvp("nested_field", "value"));
    arr.append(nested_doc);

    doc.append(bsoncxx::builder::basic::kvp("mixed_array", arr));

    std::vector<std::pair<std::string, size_t>> result;
    builder_->ExtractOffsetsRecursive(
        doc.view().data(), doc.view().data(), "", result);

    EXPECT_EQ(result.size(), 1);  // mixed_array
    EXPECT_EQ(result[0].first, "/mixed_array");
}

}  // namespace milvus::index
