#include <bson/bson.h>
#include <gtest/gtest.h>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "common/bson_shim.h"
#include "common/bson_view.h"
#include "common/protobuf_utils.h"
#include "gtest/gtest.h"
#include "index/json_stats/bson_builder.h"
#include "index/json_stats/utils.h"

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
    EXPECT_FALSE(node.value.has_value());
}

TEST_F(DomNodeTest, TypeConstructorTest) {
    // Test DOCUMENT type
    DomNode doc_node(DomNode::Type::DOCUMENT);
    EXPECT_EQ(doc_node.type, DomNode::Type::DOCUMENT);
    EXPECT_TRUE(doc_node.document_children.empty());
    EXPECT_FALSE(doc_node.value.has_value());

    // Test VALUE type
    DomNode value_node(DomNode::Type::VALUE);
    EXPECT_EQ(value_node.type, DomNode::Type::VALUE);
    EXPECT_TRUE(value_node.document_children.empty());
    EXPECT_FALSE(value_node.value.has_value());
}

TEST_F(DomNodeTest, ValueConstructorTest) {
    // Test with boolean value
    DomScalar bool_scalar;
    bool_scalar.type = JSONType::BOOL;
    bool_scalar.b = true;
    DomNode bool_node{std::move(bool_scalar)};
    EXPECT_EQ(bool_node.type, DomNode::Type::VALUE);
    EXPECT_TRUE(bool_node.value.has_value());
    EXPECT_TRUE(bool_node.value->b);

    // Test with int32 value
    DomScalar int_scalar;
    int_scalar.type = JSONType::INT32;
    int_scalar.i32 = 42;
    DomNode int_node{std::move(int_scalar)};
    EXPECT_EQ(int_node.type, DomNode::Type::VALUE);
    EXPECT_TRUE(int_node.value.has_value());
    EXPECT_EQ(int_node.value->i32, 42);

    // Test with string value
    DomScalar str_scalar;
    str_scalar.type = JSONType::STRING;
    str_scalar.str = "test";
    DomNode str_node{std::move(str_scalar)};
    EXPECT_EQ(str_node.type, DomNode::Type::VALUE);
    EXPECT_TRUE(str_node.value.has_value());
    EXPECT_STREQ(str_node.value->str.c_str(), "test");
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
    DomScalar nested_scalar;
    nested_scalar.type = JSONType::INT32;
    nested_scalar.i32 = 123;
    root.document_children["child2"].document_children["nested"] =
        DomNode(std::move(nested_scalar));

    EXPECT_EQ(root.document_children["child2"].document_children["nested"].type,
              DomNode::Type::VALUE);
    EXPECT_EQ(
        root.document_children["child2"].document_children["nested"].value->i32,
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
    EXPECT_TRUE(none_node.value->type == JSONType::NONE);

    // Test BOOL type
    auto true_node = builder.CreateValueNode("true", JSONType::BOOL);
    EXPECT_EQ(true_node.type, DomNode::Type::VALUE);
    EXPECT_TRUE(true_node.value->type == JSONType::BOOL);
    EXPECT_TRUE(true_node.value->b);

    auto false_node = builder.CreateValueNode("false", JSONType::BOOL);
    EXPECT_EQ(false_node.type, DomNode::Type::VALUE);
    EXPECT_TRUE(false_node.value->type == JSONType::BOOL);
    EXPECT_FALSE(false_node.value->b);

    // Test INT64 type
    auto int64_node =
        builder.CreateValueNode("9223372036854775807", JSONType::INT64);
    EXPECT_EQ(int64_node.type, DomNode::Type::VALUE);
    EXPECT_TRUE(int64_node.value->type == JSONType::INT64);
    EXPECT_EQ(int64_node.value->i64, 9223372036854775807LL);

    // Test DOUBLE type
    auto double_node = builder.CreateValueNode("3.14159", JSONType::DOUBLE);
    EXPECT_EQ(double_node.type, DomNode::Type::VALUE);
    EXPECT_TRUE(double_node.value->type == JSONType::DOUBLE);
    EXPECT_DOUBLE_EQ(double_node.value->d, 3.14159);

    // Test STRING type
    auto string_node = builder.CreateValueNode("hello world", JSONType::STRING);
    EXPECT_EQ(string_node.type, DomNode::Type::VALUE);
    EXPECT_TRUE(string_node.value->type == JSONType::STRING);
    EXPECT_EQ(string_node.value->str, "hello world");

    // Test ARRAY type
    auto array_node = builder.CreateValueNode("[1, 2, 3]", JSONType::ARRAY);
    EXPECT_EQ(array_node.type, DomNode::Type::VALUE);
    EXPECT_TRUE(array_node.value->type == JSONType::ARRAY);
    // The array is stored as raw BSON array bytes; parse them back to verify.
    milvus::bson::array_view array_view(array_node.value->arr_bytes.data(),
                                        array_node.value->arr_bytes.size());
    EXPECT_EQ(std::distance(array_view.begin(), array_view.end()), 3);
    auto ait = array_view.begin();
    EXPECT_EQ(ait->get_int64().value, 1);
    ++ait;
    EXPECT_EQ(ait->get_int64().value, 2);
    ++ait;
    EXPECT_EQ(ait->get_int64().value, 3);

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
    EXPECT_EQ(root.document_children["key1"].value->str, "value1");

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
                  .value->i64,
              42);

    // Test overwriting existing value with document
    builder.AppendToDom(root, {"key1", "nested"}, "value3", JSONType::STRING);
    EXPECT_EQ(root.document_children["key1"].type, DomNode::Type::DOCUMENT);
    EXPECT_EQ(root.document_children["key1"].document_children["nested"].type,
              DomNode::Type::VALUE);
    EXPECT_EQ(
        root.document_children["key1"].document_children["nested"].value->str,
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
    BsonDocument bson_doc;
    builder.ConvertDomToBson(root, bson_doc.get());

    BsonView view(bson_doc.data(), bson_doc.length());
    milvus::bson::document_view doc_view(bson_doc.data(), bson_doc.length());

    // Verify scalar fields via FindByPath
    auto string_field = view.FindByPath(doc_view, {"string_field"});
    EXPECT_TRUE(string_field.has_value());
    EXPECT_EQ(string_field.value().get_string().value, "hello");

    auto int_field = view.FindByPath(doc_view, {"int_field"});
    EXPECT_TRUE(int_field.has_value());
    EXPECT_EQ(int_field.value().get_int64().value, 42);

    auto double_field = view.FindByPath(doc_view, {"double_field"});
    EXPECT_TRUE(double_field.has_value());
    EXPECT_DOUBLE_EQ(double_field.value().get_double().value, 3.14);

    auto bool_field = view.FindByPath(doc_view, {"bool_field"});
    EXPECT_TRUE(bool_field.has_value());
    EXPECT_TRUE(bool_field.value().get_bool().value);

    auto nested_field = view.FindByPath(doc_view, {"nested", "field"});
    EXPECT_TRUE(nested_field.has_value());
    EXPECT_EQ(nested_field.value().get_string().value, "nested_value");

    // Verify array
    auto array_field = view.FindByPath(doc_view, {"array_field"});
    EXPECT_TRUE(array_field.has_value());
    auto array_view = array_field.value().get_array().value;
    EXPECT_EQ(std::distance(array_view.begin(), array_view.end()), 3);
    auto ait = array_view.begin();
    EXPECT_EQ(ait->get_int64().value, 1);
    ++ait;
    EXPECT_EQ(ait->get_int64().value, 2);
    ++ait;
    EXPECT_EQ(ait->get_int64().value, 3);
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
    BsonDocument bson_doc;
    builder.ConvertDomToBson(root, bson_doc.get());

    BsonView view(bson_doc.data(), bson_doc.length());
    milvus::bson::document_view doc_view(bson_doc.data(), bson_doc.length());

    // Verify the complex document structure
    auto name = view.FindByPath(doc_view, {"user", "name"});
    EXPECT_TRUE(name.has_value());
    EXPECT_EQ(name.value().get_string().value, "John");

    auto age = view.FindByPath(doc_view, {"user", "age"});
    EXPECT_TRUE(age.has_value());
    EXPECT_EQ(age.value().get_int64().value, 30);

    auto active = view.FindByPath(doc_view, {"user", "active"});
    EXPECT_TRUE(active.has_value());
    EXPECT_TRUE(active.value().get_bool().value);

    auto city = view.FindByPath(doc_view, {"user", "address", "city"});
    EXPECT_TRUE(city.has_value());
    EXPECT_EQ(city.value().get_string().value, "New York");

    auto zip = view.FindByPath(doc_view, {"user", "address", "zip"});
    EXPECT_TRUE(zip.has_value());
    EXPECT_EQ(zip.value().get_string().value, "10001");

    auto version = view.FindByPath(doc_view, {"metadata", "version"});
    EXPECT_TRUE(version.has_value());
    EXPECT_EQ(version.value().get_string().value, "1.0");

    auto count = view.FindByPath(doc_view, {"metadata", "count"});
    EXPECT_TRUE(count.has_value());
    EXPECT_EQ(count.value().get_int64().value, 1000);

    // Verify array
    auto scores = view.FindByPath(doc_view, {"user", "scores"});
    EXPECT_TRUE(scores.has_value());
    auto scores_array = scores.value().get_array().value;
    EXPECT_EQ(std::distance(scores_array.begin(), scores_array.end()), 3);
    auto sit = scores_array.begin();
    EXPECT_EQ(sit->get_int64().value, 85);
    ++sit;
    EXPECT_EQ(sit->get_int64().value, 90);
    ++sit;
    EXPECT_EQ(sit->get_int64().value, 95);
}

TEST_F(BsonBuilderTest, ExtractBsonKeyOffsetsTest) {
    {
        bson_t doc;
        bson_init(&doc);
        bson_append_int64(&doc, "age", -1, 30);
        const uint8_t* data = bson_get_data(&doc);
        uint32_t len = doc.len;

        auto offsets = BsonBuilder::ExtractBsonKeyOffsets(data, len);
        EXPECT_EQ(offsets.size(), 1);
        EXPECT_EQ(offsets[0].first, "/age");
        EXPECT_EQ(offsets[0].second, 4);
        BsonView view(data, len);
        auto res = view.ParseAsValueAtOffset<int64_t>(4);
        EXPECT_EQ(res.value(), 30);

        bson_destroy(&doc);
    }

    {
        bson_t doc;
        bson_init(&doc);
        bson_append_utf8(&doc, "age", -1, "30", -1);
        const uint8_t* data = bson_get_data(&doc);
        uint32_t len = doc.len;

        auto offsets = BsonBuilder::ExtractBsonKeyOffsets(data, len);
        EXPECT_EQ(offsets.size(), 1);
        EXPECT_EQ(offsets[0].first, "/age");
        EXPECT_EQ(offsets[0].second, 4);
        BsonView bson_view(data, len);
        auto res = bson_view.ParseAsValueAtOffset<std::string_view>(4);
        EXPECT_STREQ(std::string(res.value()).c_str(), "30");

        bson_destroy(&doc);
    }

    {
        bson_t doc;
        bson_init(&doc);
        bson_append_int64(&doc, "age", -1, 30);
        bson_append_utf8(&doc, "name", -1, "Alice", -1);
        bson_append_bool(&doc, "active", -1, true);
        const uint8_t* data = bson_get_data(&doc);
        uint32_t len = doc.len;

        auto offsets = BsonBuilder::ExtractBsonKeyOffsets(data, len);
        EXPECT_EQ(offsets.size(), 3);
        EXPECT_EQ(offsets[0].first, "/age");
        EXPECT_EQ(offsets[1].first, "/name");
        EXPECT_EQ(offsets[2].first, "/active");
        BsonView view(data, len);
        auto res1 = view.ParseAsValueAtOffset<int64_t>(offsets[0].second);
        EXPECT_EQ(res1.value(), 30);
        auto res2 =
            view.ParseAsValueAtOffset<std::string_view>(offsets[1].second);
        EXPECT_STREQ(std::string(res2.value()).c_str(), "Alice");
        auto res3 = view.ParseAsValueAtOffset<bool>(offsets[2].second);
        EXPECT_EQ(res3.value(), true);

        bson_destroy(&doc);
    }
}

TEST_F(BsonBuilderTest, ExtractBsonKeyOffsetsTestAllTypes) {
    // Create a complex BSON document with nested structures
    bson_t doc;
    bson_init(&doc);

    // Add simple fields
    bson_append_utf8(&doc, "string_field", -1, "value1", -1);
    bson_append_int32(&doc, "int_field", -1, 42);
    bson_append_double(&doc, "double_field", -1, 3.14);
    bson_append_bool(&doc, "bool_field", -1, true);

    // Add nested document
    bson_t nested_doc;
    bson_append_document_begin(&doc, "nested_doc", -1, &nested_doc);
    bson_append_utf8(&nested_doc, "nested_string", -1, "nested_value", -1);
    bson_append_int32(&nested_doc, "nested_int", -1, 123);
    bson_append_document_end(&doc, &nested_doc);

    // Add array
    bson_t arr;
    bson_append_array_begin(&doc, "array_field", -1, &arr);
    bson_append_utf8(&arr, "0", -1, "array_item1", -1);
    bson_append_int32(&arr, "1", -1, 456);
    bson_append_bool(&arr, "2", -1, false);
    bson_append_array_end(&doc, &arr);

    // Add nested array with document
    bson_t nested_arr;
    bson_append_array_begin(&doc, "nested_array", -1, &nested_arr);
    bson_t arr_doc;
    bson_append_document_begin(&nested_arr, "0", -1, &arr_doc);
    bson_append_utf8(&arr_doc, "arr_doc_field", -1, "arr_doc_value", -1);
    bson_append_document_end(&nested_arr, &arr_doc);
    bson_append_array_end(&doc, &nested_arr);

    const uint8_t* data = bson_get_data(&doc);
    uint32_t doc_len = doc.len;

    // Extract offsets
    auto offsets = BsonBuilder::ExtractBsonKeyOffsets(data, doc_len);

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
    size_t doc_size = doc_len;
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

    bson_destroy(&doc);
}

TEST_F(BsonBuilderTest, ExtractOffsetsRecursiveTest) {
    // Test empty document
    bson_t empty_doc;
    bson_init(&empty_doc);
    const uint8_t* empty_data = bson_get_data(&empty_doc);
    std::vector<std::pair<std::string, size_t>> empty_result;
    builder_->ExtractOffsetsRecursive(empty_data, empty_data, "", empty_result);
    EXPECT_TRUE(empty_result.empty());
    bson_destroy(&empty_doc);

    // Test simple document with one field
    bson_t simple_doc;
    bson_init(&simple_doc);
    bson_append_utf8(&simple_doc, "field", -1, "value", -1);
    const uint8_t* simple_data = bson_get_data(&simple_doc);
    std::vector<std::pair<std::string, size_t>> simple_result;
    builder_->ExtractOffsetsRecursive(
        simple_data, simple_data, "", simple_result);
    EXPECT_EQ(simple_result.size(), 1);
    EXPECT_EQ(simple_result[0].first, "/field");
    bson_destroy(&simple_doc);
}

TEST_F(BsonBuilderTest, ExtractOffsetsRecursiveWithNestedPathTest) {
    bson_t doc;
    bson_init(&doc);
    bson_t nested_doc;
    bson_append_document_begin(&doc, "parent", -1, &nested_doc);
    bson_append_utf8(&nested_doc, "nested_field", -1, "value", -1);
    bson_append_document_end(&doc, &nested_doc);

    const uint8_t* data = bson_get_data(&doc);
    std::vector<std::pair<std::string, size_t>> result;
    builder_->ExtractOffsetsRecursive(data, data, "prefix", result);

    EXPECT_EQ(result.size(), 2);
    EXPECT_EQ(result[0].first, "prefix/parent");
    EXPECT_EQ(result[1].first, "prefix/parent/nested_field");

    bson_destroy(&doc);
}

TEST_F(BsonBuilderTest, ExtractOffsetsRecursiveWithArrayElementsTest) {
    bson_t doc;
    bson_init(&doc);
    bson_t arr;
    bson_append_array_begin(&doc, "mixed_array", -1, &arr);

    // Add array with mixed types
    bson_append_utf8(&arr, "0", -1, "string_item", -1);
    bson_append_int32(&arr, "1", -1, 42);
    bson_append_bool(&arr, "2", -1, true);

    // Add nested document in array
    bson_t nested_doc;
    bson_append_document_begin(&arr, "3", -1, &nested_doc);
    bson_append_utf8(&nested_doc, "nested_field", -1, "value", -1);
    bson_append_document_end(&arr, &nested_doc);

    bson_append_array_end(&doc, &arr);

    const uint8_t* data = bson_get_data(&doc);
    std::vector<std::pair<std::string, size_t>> result;
    builder_->ExtractOffsetsRecursive(data, data, "", result);

    EXPECT_EQ(result.size(), 1);  // mixed_array
    EXPECT_EQ(result[0].first, "/mixed_array");

    bson_destroy(&doc);
}

}  // namespace milvus::index
