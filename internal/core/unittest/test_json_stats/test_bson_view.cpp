#include <bson/bson.h>
#include <stddef.h>
#include <cstdint>
#include <iostream>
#include <iterator>
#include <memory>
#include <optional>
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

namespace milvus::index {

class BsonViewTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
    }
};

TEST_F(BsonViewTest, ConstructorTest) {
    // Test vector constructor
    std::vector<uint8_t> data = {
        0x05, 0x00, 0x00, 0x00, 0x00};  // Empty BSON document
    BsonView view1(data);
    EXPECT_EQ(view1.ToString(), "{ }");
}

TEST_F(BsonViewTest, ParseAsValueAtOffsetTest) {
    // Create a BSON document with various types
    bson_t doc;
    bson_init(&doc);
    bson_append_int32(&doc, "int32_field", -1, 42);
    bson_append_double(&doc, "double_field", -1, 3.14159);
    bson_append_bool(&doc, "bool_field", -1, true);
    bson_append_utf8(&doc, "string_field", -1, "test string", -1);

    const uint8_t* data = bson_get_data(&doc);
    uint32_t len = doc.len;
    BsonView view(data, len);

    auto offsets = BsonBuilder::ExtractBsonKeyOffsets(data, len);
    for (const auto& offset : offsets) {
        std::cout << offset.first << " " << offset.second << std::endl;
    }

    // Test int32 parsing
    auto int32_val = view.ParseAsValueAtOffset<int32_t>(offsets[0].second);
    EXPECT_TRUE(int32_val.has_value());
    EXPECT_EQ(int32_val.value(), 42);

    // Test double parsing
    auto double_val = view.ParseAsValueAtOffset<double>(offsets[1].second);
    EXPECT_TRUE(double_val.has_value());
    EXPECT_EQ(double_val.value(), 3.14159);

    // Test bool parsing
    auto bool_val = view.ParseAsValueAtOffset<bool>(offsets[2].second);
    EXPECT_TRUE(bool_val.has_value());
    EXPECT_TRUE(bool_val.value());

    // Test string parsing
    auto string_val = view.ParseAsValueAtOffset<std::string>(offsets[3].second);
    EXPECT_TRUE(string_val.has_value());
    EXPECT_EQ(string_val.value(), "test string");

    bson_destroy(&doc);
}

TEST_F(BsonViewTest, ParseAsArrayAtOffsetTest) {
    // Test case 1: offset = 0 (whole array)
    {
        bson_t arr;
        bson_init(&arr);
        bson_append_utf8(&arr, "0", -1, "value1", -1);
        bson_append_int32(&arr, "1", -1, 42);
        bson_append_bool(&arr, "2", -1, true);

        BsonView view(bson_get_data(&arr), arr.len);
        auto array_view = view.ParseAsArrayAtOffset(0);

        // Verify array content
        EXPECT_TRUE(array_view.has_value());
        EXPECT_EQ(
            std::distance(array_view.value().begin(), array_view.value().end()),
            3);

        auto it = array_view.value().begin();
        EXPECT_STREQ(std::string(it->get_string().value).c_str(), "value1");
        ++it;
        EXPECT_EQ(it->get_int32().value, 42);
        ++it;
        EXPECT_TRUE(it->get_bool().value);

        bson_destroy(&arr);
    }

    // Test case 2: offset != 0 (array in document)
    {
        bson_t doc;
        bson_init(&doc);
        bson_append_utf8(&doc, "top_field", -1, "top_value", -1);
        bson_t arr;
        bson_append_array_begin(&doc, "array_field", -1, &arr);
        bson_append_utf8(&arr, "0", -1, "array_value1", -1);
        bson_append_int32(&arr, "1", -1, 123);
        bson_append_double(&arr, "2", -1, 3.14159);
        bson_append_array_end(&doc, &arr);

        const uint8_t* data = bson_get_data(&doc);
        uint32_t len = doc.len;
        BsonView view(data, len);

        // Find the offset of the array
        auto offsets = BsonBuilder::ExtractBsonKeyOffsets(data, len);
        size_t array_offset = 0;
        for (const auto& offset : offsets) {
            if (offset.first == "/array_field") {
                array_offset = offset.second;
                break;
            }
        }
        auto array_view = view.ParseAsArrayAtOffset(array_offset);

        // Verify array content
        EXPECT_TRUE(array_view.has_value());
        EXPECT_EQ(
            std::distance(array_view.value().begin(), array_view.value().end()),
            3);

        auto it = array_view.value().begin();
        EXPECT_STREQ(std::string(it->get_string().value).c_str(),
                     "array_value1");
        ++it;
        EXPECT_EQ(it->get_int32().value, 123);
        ++it;
        EXPECT_DOUBLE_EQ(it->get_double().value, 3.14159);

        bson_destroy(&doc);
    }

    // Test case 4: Error cases
    {
        bson_t doc;
        bson_init(&doc);
        bson_append_utf8(&doc, "field", -1, "value", -1);
        BsonView view(bson_get_data(&doc), doc.len);

        // Test invalid offset (out of range)
        EXPECT_THROW(view.ParseAsArrayAtOffset(1000), std::runtime_error);

        bson_destroy(&doc);
    }
}

TEST_F(BsonViewTest, FindByPathTest) {
    // Create a complex nested BSON document
    bson_t doc;
    bson_init(&doc);
    bson_t nested_doc;
    bson_append_document_begin(&doc, "level1", -1, &nested_doc);
    bson_append_utf8(&nested_doc, "nested_field", -1, "value", -1);
    bson_append_document_end(&doc, &nested_doc);
    bson_append_utf8(&doc, "simple_field", -1, "simple_value", -1);

    const uint8_t* data = bson_get_data(&doc);
    uint32_t len = doc.len;
    BsonView view(data, len);
    milvus::bson::document_view doc_view(data, len);

    // Test finding nested field
    std::vector<std::string> path = {"level1", "nested_field"};
    auto value = view.FindByPath(doc_view, path);
    EXPECT_TRUE(value.has_value());
    EXPECT_STREQ(std::string(value.value().get_string().value).c_str(),
                 "value");

    // Test finding simple field
    path = {"simple_field"};
    value = view.FindByPath(doc_view, path);
    EXPECT_TRUE(value.has_value());
    EXPECT_STREQ(std::string(value.value().get_string().value).c_str(),
                 "simple_value");

    // Test non-existent path
    path = {"non_existent"};
    value = view.FindByPath(doc_view, path);
    EXPECT_FALSE(value.has_value());

    // Test invalid nested path
    path = {"level1", "non_existent"};
    value = view.FindByPath(doc_view, path);
    EXPECT_FALSE(value.has_value());

    bson_destroy(&doc);
}

TEST_F(BsonViewTest, GetNthElementInArrayTest) {
    // Create a BSON array with mixed types
    bson_t arr;
    bson_init(&arr);
    bson_append_utf8(&arr, "0", -1, "string_item", -1);
    bson_append_int32(&arr, "1", -1, 42);
    bson_append_bool(&arr, "2", -1, true);
    bson_append_double(&arr, "3", -1, 3.14159);

    const uint8_t* data = bson_get_data(&arr);

    // Test string element
    auto string_val = BsonView::GetNthElementInArray<std::string>(data, 0);
    EXPECT_TRUE(string_val.has_value());
    EXPECT_EQ(string_val.value(), "string_item");

    // Test int32 element
    auto int_val = BsonView::GetNthElementInArray<int32_t>(data, 1);
    EXPECT_TRUE(int_val.has_value());
    EXPECT_EQ(int_val.value(), 42);

    // Test bool element
    auto bool_val = BsonView::GetNthElementInArray<bool>(data, 2);
    EXPECT_TRUE(bool_val.has_value());
    EXPECT_TRUE(bool_val.value());

    // Test double element
    auto double_val = BsonView::GetNthElementInArray<double>(data, 3);
    EXPECT_TRUE(double_val.has_value());
    EXPECT_DOUBLE_EQ(double_val.value(), 3.14159);

    // Test out of bounds
    auto out_of_bounds = BsonView::GetNthElementInArray<std::string>(data, 10);
    EXPECT_FALSE(out_of_bounds.has_value());

    // Test invalid type conversion
    auto invalid_type = BsonView::GetNthElementInArray<std::string>(data, 1);
    EXPECT_FALSE(invalid_type.has_value());

    bson_destroy(&arr);
}

TEST_F(BsonViewTest, ParseBsonFieldTest) {
    // Create a simple BSON document
    bson_t doc;
    bson_init(&doc);
    bson_append_utf8(&doc, "test_field", -1, "test_value", -1);

    const uint8_t* data = bson_get_data(&doc);
    BsonView view(data, doc.len);

    // Test field parsing
    auto field = view.ParseBsonField(data, 4);
    EXPECT_EQ(field.type, milvus::bson::type::k_string);
    EXPECT_EQ(field.key, "test_field");
    EXPECT_NE(field.value_ptr, nullptr);

    bson_destroy(&doc);
}

TEST_F(BsonViewTest, GetValueFromElementTest) {
    // Create a BSON document with various types
    bson_t doc;
    bson_init(&doc);
    bson_append_int32(&doc, "int32_field", -1, 42);
    bson_append_double(&doc, "double_field", -1, 3.14159);
    bson_append_bool(&doc, "bool_field", -1, true);
    bson_append_utf8(&doc, "string_field", -1, "test string", -1);

    milvus::bson::document_view view(bson_get_data(&doc), doc.len);

    // Collect elements by key for lookup
    auto find_elem = [&](std::string_view key) -> milvus::bson::element {
        for (auto&& e : view) {
            if (e.key() == key) {
                return e;
            }
        }
        return milvus::bson::element();
    };

    // Test int32
    auto int32_val =
        BsonView::GetValueFromElement<int32_t>(find_elem("int32_field"));
    EXPECT_TRUE(int32_val.has_value());
    EXPECT_EQ(int32_val.value(), 42);

    // Test double
    auto double_val =
        BsonView::GetValueFromElement<double>(find_elem("double_field"));
    EXPECT_TRUE(double_val.has_value());
    EXPECT_DOUBLE_EQ(double_val.value(), 3.14159);

    // Test bool
    auto bool_val =
        BsonView::GetValueFromElement<bool>(find_elem("bool_field"));
    EXPECT_TRUE(bool_val.has_value());
    EXPECT_TRUE(bool_val.value());

    // Test string
    auto string_val =
        BsonView::GetValueFromElement<std::string>(find_elem("string_field"));
    EXPECT_TRUE(string_val.has_value());
    EXPECT_EQ(string_val.value(), "test string");

    // Test string_view
    auto string_view_val = BsonView::GetValueFromElement<std::string_view>(
        find_elem("string_field"));
    EXPECT_TRUE(string_view_val.has_value());
    EXPECT_EQ(string_view_val.value(), "test string");

    // Test invalid type conversion
    auto invalid_val =
        BsonView::GetValueFromElement<std::string>(find_elem("int32_field"));
    EXPECT_FALSE(invalid_val.has_value());

    bson_destroy(&doc);
}

TEST_F(BsonViewTest, GetValueFromBsonViewTest) {
    // Create a BSON document with various types
    bson_t doc;
    bson_init(&doc);
    bson_append_int32(&doc, "int32_field", -1, 42);
    bson_append_double(&doc, "double_field", -1, 3.14159);
    bson_append_bool(&doc, "bool_field", -1, true);
    bson_append_utf8(&doc, "string_field", -1, "test string", -1);

    milvus::bson::document_view view(bson_get_data(&doc), doc.len);

    auto find_value = [&](std::string_view key) -> milvus::bson::value_view {
        for (auto&& e : view) {
            if (e.key() == key) {
                return e.get_value();
            }
        }
        return milvus::bson::value_view();
    };

    // Test int32
    auto int32_val =
        BsonView::GetValueFromBsonView<int32_t>(find_value("int32_field"));
    EXPECT_TRUE(int32_val.has_value());
    EXPECT_EQ(int32_val.value(), 42);

    // Test double
    auto double_val =
        BsonView::GetValueFromBsonView<double>(find_value("double_field"));
    EXPECT_TRUE(double_val.has_value());
    EXPECT_DOUBLE_EQ(double_val.value(), 3.14159);

    // Test bool
    auto bool_val =
        BsonView::GetValueFromBsonView<bool>(find_value("bool_field"));
    EXPECT_TRUE(bool_val.has_value());
    EXPECT_TRUE(bool_val.value());

    // Test string
    auto string_val =
        BsonView::GetValueFromBsonView<std::string>(find_value("string_field"));
    EXPECT_TRUE(string_val.has_value());
    EXPECT_EQ(string_val.value(), "test string");

    // Test string_view
    auto string_view_val = BsonView::GetValueFromBsonView<std::string_view>(
        find_value("string_field"));
    EXPECT_TRUE(string_view_val.has_value());
    EXPECT_EQ(string_view_val.value(), "test string");

    // Test invalid type conversion
    auto invalid_val =
        BsonView::GetValueFromBsonView<std::string>(find_value("int32_field"));
    EXPECT_FALSE(invalid_val.has_value());

    bson_destroy(&doc);
}

}  // namespace milvus::index
