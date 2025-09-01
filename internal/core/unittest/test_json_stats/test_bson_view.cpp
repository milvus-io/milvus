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
    bsoncxx::builder::basic::document doc;
    doc.append(bsoncxx::builder::basic::kvp("int32_field", 42));
    doc.append(bsoncxx::builder::basic::kvp("double_field", 3.14159));
    doc.append(bsoncxx::builder::basic::kvp("bool_field", true));
    doc.append(bsoncxx::builder::basic::kvp("string_field", "test string"));

    BsonView view(doc.view().data(), doc.view().length());

    auto offsets = BsonBuilder::ExtractBsonKeyOffsets(doc.view());
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
}

TEST_F(BsonViewTest, ParseAsArrayAtOffsetTest) {
    // Test case 1: offset = 0 (whole array)
    {
        bsoncxx::builder::basic::array arr;
        arr.append("value1");
        arr.append(42);
        arr.append(true);

        BsonView view(arr.view().data(), arr.view().length());
        auto array_view = view.ParseAsArrayAtOffset(0);

        // Verify array content
        EXPECT_TRUE(array_view.has_value());
        EXPECT_EQ(
            std::distance(array_view.value().begin(), array_view.value().end()),
            3);
        EXPECT_STREQ(array_view.value()[0].get_string().value.data(), "value1");
        EXPECT_EQ(array_view.value()[1].get_int32(), 42);
        EXPECT_TRUE(array_view.value()[2].get_bool());
    }

    // Test case 2: offset != 0 (array in document)
    {
        bsoncxx::builder::basic::array arr;
        arr.append("array_value1");
        arr.append(123);
        arr.append(3.14159);

        bsoncxx::builder::basic::document doc;
        doc.append(bsoncxx::builder::basic::kvp("top_field", "top_value"));
        doc.append(bsoncxx::builder::basic::kvp("array_field", arr));

        BsonView view(doc.view().data(), doc.view().length());

        // Find the offset of the array
        auto offsets = BsonBuilder::ExtractBsonKeyOffsets(doc.view());
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
        EXPECT_STREQ(array_view.value()[0].get_string().value.data(),
                     "array_value1");
        EXPECT_EQ(array_view.value()[1].get_int32(), 123);
        EXPECT_DOUBLE_EQ(array_view.value()[2].get_double(), 3.14159);
    }

    // Test case 4: Error cases
    {
        bsoncxx::builder::basic::document doc;
        doc.append(bsoncxx::builder::basic::kvp("field", "value"));
        BsonView view(doc.view().data(), doc.view().length());

        // Test invalid offset (out of range)
        EXPECT_THROW(view.ParseAsArrayAtOffset(1000), std::runtime_error);
    }
}

TEST_F(BsonViewTest, FindByPathTest) {
    // Create a complex nested BSON document
    bsoncxx::builder::basic::document nested_doc;
    nested_doc.append(bsoncxx::builder::basic::kvp("nested_field", "value"));

    bsoncxx::builder::basic::document doc;
    doc.append(bsoncxx::builder::basic::kvp("level1", nested_doc));
    doc.append(bsoncxx::builder::basic::kvp("simple_field", "simple_value"));

    BsonView view(doc.view().data(), doc.view().length());

    // Test finding nested field
    std::vector<std::string> path = {"level1", "nested_field"};
    auto value = view.FindByPath(doc.view(), path);
    EXPECT_TRUE(value.has_value());
    EXPECT_STREQ(value.value().get_string().value.data(), "value");

    // Test finding simple field
    path = {"simple_field"};
    value = view.FindByPath(doc.view(), path);
    EXPECT_TRUE(value.has_value());
    EXPECT_STREQ(value.value().get_string().value.data(), "simple_value");

    // Test non-existent path
    path = {"non_existent"};
    value = view.FindByPath(doc.view(), path);
    EXPECT_FALSE(value.has_value());

    // Test invalid nested path
    path = {"level1", "non_existent"};
    value = view.FindByPath(doc.view(), path);
    EXPECT_FALSE(value.has_value());
}

TEST_F(BsonViewTest, GetNthElementInArrayTest) {
    // Create a BSON array with mixed types
    bsoncxx::builder::basic::array arr;
    arr.append("string_item");
    arr.append(42);
    arr.append(true);
    arr.append(3.14159);

    // Test string element
    auto string_val =
        BsonView::GetNthElementInArray<std::string>(arr.view().data(), 0);
    EXPECT_TRUE(string_val.has_value());
    EXPECT_EQ(string_val.value(), "string_item");

    // Test int32 element
    auto int_val =
        BsonView::GetNthElementInArray<int32_t>(arr.view().data(), 1);
    EXPECT_TRUE(int_val.has_value());
    EXPECT_EQ(int_val.value(), 42);

    // Test bool element
    auto bool_val = BsonView::GetNthElementInArray<bool>(arr.view().data(), 2);
    EXPECT_TRUE(bool_val.has_value());
    EXPECT_TRUE(bool_val.value());

    // Test double element
    auto double_val =
        BsonView::GetNthElementInArray<double>(arr.view().data(), 3);
    EXPECT_TRUE(double_val.has_value());
    EXPECT_DOUBLE_EQ(double_val.value(), 3.14159);

    // Test out of bounds
    auto out_of_bounds =
        BsonView::GetNthElementInArray<std::string>(arr.view().data(), 10);
    EXPECT_FALSE(out_of_bounds.has_value());

    // Test invalid type conversion
    auto invalid_type =
        BsonView::GetNthElementInArray<std::string>(arr.view().data(), 1);
    EXPECT_FALSE(invalid_type.has_value());
}

TEST_F(BsonViewTest, ParseBsonFieldTest) {
    // Create a simple BSON document
    bsoncxx::builder::basic::document doc;
    doc.append(bsoncxx::builder::basic::kvp("test_field", "test_value"));

    BsonView view(doc.view().data(), doc.view().length());

    // Test field parsing
    auto field = view.ParseBsonField(doc.view().data(), 4);
    EXPECT_EQ(field.type, bsoncxx::type::k_string);
    EXPECT_EQ(field.key, "test_field");
    EXPECT_NE(field.value_ptr, nullptr);
}

TEST_F(BsonViewTest, GetValueFromElementTest) {
    // Create a BSON document with various types
    bsoncxx::builder::basic::document doc;
    doc.append(bsoncxx::builder::basic::kvp("int32_field", 42));
    doc.append(bsoncxx::builder::basic::kvp("double_field", 3.14159));
    doc.append(bsoncxx::builder::basic::kvp("bool_field", true));
    doc.append(bsoncxx::builder::basic::kvp("string_field", "test string"));

    auto view = doc.view();

    // Test int32
    auto int32_val =
        BsonView::GetValueFromElement<int32_t>(view["int32_field"]);
    EXPECT_TRUE(int32_val.has_value());
    EXPECT_EQ(int32_val.value(), 42);

    // Test double
    auto double_val =
        BsonView::GetValueFromElement<double>(view["double_field"]);
    EXPECT_TRUE(double_val.has_value());
    EXPECT_DOUBLE_EQ(double_val.value(), 3.14159);

    // Test bool
    auto bool_val = BsonView::GetValueFromElement<bool>(view["bool_field"]);
    EXPECT_TRUE(bool_val.has_value());
    EXPECT_TRUE(bool_val.value());

    // Test string
    auto string_val =
        BsonView::GetValueFromElement<std::string>(view["string_field"]);
    EXPECT_TRUE(string_val.has_value());
    EXPECT_EQ(string_val.value(), "test string");

    // Test string_view
    auto string_view_val =
        BsonView::GetValueFromElement<std::string_view>(view["string_field"]);
    EXPECT_TRUE(string_view_val.has_value());
    EXPECT_EQ(string_view_val.value(), "test string");

    // Test invalid type conversion
    auto invalid_val =
        BsonView::GetValueFromElement<std::string>(view["int32_field"]);
    EXPECT_FALSE(invalid_val.has_value());
}

TEST_F(BsonViewTest, GetValueFromBsonViewTest) {
    // Create a BSON document with various types
    bsoncxx::builder::basic::document doc;
    doc.append(bsoncxx::builder::basic::kvp("int32_field", 42));
    doc.append(bsoncxx::builder::basic::kvp("double_field", 3.14159));
    doc.append(bsoncxx::builder::basic::kvp("bool_field", true));
    doc.append(bsoncxx::builder::basic::kvp("string_field", "test string"));

    auto view = doc.view();

    // Test int32
    auto int32_val = BsonView::GetValueFromBsonView<int32_t>(
        view["int32_field"].get_value());
    EXPECT_TRUE(int32_val.has_value());
    EXPECT_EQ(int32_val.value(), 42);

    // Test double
    auto double_val = BsonView::GetValueFromBsonView<double>(
        view["double_field"].get_value());
    EXPECT_TRUE(double_val.has_value());
    EXPECT_DOUBLE_EQ(double_val.value(), 3.14159);

    // Test bool
    auto bool_val =
        BsonView::GetValueFromBsonView<bool>(view["bool_field"].get_value());
    EXPECT_TRUE(bool_val.has_value());
    EXPECT_TRUE(bool_val.value());

    // Test string
    auto string_val = BsonView::GetValueFromBsonView<std::string>(
        view["string_field"].get_value());
    EXPECT_TRUE(string_val.has_value());
    EXPECT_EQ(string_val.value(), "test string");

    // Test string_view
    auto string_view_val = BsonView::GetValueFromBsonView<std::string_view>(
        view["string_field"].get_value());
    EXPECT_TRUE(string_view_val.has_value());
    EXPECT_EQ(string_view_val.value(), "test string");

    // Test invalid type conversion
    auto invalid_val = BsonView::GetValueFromBsonView<std::string>(
        view["int32_field"].get_value());
    EXPECT_FALSE(invalid_val.has_value());
}

}  // namespace milvus::index