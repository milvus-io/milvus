#include "yaml-cpp/yaml.h"  // IWYU pragma: keep

#include "gtest/gtest.h"

namespace YAML {
namespace {
TEST(LoadNodeTest, Reassign) {
  Node node = Load("foo");
  node = Node();
}

TEST(LoadNodeTest, FallbackValues) {
  Node node = Load("foo: bar\nx: 2");
  EXPECT_EQ("bar", node["foo"].as<std::string>());
  EXPECT_EQ("bar", node["foo"].as<std::string>("hello"));
  EXPECT_EQ("hello", node["baz"].as<std::string>("hello"));
  EXPECT_EQ(2, node["x"].as<int>());
  EXPECT_EQ(2, node["x"].as<int>(5));
  EXPECT_EQ(5, node["y"].as<int>(5));
}

TEST(LoadNodeTest, NumericConversion) {
  Node node = Load("[1.5, 1, .nan, .inf, -.inf, 0x15, 015]");
  EXPECT_EQ(1.5f, node[0].as<float>());
  EXPECT_EQ(1.5, node[0].as<double>());
  EXPECT_THROW(node[0].as<int>(), TypedBadConversion<int>);
  EXPECT_EQ(1, node[1].as<int>());
  EXPECT_EQ(1.0f, node[1].as<float>());
  EXPECT_NE(node[2].as<float>(), node[2].as<float>());
  EXPECT_EQ(std::numeric_limits<float>::infinity(), node[3].as<float>());
  EXPECT_EQ(-std::numeric_limits<float>::infinity(), node[4].as<float>());
  EXPECT_EQ(21, node[5].as<int>());
  EXPECT_EQ(13, node[6].as<int>());
}

TEST(LoadNodeTest, Binary) {
  Node node = Load(
      "[!!binary \"SGVsbG8sIFdvcmxkIQ==\", !!binary "
      "\"TWFuIGlzIGRpc3Rpbmd1aXNoZWQsIG5vdCBvbmx5IGJ5IGhpcyByZWFzb24sIGJ1dCBieS"
      "B0aGlzIHNpbmd1bGFyIHBhc3Npb24gZnJvbSBvdGhlciBhbmltYWxzLCB3aGljaCBpcyBhIG"
      "x1c3Qgb2YgdGhlIG1pbmQsIHRoYXQgYnkgYSBwZXJzZXZlcmFuY2Ugb2YgZGVsaWdodCBpbi"
      "B0aGUgY29udGludWVkIGFuZCBpbmRlZmF0aWdhYmxlIGdlbmVyYXRpb24gb2Yga25vd2xlZG"
      "dlLCBleGNlZWRzIHRoZSBzaG9ydCB2ZWhlbWVuY2Ugb2YgYW55IGNhcm5hbCBwbGVhc3VyZS"
      "4K\"]");
  EXPECT_EQ(Binary(reinterpret_cast<const unsigned char*>("Hello, World!"), 13),
            node[0].as<Binary>());
  EXPECT_EQ(Binary(reinterpret_cast<const unsigned char*>(
                       "Man is distinguished, not only by his reason, "
                       "but by this singular passion from other "
                       "animals, which is a lust of the mind, that by "
                       "a perseverance of delight in the continued and "
                       "indefatigable generation of knowledge, exceeds "
                       "the short vehemence of any carnal pleasure.\n"),
                   270),
            node[1].as<Binary>());
}

TEST(LoadNodeTest, IterateSequence) {
  Node node = Load("[1, 3, 5, 7]");
  int seq[] = {1, 3, 5, 7};
  int i = 0;
  for (const_iterator it = node.begin(); it != node.end(); ++it) {
    EXPECT_TRUE(i < 4);
    int x = seq[i++];
    EXPECT_EQ(x, it->as<int>());
  }
  EXPECT_EQ(4, i);
}

TEST(LoadNodeTest, IterateMap) {
  Node node = Load("{a: A, b: B, c: C}");
  int i = 0;
  for (const_iterator it = node.begin(); it != node.end(); ++it) {
    EXPECT_TRUE(i < 3);
    i++;
    EXPECT_EQ(it->second.as<char>(), it->first.as<char>() + 'A' - 'a');
  }
  EXPECT_EQ(3, i);
}

#ifdef BOOST_FOREACH
TEST(LoadNodeTest, ForEach) {
  Node node = Load("[1, 3, 5, 7]");
  int seq[] = {1, 3, 5, 7};
  int i = 0;
  BOOST_FOREACH (const Node& item, node) {
    int x = seq[i++];
    EXPECT_EQ(x, item.as<int>());
  }
}

TEST(LoadNodeTest, ForEachMap) {
  Node node = Load("{a: A, b: B, c: C}");
  BOOST_FOREACH (const const_iterator::value_type& p, node) {
    EXPECT_EQ(p.second.as<char>(), p.first.as<char>() + 'A' - 'a');
  }
}
#endif

TEST(LoadNodeTest, CloneScalar) {
  Node node = Load("!foo monkey");
  Node clone = Clone(node);
  EXPECT_FALSE(clone == node);
  EXPECT_EQ(clone.as<std::string>(), node.as<std::string>());
  EXPECT_EQ(clone.Tag(), node.Tag());
}

TEST(LoadNodeTest, CloneSeq) {
  Node node = Load("[1, 3, 5, 7]");
  Node clone = Clone(node);
  EXPECT_FALSE(clone == node);
  EXPECT_EQ(NodeType::Sequence, clone.Type());
  EXPECT_EQ(clone.size(), node.size());
  for (std::size_t i = 0; i < node.size(); i++) {
    EXPECT_EQ(clone[i].as<int>(), node[i].as<int>());
  }
}

TEST(LoadNodeTest, CloneMap) {
  Node node = Load("{foo: bar}");
  Node clone = Clone(node);
  EXPECT_FALSE(clone == node);
  EXPECT_EQ(NodeType::Map, clone.Type());
  EXPECT_EQ(clone.size(), node.size());
  EXPECT_EQ(clone["foo"].as<std::string>(), node["foo"].as<std::string>());
}

TEST(LoadNodeTest, CloneAlias) {
  Node node = Load("&foo [*foo]");
  Node clone = Clone(node);
  EXPECT_FALSE(clone == node);
  EXPECT_EQ(NodeType::Sequence, clone.Type());
  EXPECT_EQ(clone.size(), node.size());
  EXPECT_EQ(clone[0], clone);
}

TEST(LoadNodeTest, ForceInsertIntoMap) {
  Node node;
  node["a"] = "b";
  node.force_insert("x", "y");
  node.force_insert("a", 5);
  EXPECT_EQ(3, node.size());
  EXPECT_EQ(NodeType::Map, node.Type());
  bool ab = false;
  bool a5 = false;
  bool xy = false;
  for (const_iterator it = node.begin(); it != node.end(); ++it) {
    if (it->first.as<std::string>() == "a") {
      if (it->second.as<std::string>() == "b")
        ab = true;
      else if (it->second.as<std::string>() == "5")
        a5 = true;
    } else if (it->first.as<std::string>() == "x" &&
               it->second.as<std::string>() == "y")
      xy = true;
  }
  EXPECT_TRUE(ab);
  EXPECT_TRUE(a5);
  EXPECT_TRUE(xy);
}

TEST(LoadNodeTest, ResetNode) {
  Node node = Load("[1, 2, 3]");
  EXPECT_TRUE(!node.IsNull());
  Node other = node;
  node.reset();
  EXPECT_TRUE(node.IsNull());
  EXPECT_TRUE(!other.IsNull());
  node.reset(other);
  EXPECT_TRUE(!node.IsNull());
  EXPECT_EQ(node, other);
}

TEST(LoadNodeTest, EmptyString) {
  Node node = Load("\"\"");
  EXPECT_TRUE(!node.IsNull());
}

TEST(LoadNodeTest, DereferenceIteratorError) {
  Node node = Load("[{a: b}, 1, 2]");
  EXPECT_THROW(node.begin()->first.as<int>(), InvalidNode);
  EXPECT_EQ(true, (*node.begin()).IsMap());
  EXPECT_EQ(true, node.begin()->IsMap());
  EXPECT_THROW((*node.begin()->begin()).Type(), InvalidNode);
  EXPECT_THROW(node.begin()->begin()->Type(), InvalidNode);
}

TEST(NodeTest, EmitEmptyNode) {
  Node node;
  Emitter emitter;
  emitter << node;
  EXPECT_EQ("", std::string(emitter.c_str()));
}

TEST(NodeTest, ParseNodeStyle) {
  EXPECT_EQ(EmitterStyle::Flow, Load("[1, 2, 3]").Style());
  EXPECT_EQ(EmitterStyle::Flow, Load("{foo: bar}").Style());
  EXPECT_EQ(EmitterStyle::Block, Load("- foo\n- bar").Style());
  EXPECT_EQ(EmitterStyle::Block, Load("foo: bar").Style());
}

struct ParserExceptionTestCase {
  std::string name;
  std::string input;
  std::string expected_exception;
};

TEST(NodeTest, IncompleteJson) {
  std::vector<ParserExceptionTestCase> tests = {
      {"JSON map without value", "{\"access\"", ErrorMsg::END_OF_MAP_FLOW},
      {"JSON map with colon but no value", "{\"access\":",
       ErrorMsg::END_OF_MAP_FLOW},
      {"JSON map with unclosed value quote", "{\"access\":\"",
       ErrorMsg::END_OF_MAP_FLOW},
      {"JSON map without end brace", "{\"access\":\"abc\"",
       ErrorMsg::END_OF_MAP_FLOW},
  };
  for (const ParserExceptionTestCase test : tests) {
    try {
      Load(test.input);
      FAIL() << "Expected exception " << test.expected_exception << " for "
             << test.name << ", input: " << test.input;
    } catch (const ParserException& e) {
      EXPECT_EQ(test.expected_exception, e.msg);
    }
  }
}

TEST(NodeTest, LoadTildeAsNull) {
  Node node = Load("~");
  ASSERT_TRUE(node.IsNull());
}
    
TEST(NodeTest, LoadTagWithParenthesis) {
    Node node = Load("!Complex(Tag) foo");
    EXPECT_EQ(node.Tag(), "!Complex(Tag)");
    EXPECT_EQ(node.as<std::string>(), "foo");
}

}  // namespace
}  // namespace YAML
