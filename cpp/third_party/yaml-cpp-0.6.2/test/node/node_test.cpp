#include "yaml-cpp/emitter.h"
#include "yaml-cpp/node/emit.h"
#include "yaml-cpp/node/node.h"
#include "yaml-cpp/node/impl.h"
#include "yaml-cpp/node/convert.h"
#include "yaml-cpp/node/iterator.h"
#include "yaml-cpp/node/detail/impl.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

using ::testing::AnyOf;
using ::testing::Eq;

#define EXPECT_THROW_REPRESENTATION_EXCEPTION(statement, message) \
  ASSERT_THROW(statement, RepresentationException);               \
  try {                                                           \
    statement;                                                    \
  } catch (const RepresentationException& e) {                    \
    EXPECT_EQ(e.msg, message);                                    \
  }

namespace YAML {
namespace {
TEST(NodeTest, SimpleScalar) {
  Node node = Node("Hello, World!");
  EXPECT_TRUE(node.IsScalar());
  EXPECT_EQ("Hello, World!", node.as<std::string>());
}

TEST(NodeTest, IntScalar) {
  Node node = Node(15);
  EXPECT_TRUE(node.IsScalar());
  EXPECT_EQ(15, node.as<int>());
}

TEST(NodeTest, SimpleAppendSequence) {
  Node node;
  node.push_back(10);
  node.push_back("foo");
  node.push_back("monkey");
  EXPECT_TRUE(node.IsSequence());
  EXPECT_EQ(3, node.size());
  EXPECT_EQ(10, node[0].as<int>());
  EXPECT_EQ("foo", node[1].as<std::string>());
  EXPECT_EQ("monkey", node[2].as<std::string>());
  EXPECT_TRUE(node.IsSequence());
}

TEST(NodeTest, MapElementRemoval) {
  Node node;
  node["foo"] = "bar";
  node.remove("foo");
  EXPECT_TRUE(!node["foo"]);
}

TEST(NodeTest, SimpleAssignSequence) {
  Node node;
  node[0] = 10;
  node[1] = "foo";
  node[2] = "monkey";
  EXPECT_TRUE(node.IsSequence());
  EXPECT_EQ(3, node.size());
  EXPECT_EQ(10, node[0].as<int>());
  EXPECT_EQ("foo", node[1].as<std::string>());
  EXPECT_EQ("monkey", node[2].as<std::string>());
  EXPECT_TRUE(node.IsSequence());
}

TEST(NodeTest, SimpleMap) {
  Node node;
  node["key"] = "value";
  EXPECT_TRUE(node.IsMap());
  EXPECT_EQ("value", node["key"].as<std::string>());
  EXPECT_EQ(1, node.size());
}

TEST(NodeTest, MapWithUndefinedValues) {
  Node node;
  node["key"] = "value";
  node["undefined"];
  EXPECT_TRUE(node.IsMap());
  EXPECT_EQ("value", node["key"].as<std::string>());
  EXPECT_EQ(1, node.size());

  node["undefined"] = "monkey";
  EXPECT_EQ("monkey", node["undefined"].as<std::string>());
  EXPECT_EQ(2, node.size());
}

TEST(NodeTest, SeqIntoMap) {
  Node node;
  node[0] = "test";
  node[1];
  node[2] = "value";
  EXPECT_TRUE(node.IsMap());
  EXPECT_EQ("test", node[0].as<std::string>());
  EXPECT_EQ("value", node[2].as<std::string>());
  EXPECT_EQ(2, node.size());
}

TEST(NodeTest, RemoveUnassignedNode) {
  Node node(NodeType::Map);
  node["key"];
  node.remove("key");
  EXPECT_EQ(0, node.size());
}

TEST(NodeTest, MapForceInsert) {
  Node node;
  Node k1("k1");
  Node k2("k2");
  Node v1("v1");
  Node v2("v2");
  node[k1] = v1;
  node[k2] = v1;
  EXPECT_TRUE(node.IsMap());
  EXPECT_EQ("v1", node["k1"].as<std::string>());
  EXPECT_EQ("v1", node["k2"].as<std::string>());
  EXPECT_EQ(2, node.size());

  node.force_insert(k2, v2);
  EXPECT_EQ("v1", node["k1"].as<std::string>());
  EXPECT_EQ("v1", node["k2"].as<std::string>());
  EXPECT_EQ(3, node.size());
}

TEST(NodeTest, UndefinedConstNodeWithFallback) {
  Node node;
  const Node& cn = node;
  EXPECT_EQ(cn["undefined"].as<int>(3), 3);
}

TEST(NodeTest, MapIteratorWithUndefinedValues) {
  Node node;
  node["key"] = "value";
  node["undefined"];

  std::size_t count = 0;
  for (const_iterator it = node.begin(); it != node.end(); ++it)
    count++;
  EXPECT_EQ(1, count);
}

TEST(NodeTest, ConstIteratorOnConstUndefinedNode) {
  Node node;
  const Node& cn = node;
  const Node& undefinedCn = cn["undefined"];

  std::size_t count = 0;
  for (const_iterator it = undefinedCn.begin(); it != undefinedCn.end(); ++it) {
    count++;
  }
  EXPECT_EQ(0, count);
}

TEST(NodeTest, IteratorOnConstUndefinedNode) {
  Node node;
  const Node& cn = node;
  const Node& undefinedCn = cn["undefined"];

  Node& nonConstUndefinedNode = const_cast<Node&>(undefinedCn);

  std::size_t count = 0;
  for (iterator it = nonConstUndefinedNode.begin();
       it != nonConstUndefinedNode.end(); ++it) {
    count++;
  }
  EXPECT_EQ(0, count);
}

TEST(NodeTest, SimpleSubkeys) {
  Node node;
  node["device"]["udid"] = "12345";
  node["device"]["name"] = "iPhone";
  node["device"]["os"] = "4.0";
  node["username"] = "monkey";
  EXPECT_EQ("12345", node["device"]["udid"].as<std::string>());
  EXPECT_EQ("iPhone", node["device"]["name"].as<std::string>());
  EXPECT_EQ("4.0", node["device"]["os"].as<std::string>());
  EXPECT_EQ("monkey", node["username"].as<std::string>());
}

TEST(NodeTest, StdArray) {
  std::array<int, 5> evens{{2, 4, 6, 8, 10}};
  Node node;
  node["evens"] = evens;
  std::array<int, 5> actualEvens = node["evens"].as<std::array<int, 5>>();
  EXPECT_EQ(evens, actualEvens);
}

TEST(NodeTest, StdArrayWrongSize) {
  std::array<int, 3> evens{{2, 4, 6}};
  Node node;
  node["evens"] = evens;
  EXPECT_THROW_REPRESENTATION_EXCEPTION(
      (node["evens"].as<std::array<int, 5>>()), ErrorMsg::BAD_CONVERSION);
}

TEST(NodeTest, StdVector) {
  std::vector<int> primes;
  primes.push_back(2);
  primes.push_back(3);
  primes.push_back(5);
  primes.push_back(7);
  primes.push_back(11);
  primes.push_back(13);

  Node node;
  node["primes"] = primes;
  EXPECT_EQ(primes, node["primes"].as<std::vector<int>>());
}

TEST(NodeTest, StdList) {
  std::list<int> primes;
  primes.push_back(2);
  primes.push_back(3);
  primes.push_back(5);
  primes.push_back(7);
  primes.push_back(11);
  primes.push_back(13);

  Node node;
  node["primes"] = primes;
  EXPECT_EQ(primes, node["primes"].as<std::list<int>>());
}

TEST(NodeTest, StdMap) {
  std::map<int, int> squares;
  squares[0] = 0;
  squares[1] = 1;
  squares[2] = 4;
  squares[3] = 9;
  squares[4] = 16;

  Node node;
  node["squares"] = squares;
  std::map<int, int> actualSquares = node["squares"].as<std::map<int, int>>();
  EXPECT_EQ(squares, actualSquares);
}

TEST(NodeTest, StdPair) {
  std::pair<int, std::string> p;
  p.first = 5;
  p.second = "five";

  Node node;
  node["pair"] = p;
  std::pair<int, std::string> actualP =
      node["pair"].as<std::pair<int, std::string>>();
  EXPECT_EQ(p, actualP);
}

TEST(NodeTest, SimpleAlias) {
  Node node;
  node["foo"] = "value";
  node["bar"] = node["foo"];
  EXPECT_EQ("value", node["foo"].as<std::string>());
  EXPECT_EQ("value", node["bar"].as<std::string>());
  EXPECT_EQ(node["bar"], node["foo"]);
  EXPECT_EQ(2, node.size());
}

TEST(NodeTest, AliasAsKey) {
  Node node;
  node["foo"] = "value";
  Node value = node["foo"];
  node[value] = "foo";
  EXPECT_EQ("value", node["foo"].as<std::string>());
  EXPECT_EQ("foo", node[value].as<std::string>());
  EXPECT_EQ("foo", node["value"].as<std::string>());
  EXPECT_EQ(2, node.size());
}

TEST(NodeTest, SelfReferenceSequence) {
  Node node;
  node[0] = node;
  EXPECT_TRUE(node.IsSequence());
  EXPECT_EQ(1, node.size());
  EXPECT_EQ(node, node[0]);
  EXPECT_EQ(node, node[0][0]);
  EXPECT_EQ(node[0], node[0][0]);
}

TEST(NodeTest, ValueSelfReferenceMap) {
  Node node;
  node["key"] = node;
  EXPECT_TRUE(node.IsMap());
  EXPECT_EQ(1, node.size());
  EXPECT_EQ(node, node["key"]);
  EXPECT_EQ(node, node["key"]["key"]);
  EXPECT_EQ(node["key"], node["key"]["key"]);
}

TEST(NodeTest, KeySelfReferenceMap) {
  Node node;
  node[node] = "value";
  EXPECT_TRUE(node.IsMap());
  EXPECT_EQ(1, node.size());
  EXPECT_EQ("value", node[node].as<std::string>());
}

TEST(NodeTest, SelfReferenceMap) {
  Node node;
  node[node] = node;
  EXPECT_TRUE(node.IsMap());
  EXPECT_EQ(1, node.size());
  EXPECT_EQ(node, node[node]);
  EXPECT_EQ(node, node[node][node]);
  EXPECT_EQ(node[node], node[node][node]);
}

TEST(NodeTest, TempMapVariable) {
  Node node;
  Node tmp = node["key"];
  tmp = "value";
  EXPECT_TRUE(node.IsMap());
  EXPECT_EQ(1, node.size());
  EXPECT_EQ("value", node["key"].as<std::string>());
}

TEST(NodeTest, TempMapVariableAlias) {
  Node node;
  Node tmp = node["key"];
  tmp = node["other"];
  node["other"] = "value";
  EXPECT_TRUE(node.IsMap());
  EXPECT_EQ(2, node.size());
  EXPECT_EQ("value", node["key"].as<std::string>());
  EXPECT_EQ("value", node["other"].as<std::string>());
  EXPECT_EQ(node["key"], node["other"]);
}

TEST(NodeTest, Bool) {
  Node node;
  node[true] = false;
  EXPECT_TRUE(node.IsMap());
  EXPECT_EQ(false, node[true].as<bool>());
}

TEST(NodeTest, AutoBoolConversion) {
#ifdef _MSC_VER
#pragma warning(disable : 4800)
#endif
  Node node;
  node["foo"] = "bar";
  EXPECT_TRUE(static_cast<bool>(node["foo"]));
  EXPECT_TRUE(!node["monkey"]);
  EXPECT_TRUE(!!node["foo"]);
}

TEST(NodeTest, FloatingPrecision) {
  const double x = 0.123456789;
  Node node = Node(x);
  EXPECT_EQ(x, node.as<double>());
}

TEST(NodeTest, SpaceChar) {
  Node node = Node(' ');
  EXPECT_EQ(' ', node.as<char>());
}

TEST(NodeTest, CloneNull) {
  Node node;
  Node clone = Clone(node);
  EXPECT_EQ(NodeType::Null, clone.Type());
}

TEST(NodeTest, KeyNodeExitsScope) {
  Node node;
  {
    Node temp("Hello, world");
    node[temp] = 0;
  }
  for (Node::const_iterator it = node.begin(); it != node.end(); ++it) {
    (void)it;
  }
}

TEST(NodeTest, DefaultNodeStyle) {
  Node node;
  EXPECT_EQ(EmitterStyle::Default, node.Style());
}

TEST(NodeTest, AccessNonexistentKeyOnConstNode) {
  YAML::Node node;
  node["3"] = "4";
  const YAML::Node& other = node;
  ASSERT_FALSE(other["5"]);
}

class NodeEmitterTest : public ::testing::Test {
 protected:
  void ExpectOutput(const std::string& output, const Node& node) {
    Emitter emitter;
    emitter << node;
    ASSERT_TRUE(emitter.good());
    EXPECT_EQ(output, emitter.c_str());
  }

  void ExpectAnyOutput(const Node& node, const std::string& output1,
                       const std::string& output2) {
    Emitter emitter;
    emitter << node;
    ASSERT_TRUE(emitter.good());
    EXPECT_THAT(emitter.c_str(), AnyOf(Eq(output1), Eq(output2)));
  }
};

TEST_F(NodeEmitterTest, SimpleFlowSeqNode) {
  Node node;
  node.SetStyle(EmitterStyle::Flow);
  node.push_back(1.01);
  node.push_back(2.01);
  node.push_back(3.01);

  ExpectOutput("[1.01, 2.01, 3.01]", node);
}

TEST_F(NodeEmitterTest, NestFlowSeqNode) {
  Node node, cell0, cell1;

  cell0.push_back(1.01);
  cell0.push_back(2.01);
  cell0.push_back(3.01);

  cell1.push_back(4.01);
  cell1.push_back(5.01);
  cell1.push_back(6.01);

  node.SetStyle(EmitterStyle::Flow);
  node.push_back(cell0);
  node.push_back(cell1);

  ExpectOutput("[[1.01, 2.01, 3.01], [4.01, 5.01, 6.01]]", node);
}

TEST_F(NodeEmitterTest, MixBlockFlowSeqNode) {
  Node node, cell0, cell1;

  cell0.SetStyle(EmitterStyle::Flow);
  cell0.push_back(1.01);
  cell0.push_back(2.01);
  cell0.push_back(3.01);

  cell1.push_back(4.01);
  cell1.push_back(5.01);
  cell1.push_back(6.01);

  node.SetStyle(EmitterStyle::Block);
  node.push_back(cell0);
  node.push_back(cell1);

  ExpectOutput("- [1.01, 2.01, 3.01]\n-\n  - 4.01\n  - 5.01\n  - 6.01", node);
}

TEST_F(NodeEmitterTest, NestBlockFlowMapListNode) {
  Node node, mapNode, blockNode;

  node.push_back(1.01);
  node.push_back(2.01);
  node.push_back(3.01);

  mapNode.SetStyle(EmitterStyle::Flow);
  mapNode["position"] = node;

  blockNode.push_back(1.01);
  blockNode.push_back(mapNode);

  ExpectOutput("- 1.01\n- {position: [1.01, 2.01, 3.01]}", blockNode);
}

TEST_F(NodeEmitterTest, NestBlockMixMapListNode) {
  Node node, mapNode, blockNode;

  node.push_back(1.01);
  node.push_back(2.01);
  node.push_back(3.01);

  mapNode.SetStyle(EmitterStyle::Flow);
  mapNode["position"] = node;

  blockNode["scalar"] = 1.01;
  blockNode["object"] = mapNode;

  ExpectAnyOutput(blockNode,
                  "scalar: 1.01\nobject: {position: [1.01, 2.01, 3.01]}",
                  "object: {position: [1.01, 2.01, 3.01]}\nscalar: 1.01");
}

TEST_F(NodeEmitterTest, NestBlockMapListNode) {
  Node node, mapNode;

  node.push_back(1.01);
  node.push_back(2.01);
  node.push_back(3.01);

  mapNode.SetStyle(EmitterStyle::Block);
  mapNode["position"] = node;

  ExpectOutput("position:\n  - 1.01\n  - 2.01\n  - 3.01", mapNode);
}

TEST_F(NodeEmitterTest, NestFlowMapListNode) {
  Node node, mapNode;

  node.push_back(1.01);
  node.push_back(2.01);
  node.push_back(3.01);

  mapNode.SetStyle(EmitterStyle::Flow);
  mapNode["position"] = node;

  ExpectOutput("{position: [1.01, 2.01, 3.01]}", mapNode);
}
}
}
