#include "specexamples.h"
#include "yaml-cpp/yaml.h"  // IWYU pragma: keep

#include "gtest/gtest.h"

#define EXPECT_THROW_PARSER_EXCEPTION(statement, message) \
  ASSERT_THROW(statement, ParserException);               \
  try {                                                   \
    statement;                                            \
  } catch (const ParserException& e) {                    \
    EXPECT_EQ(e.msg, message);                            \
  }

namespace YAML {
namespace {

TEST(NodeSpecTest, Ex2_1_SeqScalars) {
  Node doc = Load(ex2_1);
  EXPECT_TRUE(doc.IsSequence());
  EXPECT_EQ(3, doc.size());
  EXPECT_EQ("Mark McGwire", doc[0].as<std::string>());
  EXPECT_EQ("Sammy Sosa", doc[1].as<std::string>());
  EXPECT_EQ("Ken Griffey", doc[2].as<std::string>());
}

TEST(NodeSpecTest, Ex2_2_MappingScalarsToScalars) {
  Node doc = Load(ex2_2);
  EXPECT_TRUE(doc.IsMap());
  EXPECT_EQ(3, doc.size());
  EXPECT_EQ("65", doc["hr"].as<std::string>());
  EXPECT_EQ("0.278", doc["avg"].as<std::string>());
  EXPECT_EQ("147", doc["rbi"].as<std::string>());
}

TEST(NodeSpecTest, Ex2_3_MappingScalarsToSequences) {
  Node doc = Load(ex2_3);
  EXPECT_TRUE(doc.IsMap());
  EXPECT_EQ(2, doc.size());
  EXPECT_EQ(3, doc["american"].size());
  EXPECT_EQ("Boston Red Sox", doc["american"][0].as<std::string>());
  EXPECT_EQ("Detroit Tigers", doc["american"][1].as<std::string>());
  EXPECT_EQ("New York Yankees", doc["american"][2].as<std::string>());
  EXPECT_EQ(3, doc["national"].size());
  EXPECT_EQ("New York Mets", doc["national"][0].as<std::string>());
  EXPECT_EQ("Chicago Cubs", doc["national"][1].as<std::string>());
  EXPECT_EQ("Atlanta Braves", doc["national"][2].as<std::string>());
}

TEST(NodeSpecTest, Ex2_4_SequenceOfMappings) {
  Node doc = Load(ex2_4);
  EXPECT_EQ(2, doc.size());
  EXPECT_EQ(3, doc[0].size());
  EXPECT_EQ("Mark McGwire", doc[0]["name"].as<std::string>());
  EXPECT_EQ("65", doc[0]["hr"].as<std::string>());
  EXPECT_EQ("0.278", doc[0]["avg"].as<std::string>());
  EXPECT_EQ(3, doc[1].size());
  EXPECT_EQ("Sammy Sosa", doc[1]["name"].as<std::string>());
  EXPECT_EQ("63", doc[1]["hr"].as<std::string>());
  EXPECT_EQ("0.288", doc[1]["avg"].as<std::string>());
}

TEST(NodeSpecTest, Ex2_5_SequenceOfSequences) {
  Node doc = Load(ex2_5);
  EXPECT_EQ(3, doc.size());
  EXPECT_EQ(3, doc[0].size());
  EXPECT_EQ("name", doc[0][0].as<std::string>());
  EXPECT_EQ("hr", doc[0][1].as<std::string>());
  EXPECT_EQ("avg", doc[0][2].as<std::string>());
  EXPECT_EQ(3, doc[1].size());
  EXPECT_EQ("Mark McGwire", doc[1][0].as<std::string>());
  EXPECT_EQ("65", doc[1][1].as<std::string>());
  EXPECT_EQ("0.278", doc[1][2].as<std::string>());
  EXPECT_EQ(3, doc[2].size());
  EXPECT_EQ("Sammy Sosa", doc[2][0].as<std::string>());
  EXPECT_EQ("63", doc[2][1].as<std::string>());
  EXPECT_EQ("0.288", doc[2][2].as<std::string>());
}

TEST(NodeSpecTest, Ex2_6_MappingOfMappings) {
  Node doc = Load(ex2_6);
  EXPECT_EQ(2, doc.size());
  EXPECT_EQ(2, doc["Mark McGwire"].size());
  EXPECT_EQ("65", doc["Mark McGwire"]["hr"].as<std::string>());
  EXPECT_EQ("0.278", doc["Mark McGwire"]["avg"].as<std::string>());
  EXPECT_EQ(2, doc["Sammy Sosa"].size());
  EXPECT_EQ("63", doc["Sammy Sosa"]["hr"].as<std::string>());
  EXPECT_EQ("0.288", doc["Sammy Sosa"]["avg"].as<std::string>());
}

TEST(NodeSpecTest, Ex2_7_TwoDocumentsInAStream) {
  std::vector<Node> docs = LoadAll(ex2_7);
  EXPECT_EQ(2, docs.size());

  {
    Node doc = docs[0];
    EXPECT_EQ(3, doc.size());
    EXPECT_EQ("Mark McGwire", doc[0].as<std::string>());
    EXPECT_EQ("Sammy Sosa", doc[1].as<std::string>());
    EXPECT_EQ("Ken Griffey", doc[2].as<std::string>());
  }

  {
    Node doc = docs[1];
    EXPECT_EQ(2, doc.size());
    EXPECT_EQ("Chicago Cubs", doc[0].as<std::string>());
    EXPECT_EQ("St Louis Cardinals", doc[1].as<std::string>());
  }
}

TEST(NodeSpecTest, Ex2_8_PlayByPlayFeed) {
  std::vector<Node> docs = LoadAll(ex2_8);
  EXPECT_EQ(2, docs.size());

  {
    Node doc = docs[0];
    EXPECT_EQ(3, doc.size());
    EXPECT_EQ("20:03:20", doc["time"].as<std::string>());
    EXPECT_EQ("Sammy Sosa", doc["player"].as<std::string>());
    EXPECT_EQ("strike (miss)", doc["action"].as<std::string>());
  }

  {
    Node doc = docs[1];
    EXPECT_EQ(3, doc.size());
    EXPECT_EQ("20:03:47", doc["time"].as<std::string>());
    EXPECT_EQ("Sammy Sosa", doc["player"].as<std::string>());
    EXPECT_EQ("grand slam", doc["action"].as<std::string>());
  }
}

TEST(NodeSpecTest, Ex2_9_SingleDocumentWithTwoComments) {
  Node doc = Load(ex2_9);
  EXPECT_EQ(2, doc.size());
  EXPECT_EQ(2, doc["hr"].size());
  EXPECT_EQ("Mark McGwire", doc["hr"][0].as<std::string>());
  EXPECT_EQ("Sammy Sosa", doc["hr"][1].as<std::string>());
  EXPECT_EQ(2, doc["rbi"].size());
  EXPECT_EQ("Sammy Sosa", doc["rbi"][0].as<std::string>());
  EXPECT_EQ("Ken Griffey", doc["rbi"][1].as<std::string>());
}

TEST(NodeSpecTest, Ex2_10_SimpleAnchor) {
  Node doc = Load(ex2_10);
  EXPECT_EQ(2, doc.size());
  EXPECT_EQ(2, doc["hr"].size());
  EXPECT_EQ("Mark McGwire", doc["hr"][0].as<std::string>());
  EXPECT_EQ("Sammy Sosa", doc["hr"][1].as<std::string>());
  EXPECT_EQ(2, doc["rbi"].size());
  EXPECT_EQ("Sammy Sosa", doc["rbi"][0].as<std::string>());
  EXPECT_EQ("Ken Griffey", doc["rbi"][1].as<std::string>());
}

TEST(NodeSpecTest, Ex2_11_MappingBetweenSequences) {
  Node doc = Load(ex2_11);

  std::vector<std::string> tigers_cubs;
  tigers_cubs.push_back("Detroit Tigers");
  tigers_cubs.push_back("Chicago cubs");

  std::vector<std::string> yankees_braves;
  yankees_braves.push_back("New York Yankees");
  yankees_braves.push_back("Atlanta Braves");

  EXPECT_EQ(2, doc.size());
  EXPECT_EQ(1, doc[tigers_cubs].size());
  EXPECT_EQ("2001-07-23", doc[tigers_cubs][0].as<std::string>());
  EXPECT_EQ(3, doc[yankees_braves].size());
  EXPECT_EQ("2001-07-02", doc[yankees_braves][0].as<std::string>());
  EXPECT_EQ("2001-08-12", doc[yankees_braves][1].as<std::string>());
  EXPECT_EQ("2001-08-14", doc[yankees_braves][2].as<std::string>());
}

TEST(NodeSpecTest, Ex2_12_CompactNestedMapping) {
  Node doc = Load(ex2_12);
  EXPECT_EQ(3, doc.size());
  EXPECT_EQ(2, doc[0].size());
  EXPECT_EQ("Super Hoop", doc[0]["item"].as<std::string>());
  EXPECT_EQ(1, doc[0]["quantity"].as<int>());
  EXPECT_EQ(2, doc[1].size());
  EXPECT_EQ("Basketball", doc[1]["item"].as<std::string>());
  EXPECT_EQ(4, doc[1]["quantity"].as<int>());
  EXPECT_EQ(2, doc[2].size());
  EXPECT_EQ("Big Shoes", doc[2]["item"].as<std::string>());
  EXPECT_EQ(1, doc[2]["quantity"].as<int>());
}

TEST(NodeSpecTest, Ex2_13_InLiteralsNewlinesArePreserved) {
  Node doc = Load(ex2_13);
  EXPECT_TRUE(doc.as<std::string>() ==
              "\\//||\\/||\n"
              "// ||  ||__");
}

TEST(NodeSpecTest, Ex2_14_InFoldedScalarsNewlinesBecomeSpaces) {
  Node doc = Load(ex2_14);
  EXPECT_TRUE(doc.as<std::string>() ==
              "Mark McGwire's year was crippled by a knee injury.");
}

TEST(NodeSpecTest,
     Ex2_15_FoldedNewlinesArePreservedForMoreIndentedAndBlankLines) {
  Node doc = Load(ex2_15);
  EXPECT_TRUE(doc.as<std::string>() ==
              "Sammy Sosa completed another fine season with great stats.\n\n"
              "  63 Home Runs\n"
              "  0.288 Batting Average\n\n"
              "What a year!");
}

TEST(NodeSpecTest, Ex2_16_IndentationDeterminesScope) {
  Node doc = Load(ex2_16);
  EXPECT_EQ(3, doc.size());
  EXPECT_EQ("Mark McGwire", doc["name"].as<std::string>());
  EXPECT_TRUE(doc["accomplishment"].as<std::string>() ==
              "Mark set a major league home run record in 1998.\n");
  EXPECT_TRUE(doc["stats"].as<std::string>() ==
              "65 Home Runs\n0.278 Batting Average\n");
}

TEST(NodeSpecTest, Ex2_17_QuotedScalars) {
  Node doc = Load(ex2_17);
  EXPECT_EQ(6, doc.size());
  EXPECT_EQ("Sosa did fine.\xe2\x98\xba", doc["unicode"].as<std::string>());
  EXPECT_EQ("\b1998\t1999\t2000\n", doc["control"].as<std::string>());
  EXPECT_EQ("\x0d\x0a is \r\n", doc["hex esc"].as<std::string>());
  EXPECT_EQ("\"Howdy!\" he cried.", doc["single"].as<std::string>());
  EXPECT_EQ(" # Not a 'comment'.", doc["quoted"].as<std::string>());
  EXPECT_EQ("|\\-*-/|", doc["tie-fighter"].as<std::string>());
}

TEST(NodeSpecTest, Ex2_18_MultiLineFlowScalars) {
  Node doc = Load(ex2_18);
  EXPECT_EQ(2, doc.size());
  EXPECT_TRUE(doc["plain"].as<std::string>() ==
              "This unquoted scalar spans many lines.");
  EXPECT_TRUE(doc["quoted"].as<std::string>() ==
              "So does this quoted scalar.\n");
}

// TODO: 2.19 - 2.22 schema tags

TEST(NodeSpecTest, Ex2_23_VariousExplicitTags) {
  Node doc = Load(ex2_23);
  EXPECT_EQ(3, doc.size());
  EXPECT_EQ("tag:yaml.org,2002:str", doc["not-date"].Tag());
  EXPECT_EQ("2002-04-28", doc["not-date"].as<std::string>());
  EXPECT_EQ("tag:yaml.org,2002:binary", doc["picture"].Tag());
  EXPECT_TRUE(doc["picture"].as<std::string>() ==
              "R0lGODlhDAAMAIQAAP//9/X\n"
              "17unp5WZmZgAAAOfn515eXv\n"
              "Pz7Y6OjuDg4J+fn5OTk6enp\n"
              "56enmleECcgggoBADs=\n");
  EXPECT_EQ("!something", doc["application specific tag"].Tag());
  EXPECT_TRUE(doc["application specific tag"].as<std::string>() ==
              "The semantics of the tag\n"
              "above may be different for\n"
              "different documents.");
}

TEST(NodeSpecTest, Ex2_24_GlobalTags) {
  Node doc = Load(ex2_24);
  EXPECT_EQ("tag:clarkevans.com,2002:shape", doc.Tag());
  EXPECT_EQ(3, doc.size());
  EXPECT_EQ("tag:clarkevans.com,2002:circle", doc[0].Tag());
  EXPECT_EQ(2, doc[0].size());
  EXPECT_EQ(2, doc[0]["center"].size());
  EXPECT_EQ(73, doc[0]["center"]["x"].as<int>());
  EXPECT_EQ(129, doc[0]["center"]["y"].as<int>());
  EXPECT_EQ(7, doc[0]["radius"].as<int>());
  EXPECT_EQ("tag:clarkevans.com,2002:line", doc[1].Tag());
  EXPECT_EQ(2, doc[1].size());
  EXPECT_EQ(2, doc[1]["start"].size());
  EXPECT_EQ(73, doc[1]["start"]["x"].as<int>());
  EXPECT_EQ(129, doc[1]["start"]["y"].as<int>());
  EXPECT_EQ(2, doc[1]["finish"].size());
  EXPECT_EQ(89, doc[1]["finish"]["x"].as<int>());
  EXPECT_EQ(102, doc[1]["finish"]["y"].as<int>());
  EXPECT_EQ("tag:clarkevans.com,2002:label", doc[2].Tag());
  EXPECT_EQ(3, doc[2].size());
  EXPECT_EQ(2, doc[2]["start"].size());
  EXPECT_EQ(73, doc[2]["start"]["x"].as<int>());
  EXPECT_EQ(129, doc[2]["start"]["y"].as<int>());
  EXPECT_EQ("0xFFEEBB", doc[2]["color"].as<std::string>());
  EXPECT_EQ("Pretty vector drawing.", doc[2]["text"].as<std::string>());
}

TEST(NodeSpecTest, Ex2_25_UnorderedSets) {
  Node doc = Load(ex2_25);
  EXPECT_EQ("tag:yaml.org,2002:set", doc.Tag());
  EXPECT_EQ(3, doc.size());
  EXPECT_TRUE(doc["Mark McGwire"].IsNull());
  EXPECT_TRUE(doc["Sammy Sosa"].IsNull());
  EXPECT_TRUE(doc["Ken Griffey"].IsNull());
}

TEST(NodeSpecTest, Ex2_16_OrderedMappings) {
  Node doc = Load(ex2_26);
  EXPECT_EQ("tag:yaml.org,2002:omap", doc.Tag());
  EXPECT_EQ(3, doc.size());
  EXPECT_EQ(1, doc[0].size());
  EXPECT_EQ(65, doc[0]["Mark McGwire"].as<int>());
  EXPECT_EQ(1, doc[1].size());
  EXPECT_EQ(63, doc[1]["Sammy Sosa"].as<int>());
  EXPECT_EQ(1, doc[2].size());
  EXPECT_EQ(58, doc[2]["Ken Griffey"].as<int>());
}

TEST(NodeSpecTest, Ex2_27_Invoice) {
  Node doc = Load(ex2_27);
  EXPECT_EQ("tag:clarkevans.com,2002:invoice", doc.Tag());
  EXPECT_EQ(8, doc.size());
  EXPECT_EQ(34843, doc["invoice"].as<int>());
  EXPECT_EQ("2001-01-23", doc["date"].as<std::string>());
  EXPECT_EQ(3, doc["bill-to"].size());
  EXPECT_EQ("Chris", doc["bill-to"]["given"].as<std::string>());
  EXPECT_EQ("Dumars", doc["bill-to"]["family"].as<std::string>());
  EXPECT_EQ(4, doc["bill-to"]["address"].size());
  EXPECT_TRUE(doc["bill-to"]["address"]["lines"].as<std::string>() ==
              "458 Walkman Dr.\nSuite #292\n");
  EXPECT_TRUE(doc["bill-to"]["address"]["city"].as<std::string>() ==
              "Royal Oak");
  EXPECT_EQ("MI", doc["bill-to"]["address"]["state"].as<std::string>());
  EXPECT_EQ("48046", doc["bill-to"]["address"]["postal"].as<std::string>());
  EXPECT_EQ(3, doc["ship-to"].size());
  EXPECT_EQ("Chris", doc["ship-to"]["given"].as<std::string>());
  EXPECT_EQ("Dumars", doc["ship-to"]["family"].as<std::string>());
  EXPECT_EQ(4, doc["ship-to"]["address"].size());
  EXPECT_TRUE(doc["ship-to"]["address"]["lines"].as<std::string>() ==
              "458 Walkman Dr.\nSuite #292\n");
  EXPECT_TRUE(doc["ship-to"]["address"]["city"].as<std::string>() ==
              "Royal Oak");
  EXPECT_EQ("MI", doc["ship-to"]["address"]["state"].as<std::string>());
  EXPECT_EQ("48046", doc["ship-to"]["address"]["postal"].as<std::string>());
  EXPECT_EQ(2, doc["product"].size());
  EXPECT_EQ(4, doc["product"][0].size());
  EXPECT_EQ("BL394D", doc["product"][0]["sku"].as<std::string>());
  EXPECT_EQ(4, doc["product"][0]["quantity"].as<int>());
  EXPECT_TRUE(doc["product"][0]["description"].as<std::string>() ==
              "Basketball");
  EXPECT_EQ("450.00", doc["product"][0]["price"].as<std::string>());
  EXPECT_EQ(4, doc["product"][1].size());
  EXPECT_EQ("BL4438H", doc["product"][1]["sku"].as<std::string>());
  EXPECT_EQ(1, doc["product"][1]["quantity"].as<int>());
  EXPECT_TRUE(doc["product"][1]["description"].as<std::string>() ==
              "Super Hoop");
  EXPECT_EQ("2392.00", doc["product"][1]["price"].as<std::string>());
  EXPECT_EQ("251.42", doc["tax"].as<std::string>());
  EXPECT_EQ("4443.52", doc["total"].as<std::string>());
  EXPECT_EQ(
      "Late afternoon is best. Backup contact is Nancy Billsmer @ 338-4338.",
      doc["comments"].as<std::string>());
}

TEST(NodeSpecTest, Ex2_28_LogFile) {
  std::vector<Node> docs = LoadAll(ex2_28);
  EXPECT_EQ(3, docs.size());

  {
    Node doc = docs[0];
    EXPECT_EQ(3, doc.size());
    EXPECT_EQ("2001-11-23 15:01:42 -5", doc["Time"].as<std::string>());
    EXPECT_EQ("ed", doc["User"].as<std::string>());
    EXPECT_TRUE(doc["Warning"].as<std::string>() ==
                "This is an error message for the log file");
  }

  {
    Node doc = docs[1];
    EXPECT_EQ(3, doc.size());
    EXPECT_EQ("2001-11-23 15:02:31 -5", doc["Time"].as<std::string>());
    EXPECT_EQ("ed", doc["User"].as<std::string>());
    EXPECT_TRUE(doc["Warning"].as<std::string>() ==
                "A slightly different error message.");
  }

  {
    Node doc = docs[2];
    EXPECT_EQ(4, doc.size());
    EXPECT_EQ("2001-11-23 15:03:17 -5", doc["Date"].as<std::string>());
    EXPECT_EQ("ed", doc["User"].as<std::string>());
    EXPECT_EQ("Unknown variable \"bar\"", doc["Fatal"].as<std::string>());
    EXPECT_EQ(2, doc["Stack"].size());
    EXPECT_EQ(3, doc["Stack"][0].size());
    EXPECT_EQ("TopClass.py", doc["Stack"][0]["file"].as<std::string>());
    EXPECT_EQ("23", doc["Stack"][0]["line"].as<std::string>());
    EXPECT_TRUE(doc["Stack"][0]["code"].as<std::string>() ==
                "x = MoreObject(\"345\\n\")\n");
    EXPECT_EQ(3, doc["Stack"][1].size());
    EXPECT_EQ("MoreClass.py", doc["Stack"][1]["file"].as<std::string>());
    EXPECT_EQ("58", doc["Stack"][1]["line"].as<std::string>());
    EXPECT_EQ("foo = bar", doc["Stack"][1]["code"].as<std::string>());
  }
}

// TODO: 5.1 - 5.2 BOM

TEST(NodeSpecTest, Ex5_3_BlockStructureIndicators) {
  Node doc = Load(ex5_3);
  EXPECT_EQ(2, doc.size());
  EXPECT_EQ(2, doc["sequence"].size());
  EXPECT_EQ("one", doc["sequence"][0].as<std::string>());
  EXPECT_EQ("two", doc["sequence"][1].as<std::string>());
  EXPECT_EQ(2, doc["mapping"].size());
  EXPECT_EQ("blue", doc["mapping"]["sky"].as<std::string>());
  EXPECT_EQ("green", doc["mapping"]["sea"].as<std::string>());
}

TEST(NodeSpecTest, Ex5_4_FlowStructureIndicators) {
  Node doc = Load(ex5_4);
  EXPECT_EQ(2, doc.size());
  EXPECT_EQ(2, doc["sequence"].size());
  EXPECT_EQ("one", doc["sequence"][0].as<std::string>());
  EXPECT_EQ("two", doc["sequence"][1].as<std::string>());
  EXPECT_EQ(2, doc["mapping"].size());
  EXPECT_EQ("blue", doc["mapping"]["sky"].as<std::string>());
  EXPECT_EQ("green", doc["mapping"]["sea"].as<std::string>());
}

TEST(NodeSpecTest, Ex5_5_CommentIndicator) {
  Node doc = Load(ex5_5);
  EXPECT_TRUE(doc.IsNull());
}

TEST(NodeSpecTest, Ex5_6_NodePropertyIndicators) {
  Node doc = Load(ex5_6);
  EXPECT_EQ(2, doc.size());
  EXPECT_TRUE(doc["anchored"].as<std::string>() ==
              "value");  // TODO: assert tag
  EXPECT_EQ("value", doc["alias"].as<std::string>());
}

TEST(NodeSpecTest, Ex5_7_BlockScalarIndicators) {
  Node doc = Load(ex5_7);
  EXPECT_EQ(2, doc.size());
  EXPECT_EQ("some\ntext\n", doc["literal"].as<std::string>());
  EXPECT_EQ("some text\n", doc["folded"].as<std::string>());
}

TEST(NodeSpecTest, Ex5_8_QuotedScalarIndicators) {
  Node doc = Load(ex5_8);
  EXPECT_EQ(2, doc.size());
  EXPECT_EQ("text", doc["single"].as<std::string>());
  EXPECT_EQ("text", doc["double"].as<std::string>());
}

// TODO: 5.9 directive
// TODO: 5.10 reserved indicator

TEST(NodeSpecTest, Ex5_11_LineBreakCharacters) {
  Node doc = Load(ex5_11);
  EXPECT_TRUE(doc.as<std::string>() ==
              "Line break (no glyph)\nLine break (glyphed)\n");
}

TEST(NodeSpecTest, Ex5_12_TabsAndSpaces) {
  Node doc = Load(ex5_12);
  EXPECT_EQ(2, doc.size());
  EXPECT_EQ("Quoted\t", doc["quoted"].as<std::string>());
  EXPECT_TRUE(doc["block"].as<std::string>() ==
              "void main() {\n"
              "\tprintf(\"Hello, world!\\n\");\n"
              "}");
}

TEST(NodeSpecTest, Ex5_13_EscapedCharacters) {
  Node doc = Load(ex5_13);
  EXPECT_TRUE(doc.as<std::string>() ==
              "Fun with \x5C \x22 \x07 \x08 \x1B \x0C \x0A \x0D \x09 \x0B " +
                  std::string("\x00", 1) +
                  " \x20 \xA0 \x85 \xe2\x80\xa8 \xe2\x80\xa9 A A A");
}

TEST(NodeSpecTest, Ex5_14_InvalidEscapedCharacters) {
  EXPECT_THROW_PARSER_EXCEPTION(Load(ex5_14),
                                std::string(ErrorMsg::INVALID_ESCAPE) + "c");
}

TEST(NodeSpecTest, Ex6_1_IndentationSpaces) {
  Node doc = Load(ex6_1);
  EXPECT_EQ(1, doc.size());
  EXPECT_EQ(2, doc["Not indented"].size());
  EXPECT_TRUE(doc["Not indented"]["By one space"].as<std::string>() ==
              "By four\n  spaces\n");
  EXPECT_EQ(3, doc["Not indented"]["Flow style"].size());
  EXPECT_TRUE(doc["Not indented"]["Flow style"][0].as<std::string>() ==
              "By two");
  EXPECT_TRUE(doc["Not indented"]["Flow style"][1].as<std::string>() ==
              "Also by two");
  EXPECT_TRUE(doc["Not indented"]["Flow style"][2].as<std::string>() ==
              "Still by two");
}

TEST(NodeSpecTest, Ex6_2_IndentationIndicators) {
  Node doc = Load(ex6_2);
  EXPECT_EQ(1, doc.size());
  EXPECT_EQ(2, doc["a"].size());
  EXPECT_EQ("b", doc["a"][0].as<std::string>());
  EXPECT_EQ(2, doc["a"][1].size());
  EXPECT_EQ("c", doc["a"][1][0].as<std::string>());
  EXPECT_EQ("d", doc["a"][1][1].as<std::string>());
}

TEST(NodeSpecTest, Ex6_3_SeparationSpaces) {
  Node doc = Load(ex6_3);
  EXPECT_EQ(2, doc.size());
  EXPECT_EQ(1, doc[0].size());
  EXPECT_EQ("bar", doc[0]["foo"].as<std::string>());
  EXPECT_EQ(2, doc[1].size());
  EXPECT_EQ("baz", doc[1][0].as<std::string>());
  EXPECT_EQ("baz", doc[1][1].as<std::string>());
}

TEST(NodeSpecTest, Ex6_4_LinePrefixes) {
  Node doc = Load(ex6_4);
  EXPECT_EQ(3, doc.size());
  EXPECT_EQ("text lines", doc["plain"].as<std::string>());
  EXPECT_EQ("text lines", doc["quoted"].as<std::string>());
  EXPECT_EQ("text\n \tlines\n", doc["block"].as<std::string>());
}

TEST(NodeSpecTest, Ex6_5_EmptyLines) {
  Node doc = Load(ex6_5);
  EXPECT_EQ(2, doc.size());
  EXPECT_EQ("Empty line\nas a line feed", doc["Folding"].as<std::string>());
  EXPECT_EQ("Clipped empty lines\n", doc["Chomping"].as<std::string>());
}

TEST(NodeSpecTest, Ex6_6_LineFolding) {
  Node doc = Load(ex6_6);
  EXPECT_EQ("trimmed\n\n\nas space", doc.as<std::string>());
}

TEST(NodeSpecTest, Ex6_7_BlockFolding) {
  Node doc = Load(ex6_7);
  EXPECT_EQ("foo \n\n\t bar\n\nbaz\n", doc.as<std::string>());
}

TEST(NodeSpecTest, Ex6_8_FlowFolding) {
  Node doc = Load(ex6_8);
  EXPECT_EQ(" foo\nbar\nbaz ", doc.as<std::string>());
}

TEST(NodeSpecTest, Ex6_9_SeparatedComment) {
  Node doc = Load(ex6_9);
  EXPECT_EQ(1, doc.size());
  EXPECT_EQ("value", doc["key"].as<std::string>());
}

TEST(NodeSpecTest, Ex6_10_CommentLines) {
  Node doc = Load(ex6_10);
  EXPECT_TRUE(doc.IsNull());
}

TEST(NodeSpecTest, Ex6_11_MultiLineComments) {
  Node doc = Load(ex6_11);
  EXPECT_EQ(1, doc.size());
  EXPECT_EQ("value", doc["key"].as<std::string>());
}

TEST(NodeSpecTest, Ex6_12_SeparationSpacesII) {
  Node doc = Load(ex6_12);

  std::map<std::string, std::string> sammy;
  sammy["first"] = "Sammy";
  sammy["last"] = "Sosa";

  EXPECT_EQ(1, doc.size());
  EXPECT_EQ(2, doc[sammy].size());
  EXPECT_EQ(65, doc[sammy]["hr"].as<int>());
  EXPECT_EQ("0.278", doc[sammy]["avg"].as<std::string>());
}

TEST(NodeSpecTest, Ex6_13_ReservedDirectives) {
  Node doc = Load(ex6_13);
  EXPECT_EQ("foo", doc.as<std::string>());
}

TEST(NodeSpecTest, Ex6_14_YAMLDirective) {
  Node doc = Load(ex6_14);
  EXPECT_EQ("foo", doc.as<std::string>());
}

TEST(NodeSpecTest, Ex6_15_InvalidRepeatedYAMLDirective) {
  EXPECT_THROW_PARSER_EXCEPTION(Load(ex6_15),
                                ErrorMsg::REPEATED_YAML_DIRECTIVE);
}

TEST(NodeSpecTest, Ex6_16_TagDirective) {
  Node doc = Load(ex6_16);
  EXPECT_EQ("tag:yaml.org,2002:str", doc.Tag());
  EXPECT_EQ("foo", doc.as<std::string>());
}

TEST(NodeSpecTest, Ex6_17_InvalidRepeatedTagDirective) {
  EXPECT_THROW_PARSER_EXCEPTION(Load(ex6_17), ErrorMsg::REPEATED_TAG_DIRECTIVE);
}

TEST(NodeSpecTest, Ex6_18_PrimaryTagHandle) {
  std::vector<Node> docs = LoadAll(ex6_18);
  EXPECT_EQ(2, docs.size());

  {
    Node doc = docs[0];
    EXPECT_EQ("!foo", doc.Tag());
    EXPECT_EQ("bar", doc.as<std::string>());
  }

  {
    Node doc = docs[1];
    EXPECT_EQ("tag:example.com,2000:app/foo", doc.Tag());
    EXPECT_EQ("bar", doc.as<std::string>());
  }
}

TEST(NodeSpecTest, Ex6_19_SecondaryTagHandle) {
  Node doc = Load(ex6_19);
  EXPECT_EQ("tag:example.com,2000:app/int", doc.Tag());
  EXPECT_EQ("1 - 3", doc.as<std::string>());
}

TEST(NodeSpecTest, Ex6_20_TagHandles) {
  Node doc = Load(ex6_20);
  EXPECT_EQ("tag:example.com,2000:app/foo", doc.Tag());
  EXPECT_EQ("bar", doc.as<std::string>());
}

TEST(NodeSpecTest, Ex6_21_LocalTagPrefix) {
  std::vector<Node> docs = LoadAll(ex6_21);
  EXPECT_EQ(2, docs.size());

  {
    Node doc = docs[0];
    EXPECT_EQ("!my-light", doc.Tag());
    EXPECT_EQ("fluorescent", doc.as<std::string>());
  }

  {
    Node doc = docs[1];
    EXPECT_EQ("!my-light", doc.Tag());
    EXPECT_EQ("green", doc.as<std::string>());
  }
}

TEST(NodeSpecTest, Ex6_22_GlobalTagPrefix) {
  Node doc = Load(ex6_22);
  EXPECT_EQ(1, doc.size());
  EXPECT_EQ("tag:example.com,2000:app/foo", doc[0].Tag());
  EXPECT_EQ("bar", doc[0].as<std::string>());
}

TEST(NodeSpecTest, Ex6_23_NodeProperties) {
  Node doc = Load(ex6_23);
  EXPECT_EQ(2, doc.size());
  for (const_iterator it = doc.begin(); it != doc.end(); ++it) {
    if (it->first.as<std::string>() == "foo") {
      EXPECT_EQ("tag:yaml.org,2002:str", it->first.Tag());
      EXPECT_EQ("tag:yaml.org,2002:str", it->second.Tag());
      EXPECT_EQ("bar", it->second.as<std::string>());
    } else if (it->first.as<std::string>() == "baz") {
      EXPECT_EQ("foo", it->second.as<std::string>());
    } else
      FAIL() << "unknown key";
  }
}

TEST(NodeSpecTest, Ex6_24_VerbatimTags) {
  Node doc = Load(ex6_24);
  EXPECT_EQ(1, doc.size());
  for (const_iterator it = doc.begin(); it != doc.end(); ++it) {
    EXPECT_EQ("tag:yaml.org,2002:str", it->first.Tag());
    EXPECT_EQ("foo", it->first.as<std::string>());
    EXPECT_EQ("!bar", it->second.Tag());
    EXPECT_EQ("baz", it->second.as<std::string>());
  }
}

TEST(NodeSpecTest, DISABLED_Ex6_25_InvalidVerbatimTags) {
  Node doc = Load(ex6_25);
  // TODO: check tags (but we probably will say these are valid, I think)
  FAIL() << "not implemented yet";
}

TEST(NodeSpecTest, Ex6_26_TagShorthands) {
  Node doc = Load(ex6_26);
  EXPECT_EQ(3, doc.size());
  EXPECT_EQ("!local", doc[0].Tag());
  EXPECT_EQ("foo", doc[0].as<std::string>());
  EXPECT_EQ("tag:yaml.org,2002:str", doc[1].Tag());
  EXPECT_EQ("bar", doc[1].as<std::string>());
  EXPECT_EQ("tag:example.com,2000:app/tag%21", doc[2].Tag());
  EXPECT_EQ("baz", doc[2].as<std::string>());
}

TEST(NodeSpecTest, Ex6_27a_InvalidTagShorthands) {
  EXPECT_THROW_PARSER_EXCEPTION(Load(ex6_27a), ErrorMsg::TAG_WITH_NO_SUFFIX);
}

// TODO: should we reject this one (since !h! is not declared)?
TEST(NodeSpecTest, DISABLED_Ex6_27b_InvalidTagShorthands) {
  Load(ex6_27b);
  FAIL() << "not implemented yet";
}

TEST(NodeSpecTest, Ex6_28_NonSpecificTags) {
  Node doc = Load(ex6_28);
  EXPECT_EQ(3, doc.size());
  EXPECT_EQ("12", doc[0].as<std::string>());  // TODO: check tags. How?
  EXPECT_EQ(12, doc[1].as<int>());
  EXPECT_EQ("12", doc[2].as<std::string>());
}

TEST(NodeSpecTest, Ex6_29_NodeAnchors) {
  Node doc = Load(ex6_29);
  EXPECT_EQ(2, doc.size());
  EXPECT_EQ("Value", doc["First occurrence"].as<std::string>());
  EXPECT_EQ("Value", doc["Second occurrence"].as<std::string>());
}

TEST(NodeSpecTest, Ex7_1_AliasNodes) {
  Node doc = Load(ex7_1);
  EXPECT_EQ(4, doc.size());
  EXPECT_EQ("Foo", doc["First occurrence"].as<std::string>());
  EXPECT_EQ("Foo", doc["Second occurrence"].as<std::string>());
  EXPECT_EQ("Bar", doc["Override anchor"].as<std::string>());
  EXPECT_EQ("Bar", doc["Reuse anchor"].as<std::string>());
}

TEST(NodeSpecTest, Ex7_2_EmptyNodes) {
  Node doc = Load(ex7_2);
  EXPECT_EQ(2, doc.size());
  for (const_iterator it = doc.begin(); it != doc.end(); ++it) {
    if (it->first.as<std::string>() == "foo") {
      EXPECT_EQ("tag:yaml.org,2002:str", it->second.Tag());
      EXPECT_EQ("", it->second.as<std::string>());
    } else if (it->first.as<std::string>() == "") {
      EXPECT_EQ("tag:yaml.org,2002:str", it->first.Tag());
      EXPECT_EQ("bar", it->second.as<std::string>());
    } else
      FAIL() << "unexpected key";
  }
}

TEST(NodeSpecTest, Ex7_3_CompletelyEmptyNodes) {
  Node doc = Load(ex7_3);
  EXPECT_EQ(2, doc.size());
  EXPECT_TRUE(doc["foo"].IsNull());
  EXPECT_EQ("bar", doc[Null].as<std::string>());
}

TEST(NodeSpecTest, Ex7_4_DoubleQuotedImplicitKeys) {
  Node doc = Load(ex7_4);
  EXPECT_EQ(1, doc.size());
  EXPECT_EQ(1, doc["implicit block key"].size());
  EXPECT_EQ(1, doc["implicit block key"][0].size());
  EXPECT_EQ(
      "value",
      doc["implicit block key"][0]["implicit flow key"].as<std::string>());
}

TEST(NodeSpecTest, Ex7_5_DoubleQuotedLineBreaks) {
  Node doc = Load(ex7_5);
  EXPECT_TRUE(doc.as<std::string>() ==
              "folded to a space,\nto a line feed, or \t \tnon-content");
}

TEST(NodeSpecTest, Ex7_6_DoubleQuotedLines) {
  Node doc = Load(ex7_6);
  EXPECT_TRUE(doc.as<std::string>() ==
              " 1st non-empty\n2nd non-empty 3rd non-empty ");
}

TEST(NodeSpecTest, Ex7_7_SingleQuotedCharacters) {
  Node doc = Load(ex7_7);
  EXPECT_EQ("here's to \"quotes\"", doc.as<std::string>());
}

TEST(NodeSpecTest, Ex7_8_SingleQuotedImplicitKeys) {
  Node doc = Load(ex7_8);
  EXPECT_EQ(1, doc.size());
  EXPECT_EQ(1, doc["implicit block key"].size());
  EXPECT_EQ(1, doc["implicit block key"][0].size());
  EXPECT_EQ(
      "value",
      doc["implicit block key"][0]["implicit flow key"].as<std::string>());
}

TEST(NodeSpecTest, Ex7_9_SingleQuotedLines) {
  Node doc = Load(ex7_9);
  EXPECT_TRUE(doc.as<std::string>() ==
              " 1st non-empty\n2nd non-empty 3rd non-empty ");
}

TEST(NodeSpecTest, Ex7_10_PlainCharacters) {
  Node doc = Load(ex7_10);
  EXPECT_EQ(6, doc.size());
  EXPECT_EQ("::vector", doc[0].as<std::string>());
  EXPECT_EQ(": - ()", doc[1].as<std::string>());
  EXPECT_EQ("Up, up, and away!", doc[2].as<std::string>());
  EXPECT_EQ(-123, doc[3].as<int>());
  EXPECT_EQ("http://example.com/foo#bar", doc[4].as<std::string>());
  EXPECT_EQ(5, doc[5].size());
  EXPECT_EQ("::vector", doc[5][0].as<std::string>());
  EXPECT_EQ(": - ()", doc[5][1].as<std::string>());
  EXPECT_EQ("Up, up, and away!", doc[5][2].as<std::string>());
  EXPECT_EQ(-123, doc[5][3].as<int>());
  EXPECT_EQ("http://example.com/foo#bar", doc[5][4].as<std::string>());
}

TEST(NodeSpecTest, Ex7_11_PlainImplicitKeys) {
  Node doc = Load(ex7_11);
  EXPECT_EQ(1, doc.size());
  EXPECT_EQ(1, doc["implicit block key"].size());
  EXPECT_EQ(1, doc["implicit block key"][0].size());
  EXPECT_EQ(
      "value",
      doc["implicit block key"][0]["implicit flow key"].as<std::string>());
}

TEST(NodeSpecTest, Ex7_12_PlainLines) {
  Node doc = Load(ex7_12);
  EXPECT_TRUE(doc.as<std::string>() ==
              "1st non-empty\n2nd non-empty 3rd non-empty");
}

TEST(NodeSpecTest, Ex7_13_FlowSequence) {
  Node doc = Load(ex7_13);
  EXPECT_EQ(2, doc.size());
  EXPECT_EQ(2, doc[0].size());
  EXPECT_EQ("one", doc[0][0].as<std::string>());
  EXPECT_EQ("two", doc[0][1].as<std::string>());
  EXPECT_EQ(2, doc[1].size());
  EXPECT_EQ("three", doc[1][0].as<std::string>());
  EXPECT_EQ("four", doc[1][1].as<std::string>());
}

TEST(NodeSpecTest, Ex7_14_FlowSequenceEntries) {
  Node doc = Load(ex7_14);
  EXPECT_EQ(5, doc.size());
  EXPECT_EQ("double quoted", doc[0].as<std::string>());
  EXPECT_EQ("single quoted", doc[1].as<std::string>());
  EXPECT_EQ("plain text", doc[2].as<std::string>());
  EXPECT_EQ(1, doc[3].size());
  EXPECT_EQ("nested", doc[3][0].as<std::string>());
  EXPECT_EQ(1, doc[4].size());
  EXPECT_EQ("pair", doc[4]["single"].as<std::string>());
}

TEST(NodeSpecTest, Ex7_15_FlowMappings) {
  Node doc = Load(ex7_15);
  EXPECT_EQ(2, doc.size());
  EXPECT_EQ(2, doc[0].size());
  EXPECT_EQ("two", doc[0]["one"].as<std::string>());
  EXPECT_EQ("four", doc[0]["three"].as<std::string>());
  EXPECT_EQ(2, doc[1].size());
  EXPECT_EQ("six", doc[1]["five"].as<std::string>());
  EXPECT_EQ("eight", doc[1]["seven"].as<std::string>());
}

TEST(NodeSpecTest, Ex7_16_FlowMappingEntries) {
  Node doc = Load(ex7_16);
  EXPECT_EQ(3, doc.size());
  EXPECT_EQ("entry", doc["explicit"].as<std::string>());
  EXPECT_EQ("entry", doc["implicit"].as<std::string>());
  EXPECT_TRUE(doc[Null].IsNull());
}

TEST(NodeSpecTest, Ex7_17_FlowMappingSeparateValues) {
  Node doc = Load(ex7_17);
  EXPECT_EQ(4, doc.size());
  EXPECT_EQ("separate", doc["unquoted"].as<std::string>());
  EXPECT_TRUE(doc["http://foo.com"].IsNull());
  EXPECT_TRUE(doc["omitted value"].IsNull());
  EXPECT_EQ("omitted key", doc[Null].as<std::string>());
}

TEST(NodeSpecTest, Ex7_18_FlowMappingAdjacentValues) {
  Node doc = Load(ex7_18);
  EXPECT_EQ(3, doc.size());
  EXPECT_EQ("value", doc["adjacent"].as<std::string>());
  EXPECT_EQ("value", doc["readable"].as<std::string>());
  EXPECT_TRUE(doc["empty"].IsNull());
}

TEST(NodeSpecTest, Ex7_19_SinglePairFlowMappings) {
  Node doc = Load(ex7_19);
  EXPECT_EQ(1, doc.size());
  EXPECT_EQ(1, doc[0].size());
  EXPECT_EQ("bar", doc[0]["foo"].as<std::string>());
}

TEST(NodeSpecTest, Ex7_20_SinglePairExplicitEntry) {
  Node doc = Load(ex7_20);
  EXPECT_EQ(1, doc.size());
  EXPECT_EQ(1, doc[0].size());
  EXPECT_EQ("baz", doc[0]["foo bar"].as<std::string>());
}

TEST(NodeSpecTest, Ex7_21_SinglePairImplicitEntries) {
  Node doc = Load(ex7_21);
  EXPECT_EQ(3, doc.size());
  EXPECT_EQ(1, doc[0].size());
  EXPECT_EQ(1, doc[0][0].size());
  EXPECT_EQ("separate", doc[0][0]["YAML"].as<std::string>());
  EXPECT_EQ(1, doc[1].size());
  EXPECT_EQ(1, doc[1][0].size());
  EXPECT_EQ("empty key entry", doc[1][0][Null].as<std::string>());
  EXPECT_EQ(1, doc[2].size());
  EXPECT_EQ(1, doc[2][0].size());

  std::map<std::string, std::string> key;
  key["JSON"] = "like";
  EXPECT_EQ("adjacent", doc[2][0][key].as<std::string>());
}

TEST(NodeSpecTest, Ex7_22_InvalidImplicitKeys) {
  EXPECT_THROW_PARSER_EXCEPTION(Load(ex7_22), ErrorMsg::END_OF_SEQ_FLOW);
}

TEST(NodeSpecTest, Ex7_23_FlowContent) {
  Node doc = Load(ex7_23);
  EXPECT_EQ(5, doc.size());
  EXPECT_EQ(2, doc[0].size());
  EXPECT_EQ("a", doc[0][0].as<std::string>());
  EXPECT_EQ("b", doc[0][1].as<std::string>());
  EXPECT_EQ(1, doc[1].size());
  EXPECT_EQ("b", doc[1]["a"].as<std::string>());
  EXPECT_EQ("a", doc[2].as<std::string>());
  EXPECT_EQ('b', doc[3].as<char>());
  EXPECT_EQ("c", doc[4].as<std::string>());
}

TEST(NodeSpecTest, Ex7_24_FlowNodes) {
  Node doc = Load(ex7_24);
  EXPECT_EQ(5, doc.size());
  EXPECT_EQ("tag:yaml.org,2002:str", doc[0].Tag());
  EXPECT_EQ("a", doc[0].as<std::string>());
  EXPECT_EQ('b', doc[1].as<char>());
  EXPECT_EQ("c", doc[2].as<std::string>());
  EXPECT_EQ("c", doc[3].as<std::string>());
  EXPECT_EQ("tag:yaml.org,2002:str", doc[4].Tag());
  EXPECT_EQ("", doc[4].as<std::string>());
}

TEST(NodeSpecTest, Ex8_1_BlockScalarHeader) {
  Node doc = Load(ex8_1);
  EXPECT_EQ(4, doc.size());
  EXPECT_EQ("literal\n", doc[0].as<std::string>());
  EXPECT_EQ(" folded\n", doc[1].as<std::string>());
  EXPECT_EQ("keep\n\n", doc[2].as<std::string>());
  EXPECT_EQ(" strip", doc[3].as<std::string>());
}

TEST(NodeSpecTest, Ex8_2_BlockIndentationHeader) {
  Node doc = Load(ex8_2);
  EXPECT_EQ(4, doc.size());
  EXPECT_EQ("detected\n", doc[0].as<std::string>());
  EXPECT_EQ("\n\n# detected\n", doc[1].as<std::string>());
  EXPECT_EQ(" explicit\n", doc[2].as<std::string>());
  EXPECT_EQ("\t\ndetected\n", doc[3].as<std::string>());
}

TEST(NodeSpecTest, Ex8_3a_InvalidBlockScalarIndentationIndicators) {
  EXPECT_THROW_PARSER_EXCEPTION(Load(ex8_3a), ErrorMsg::END_OF_SEQ);
}

TEST(NodeSpecTest, Ex8_3b_InvalidBlockScalarIndentationIndicators) {
  EXPECT_THROW_PARSER_EXCEPTION(Load(ex8_3b), ErrorMsg::END_OF_SEQ);
}

TEST(NodeSpecTest, Ex8_3c_InvalidBlockScalarIndentationIndicators) {
  EXPECT_THROW_PARSER_EXCEPTION(Load(ex8_3c), ErrorMsg::END_OF_SEQ);
}

TEST(NodeSpecTest, Ex8_4_ChompingFinalLineBreak) {
  Node doc = Load(ex8_4);
  EXPECT_EQ(3, doc.size());
  EXPECT_EQ("text", doc["strip"].as<std::string>());
  EXPECT_EQ("text\n", doc["clip"].as<std::string>());
  EXPECT_EQ("text\n", doc["keep"].as<std::string>());
}

TEST(NodeSpecTest, DISABLED_Ex8_5_ChompingTrailingLines) {
  Node doc = Load(ex8_5);
  EXPECT_EQ(3, doc.size());
  EXPECT_EQ("# text", doc["strip"].as<std::string>());
  EXPECT_EQ("# text\n", doc["clip"].as<std::string>());
  // NOTE: I believe this is a bug in the YAML spec -
  // it should be "# text\n\n"
  EXPECT_EQ("# text\n", doc["keep"].as<std::string>());
}

TEST(NodeSpecTest, Ex8_6_EmptyScalarChomping) {
  Node doc = Load(ex8_6);
  EXPECT_EQ(3, doc.size());
  EXPECT_EQ("", doc["strip"].as<std::string>());
  EXPECT_EQ("", doc["clip"].as<std::string>());
  EXPECT_EQ("\n", doc["keep"].as<std::string>());
}

TEST(NodeSpecTest, Ex8_7_LiteralScalar) {
  Node doc = Load(ex8_7);
  EXPECT_EQ("literal\n\ttext\n", doc.as<std::string>());
}

TEST(NodeSpecTest, Ex8_8_LiteralContent) {
  Node doc = Load(ex8_8);
  EXPECT_EQ("\n\nliteral\n \n\ntext\n", doc.as<std::string>());
}

TEST(NodeSpecTest, Ex8_9_FoldedScalar) {
  Node doc = Load(ex8_9);
  EXPECT_EQ("folded text\n", doc.as<std::string>());
}

TEST(NodeSpecTest, Ex8_10_FoldedLines) {
  Node doc = Load(ex8_10);
  EXPECT_TRUE(doc.as<std::string>() ==
              "\nfolded line\nnext line\n  * bullet\n\n  * list\n  * "
              "lines\n\nlast line\n");
}

TEST(NodeSpecTest, Ex8_11_MoreIndentedLines) {
  Node doc = Load(ex8_11);
  EXPECT_TRUE(doc.as<std::string>() ==
              "\nfolded line\nnext line\n  * bullet\n\n  * list\n  * "
              "lines\n\nlast line\n");
}

TEST(NodeSpecTest, Ex8_12_EmptySeparationLines) {
  Node doc = Load(ex8_12);
  EXPECT_TRUE(doc.as<std::string>() ==
              "\nfolded line\nnext line\n  * bullet\n\n  * list\n  * "
              "lines\n\nlast line\n");
}

TEST(NodeSpecTest, Ex8_13_FinalEmptyLines) {
  Node doc = Load(ex8_13);
  EXPECT_TRUE(doc.as<std::string>() ==
              "\nfolded line\nnext line\n  * bullet\n\n  * list\n  * "
              "lines\n\nlast line\n");
}

TEST(NodeSpecTest, Ex8_14_BlockSequence) {
  Node doc = Load(ex8_14);
  EXPECT_EQ(1, doc.size());
  EXPECT_EQ(2, doc["block sequence"].size());
  EXPECT_EQ("one", doc["block sequence"][0].as<std::string>());
  EXPECT_EQ(1, doc["block sequence"][1].size());
  EXPECT_EQ("three", doc["block sequence"][1]["two"].as<std::string>());
}

TEST(NodeSpecTest, Ex8_15_BlockSequenceEntryTypes) {
  Node doc = Load(ex8_15);
  EXPECT_EQ(4, doc.size());
  EXPECT_TRUE(doc[0].IsNull());
  EXPECT_EQ("block node\n", doc[1].as<std::string>());
  EXPECT_EQ(2, doc[2].size());
  EXPECT_EQ("one", doc[2][0].as<std::string>());
  EXPECT_EQ("two", doc[2][1].as<std::string>());
  EXPECT_EQ(1, doc[3].size());
  EXPECT_EQ("two", doc[3]["one"].as<std::string>());
}

TEST(NodeSpecTest, Ex8_16_BlockMappings) {
  Node doc = Load(ex8_16);
  EXPECT_EQ(1, doc.size());
  EXPECT_EQ(1, doc["block mapping"].size());
  EXPECT_EQ("value", doc["block mapping"]["key"].as<std::string>());
}

TEST(NodeSpecTest, Ex8_17_ExplicitBlockMappingEntries) {
  Node doc = Load(ex8_17);
  EXPECT_EQ(2, doc.size());
  EXPECT_TRUE(doc["explicit key"].IsNull());
  EXPECT_EQ(2, doc["block key\n"].size());
  EXPECT_EQ("one", doc["block key\n"][0].as<std::string>());
  EXPECT_EQ("two", doc["block key\n"][1].as<std::string>());
}

TEST(NodeSpecTest, Ex8_18_ImplicitBlockMappingEntries) {
  Node doc = Load(ex8_18);
  EXPECT_EQ(3, doc.size());
  EXPECT_EQ("in-line value", doc["plain key"].as<std::string>());
  EXPECT_TRUE(doc[Null].IsNull());
  EXPECT_EQ(1, doc["quoted key"].size());
  EXPECT_EQ("entry", doc["quoted key"][0].as<std::string>());
}

TEST(NodeSpecTest, Ex8_19_CompactBlockMappings) {
  Node doc = Load(ex8_19);
  EXPECT_EQ(2, doc.size());
  EXPECT_EQ(1, doc[0].size());
  EXPECT_EQ("yellow", doc[0]["sun"].as<std::string>());
  EXPECT_EQ(1, doc[1].size());
  std::map<std::string, std::string> key;
  key["earth"] = "blue";
  EXPECT_EQ(1, doc[1][key].size());
  EXPECT_EQ("white", doc[1][key]["moon"].as<std::string>());
}

TEST(NodeSpecTest, Ex8_20_BlockNodeTypes) {
  Node doc = Load(ex8_20);
  EXPECT_EQ(3, doc.size());
  EXPECT_EQ("flow in block", doc[0].as<std::string>());
  EXPECT_EQ("Block scalar\n", doc[1].as<std::string>());
  EXPECT_EQ(1, doc[2].size());
  EXPECT_EQ("bar", doc[2]["foo"].as<std::string>());
}

TEST(NodeSpecTest, DISABLED_Ex8_21_BlockScalarNodes) {
  Node doc = Load(ex8_21);
  EXPECT_EQ(2, doc.size());
  // NOTE: I believe this is a bug in the YAML spec -
  // it should be "value\n"
  EXPECT_EQ("value", doc["literal"].as<std::string>());
  EXPECT_EQ("value", doc["folded"].as<std::string>());
  EXPECT_EQ("!foo", doc["folded"].Tag());
}

TEST(NodeSpecTest, Ex8_22_BlockCollectionNodes) {
  Node doc = Load(ex8_22);
  EXPECT_EQ(2, doc.size());
  EXPECT_EQ(2, doc["sequence"].size());
  EXPECT_EQ("entry", doc["sequence"][0].as<std::string>());
  EXPECT_EQ(1, doc["sequence"][1].size());
  EXPECT_EQ("nested", doc["sequence"][1][0].as<std::string>());
  EXPECT_EQ(1, doc["mapping"].size());
  EXPECT_EQ("bar", doc["mapping"]["foo"].as<std::string>());
}
}
}
