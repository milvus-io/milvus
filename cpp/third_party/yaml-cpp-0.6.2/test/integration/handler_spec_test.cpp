#include "handler_test.h"
#include "specexamples.h"   // IWYU pragma: keep
#include "yaml-cpp/yaml.h"  // IWYU pragma: keep

#include "gmock/gmock.h"
#include "gtest/gtest.h"

using ::testing::_;

#define EXPECT_THROW_PARSER_EXCEPTION(statement, message) \
  ASSERT_THROW(statement, ParserException);               \
  try {                                                   \
    statement;                                            \
  } catch (const ParserException& e) {                    \
    EXPECT_EQ(e.msg, message);                            \
  }

namespace YAML {
namespace {

typedef HandlerTest HandlerSpecTest;

TEST_F(HandlerSpecTest, Ex2_1_SeqScalars) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Mark McGwire"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Sammy Sosa"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Ken Griffey"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex2_1);
}

TEST_F(HandlerSpecTest, Ex2_2_MappingScalarsToScalars) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "hr"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "65"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "avg"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "0.278"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "rbi"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "147"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex2_2);
}

TEST_F(HandlerSpecTest, Ex2_3_MappingScalarsToSequences) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "american"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Boston Red Sox"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Detroit Tigers"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "New York Yankees"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "national"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "New York Mets"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Chicago Cubs"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Atlanta Braves"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex2_3);
}

TEST_F(HandlerSpecTest, Ex2_4_SequenceOfMappings) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "name"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Mark McGwire"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "hr"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "65"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "avg"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "0.278"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "name"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Sammy Sosa"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "hr"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "63"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "avg"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "0.288"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex2_4);
}

TEST_F(HandlerSpecTest, Ex2_5_SequenceOfSequences) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "name"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "hr"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "avg"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Mark McGwire"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "65"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "0.278"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Sammy Sosa"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "63"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "0.288"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex2_5);
}

TEST_F(HandlerSpecTest, Ex2_6_MappingOfMappings) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Mark McGwire"));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "hr"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "65"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "avg"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "0.278"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Sammy Sosa"));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "hr"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "63"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "avg"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "0.288"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex2_6);
}

TEST_F(HandlerSpecTest, Ex2_7_TwoDocumentsInAStream) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Mark McGwire"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Sammy Sosa"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Ken Griffey"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Chicago Cubs"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "St Louis Cardinals"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex2_7);
}

TEST_F(HandlerSpecTest, Ex2_8_PlayByPlayFeed) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "time"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "20:03:20"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "player"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Sammy Sosa"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "action"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "strike (miss)"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "time"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "20:03:47"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "player"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Sammy Sosa"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "action"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "grand slam"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex2_8);
}

TEST_F(HandlerSpecTest, Ex2_9_SingleDocumentWithTwoComments) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "hr"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Mark McGwire"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Sammy Sosa"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "rbi"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Sammy Sosa"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Ken Griffey"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex2_9);
}

TEST_F(HandlerSpecTest, Ex2_10_SimpleAnchor) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "hr"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Mark McGwire"));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "Sammy Sosa"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "rbi"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnAlias(_, 1));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Ken Griffey"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex2_10);
}

TEST_F(HandlerSpecTest, Ex2_11_MappingBetweenSequences) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Detroit Tigers"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Chicago cubs"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "2001-07-23"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "New York Yankees"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Atlanta Braves"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "2001-07-02"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "2001-08-12"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "2001-08-14"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex2_11);
}

TEST_F(HandlerSpecTest, Ex2_12_CompactNestedMapping) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "item"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Super Hoop"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "quantity"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "1"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "item"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Basketball"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "quantity"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "4"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "item"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Big Shoes"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "quantity"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "1"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex2_12);
}

TEST_F(HandlerSpecTest, Ex2_13_InLiteralsNewlinesArePreserved) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "!", 0,
                                "\\//||\\/||\n"
                                "// ||  ||__"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex2_13);
}

TEST_F(HandlerSpecTest, Ex2_14_InFoldedScalarsNewlinesBecomeSpaces) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler,
              OnScalar(_, "!", 0,
                       "Mark McGwire's year was crippled by a knee injury."));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex2_14);
}

TEST_F(HandlerSpecTest,
       Ex2_15_FoldedNewlinesArePreservedForMoreIndentedAndBlankLines) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(
      handler,
      OnScalar(_, "!", 0,
               "Sammy Sosa completed another fine season with great stats.\n"
               "\n"
               "  63 Home Runs\n"
               "  0.288 Batting Average\n"
               "\n"
               "What a year!"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex2_15);
}

TEST_F(HandlerSpecTest, Ex2_16_IndentationDeterminesScope) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "name"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Mark McGwire"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "accomplishment"));
  EXPECT_CALL(handler,
              OnScalar(_, "!", 0,
                       "Mark set a major league home run record in 1998.\n"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "stats"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0,
                                "65 Home Runs\n"
                                "0.278 Batting Average\n"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex2_16);
}

TEST_F(HandlerSpecTest, Ex2_17_QuotedScalars) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "unicode"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "Sosa did fine.\xE2\x98\xBA"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "control"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "\b1998\t1999\t2000\n"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "hex esc"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "\x0d\x0a is \r\n"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "single"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "\"Howdy!\" he cried."));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "quoted"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, " # Not a 'comment'."));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "tie-fighter"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "|\\-*-/|"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex2_17);
}

TEST_F(HandlerSpecTest, Ex2_18_MultiLineFlowScalars) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "plain"));
  EXPECT_CALL(handler,
              OnScalar(_, "?", 0, "This unquoted scalar spans many lines."));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "quoted"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "So does this quoted scalar.\n"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex2_18);
}

// TODO: 2.19 - 2.22 schema tags

TEST_F(HandlerSpecTest, Ex2_23_VariousExplicitTags) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "not-date"));
  EXPECT_CALL(handler, OnScalar(_, "tag:yaml.org,2002:str", 0, "2002-04-28"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "picture"));
  EXPECT_CALL(handler, OnScalar(_, "tag:yaml.org,2002:binary", 0,
                                "R0lGODlhDAAMAIQAAP//9/X\n"
                                "17unp5WZmZgAAAOfn515eXv\n"
                                "Pz7Y6OjuDg4J+fn5OTk6enp\n"
                                "56enmleECcgggoBADs=\n"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "application specific tag"));
  EXPECT_CALL(handler, OnScalar(_, "!something", 0,
                                "The semantics of the tag\n"
                                "above may be different for\n"
                                "different documents."));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex2_23);
}

TEST_F(HandlerSpecTest, Ex2_24_GlobalTags) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "tag:clarkevans.com,2002:shape", 0,
                                       EmitterStyle::Block));
  EXPECT_CALL(handler, OnMapStart(_, "tag:clarkevans.com,2002:circle", 0,
                                  EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "center"));
  EXPECT_CALL(handler, OnMapStart(_, "?", 1, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "x"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "73"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "y"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "129"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "radius"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "7"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "tag:clarkevans.com,2002:line", 0,
                                  EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "start"));
  EXPECT_CALL(handler, OnAlias(_, 1));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "finish"));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "x"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "89"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "y"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "102"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "tag:clarkevans.com,2002:label", 0,
                                  EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "start"));
  EXPECT_CALL(handler, OnAlias(_, 1));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "color"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "0xFFEEBB"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "text"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Pretty vector drawing."));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex2_24);
}

TEST_F(HandlerSpecTest, Ex2_25_UnorderedSets) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler,
              OnMapStart(_, "tag:yaml.org,2002:set", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Mark McGwire"));
  EXPECT_CALL(handler, OnNull(_, 0));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Sammy Sosa"));
  EXPECT_CALL(handler, OnNull(_, 0));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Ken Griffey"));
  EXPECT_CALL(handler, OnNull(_, 0));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex2_25);
}

TEST_F(HandlerSpecTest, Ex2_26_OrderedMappings) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "tag:yaml.org,2002:omap", 0,
                                       EmitterStyle::Block));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Mark McGwire"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "65"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Sammy Sosa"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "63"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Ken Griffey"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "58"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex2_26);
}

TEST_F(HandlerSpecTest, Ex2_27_Invoice) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "tag:clarkevans.com,2002:invoice", 0,
                                  EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "invoice"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "34843"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "date"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "2001-01-23"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bill-to"));
  EXPECT_CALL(handler, OnMapStart(_, "?", 1, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "given"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Chris"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "family"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Dumars"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "address"));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "lines"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0,
                                "458 Walkman Dr.\n"
                                "Suite #292\n"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "city"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Royal Oak"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "state"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "MI"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "postal"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "48046"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "ship-to"));
  EXPECT_CALL(handler, OnAlias(_, 1));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "product"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "sku"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "BL394D"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "quantity"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "4"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "description"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Basketball"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "price"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "450.00"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "sku"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "BL4438H"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "quantity"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "1"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "description"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Super Hoop"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "price"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "2392.00"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "tax"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "251.42"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "total"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "4443.52"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "comments"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0,
                                "Late afternoon is best. Backup contact is "
                                "Nancy Billsmer @ 338-4338."));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex2_27);
}

TEST_F(HandlerSpecTest, Ex2_28_LogFile) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Time"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "2001-11-23 15:01:42 -5"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "User"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "ed"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Warning"));
  EXPECT_CALL(handler,
              OnScalar(_, "?", 0, "This is an error message for the log file"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Time"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "2001-11-23 15:02:31 -5"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "User"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "ed"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Warning"));
  EXPECT_CALL(handler,
              OnScalar(_, "?", 0, "A slightly different error message."));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Date"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "2001-11-23 15:03:17 -5"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "User"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "ed"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Fatal"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Unknown variable \"bar\""));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Stack"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "file"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "TopClass.py"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "line"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "23"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "code"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "x = MoreObject(\"345\\n\")\n"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "file"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "MoreClass.py"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "line"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "58"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "code"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "foo = bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex2_28);
}

// TODO: 5.1 - 5.2 BOM

TEST_F(HandlerSpecTest, Ex5_3_BlockStructureIndicators) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "sequence"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "one"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "two"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "mapping"));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "sky"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "blue"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "sea"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "green"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex5_3);
}

TEST_F(HandlerSpecTest, Ex5_4_FlowStructureIndicators) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "sequence"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "one"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "two"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "mapping"));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "sky"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "blue"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "sea"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "green"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex5_4);
}

TEST_F(HandlerSpecTest, Ex5_5_CommentIndicator) { Parse(ex5_5); }

TEST_F(HandlerSpecTest, Ex5_6_NodePropertyIndicators) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "anchored"));
  EXPECT_CALL(handler, OnScalar(_, "!local", 1, "value"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "alias"));
  EXPECT_CALL(handler, OnAlias(_, 1));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex5_6);
}

TEST_F(HandlerSpecTest, Ex5_7_BlockScalarIndicators) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "literal"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "some\ntext\n"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "folded"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "some text\n"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex5_7);
}

TEST_F(HandlerSpecTest, Ex5_8_QuotedScalarIndicators) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "single"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "text"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "double"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "text"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex5_8);
}

// TODO: 5.9 directive
// TODO: 5.10 reserved indicator

TEST_F(HandlerSpecTest, Ex5_11_LineBreakCharacters) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "!", 0,
                                "Line break (no glyph)\n"
                                "Line break (glyphed)\n"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex5_11);
}

TEST_F(HandlerSpecTest, Ex5_12_TabsAndSpaces) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "quoted"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "Quoted\t"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "block"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0,
                                "void main() {\n"
                                "\tprintf(\"Hello, world!\\n\");\n"
                                "}"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex5_12);
}

TEST_F(HandlerSpecTest, Ex5_13_EscapedCharacters) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(
      handler,
      OnScalar(_, "!", 0,
               "Fun with \x5C \x22 \x07 \x08 \x1B \x0C \x0A \x0D \x09 \x0B " +
                   std::string("\x00", 1) +
                   " \x20 \xA0 \x85 \xe2\x80\xa8 \xe2\x80\xa9 A A A"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex5_13);
}

TEST_F(HandlerSpecTest, Ex5_14_InvalidEscapedCharacters) {
  EXPECT_THROW_PARSER_EXCEPTION(IgnoreParse(ex5_14),
                                std::string(ErrorMsg::INVALID_ESCAPE) + "c");
}

TEST_F(HandlerSpecTest, Ex6_1_IndentationSpaces) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Not indented"));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "By one space"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "By four\n  spaces\n"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Flow style"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "By two"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Also by two"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Still by two"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex6_1);
}

TEST_F(HandlerSpecTest, Ex6_2_IndentationIndicators) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "a"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "b"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "c"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "d"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex6_2);
}

TEST_F(HandlerSpecTest, Ex6_3_SeparationSpaces) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "baz"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "baz"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex6_3);
}

TEST_F(HandlerSpecTest, Ex6_4_LinePrefixes) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "plain"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "text lines"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "quoted"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "text lines"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "block"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "text\n \tlines\n"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex6_4);
}

TEST_F(HandlerSpecTest, Ex6_5_EmptyLines) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Folding"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "Empty line\nas a line feed"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Chomping"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "Clipped empty lines\n"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex6_5);
}

TEST_F(HandlerSpecTest, Ex6_6_LineFolding) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "trimmed\n\n\nas space"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex6_6);
}

TEST_F(HandlerSpecTest, Ex6_7_BlockFolding) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "foo \n\n\t bar\n\nbaz\n"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex6_7);
}

TEST_F(HandlerSpecTest, Ex6_8_FlowFolding) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, " foo\nbar\nbaz "));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex6_8);
}

TEST_F(HandlerSpecTest, Ex6_9_SeparatedComment) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "key"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "value"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex6_9);
}

TEST_F(HandlerSpecTest, Ex6_10_CommentLines) { Parse(ex6_10); }

TEST_F(HandlerSpecTest, _MultiLineComments) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "key"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "value"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex6_11);
}

TEST_F(HandlerSpecTest, Ex6_12_SeparationSpacesII) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "first"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Sammy"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "last"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Sosa"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "hr"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "65"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "avg"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "0.278"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex6_12);
}

TEST_F(HandlerSpecTest, Ex6_13_ReservedDirectives) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex6_13);
}

TEST_F(HandlerSpecTest, Ex6_14_YAMLDirective) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex6_14);
}

TEST_F(HandlerSpecTest, Ex6_15_InvalidRepeatedYAMLDirective) {
  EXPECT_THROW_PARSER_EXCEPTION(IgnoreParse(ex6_15),
                                ErrorMsg::REPEATED_YAML_DIRECTIVE);
}

TEST_F(HandlerSpecTest, Ex6_16_TagDirective) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "tag:yaml.org,2002:str", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex6_16);
}

TEST_F(HandlerSpecTest, Ex6_17_InvalidRepeatedTagDirective) {
  EXPECT_THROW_PARSER_EXCEPTION(IgnoreParse(ex6_17),
                                ErrorMsg::REPEATED_TAG_DIRECTIVE);
}

TEST_F(HandlerSpecTest, Ex6_18_PrimaryTagHandle) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "!foo", 0, "bar"));
  EXPECT_CALL(handler, OnDocumentEnd());
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "tag:example.com,2000:app/foo", 0, "bar"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex6_18);
}

TEST_F(HandlerSpecTest, Ex6_19_SecondaryTagHandle) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "tag:example.com,2000:app/int", 0, "1 - 3"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex6_19);
}

TEST_F(HandlerSpecTest, Ex6_20_TagHandles) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "tag:example.com,2000:app/foo", 0, "bar"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex6_20);
}

TEST_F(HandlerSpecTest, Ex6_21_LocalTagPrefix) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "!my-light", 0, "fluorescent"));
  EXPECT_CALL(handler, OnDocumentEnd());
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "!my-light", 0, "green"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex6_21);
}

TEST_F(HandlerSpecTest, Ex6_22_GlobalTagPrefix) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "tag:example.com,2000:app/foo", 0, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex6_22);
}

TEST_F(HandlerSpecTest, Ex6_23_NodeProperties) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "tag:yaml.org,2002:str", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag:yaml.org,2002:str", 0, "bar"));
  EXPECT_CALL(handler, OnScalar(_, "?", 2, "baz"));
  EXPECT_CALL(handler, OnAlias(_, 1));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex6_23);
}

TEST_F(HandlerSpecTest, Ex6_24_VerbatimTags) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "tag:yaml.org,2002:str", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "!bar", 0, "baz"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex6_24);
}

// TODO: Implement
TEST_F(HandlerSpecTest, DISABLED_Ex6_25_InvalidVerbatimTags) {
  Parse(ex6_25);
  FAIL() << "not implemented yet";
}

TEST_F(HandlerSpecTest, Ex6_26_TagShorthands) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "!local", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag:yaml.org,2002:str", 0, "bar"));
  EXPECT_CALL(handler,
              OnScalar(_, "tag:example.com,2000:app/tag%21", 0, "baz"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex6_26);
}

TEST_F(HandlerSpecTest, Ex6_27a_InvalidTagShorthands) {
  EXPECT_THROW_PARSER_EXCEPTION(IgnoreParse(ex6_27a),
                                ErrorMsg::TAG_WITH_NO_SUFFIX);
}

// TODO: should we reject this one (since !h! is not declared)?
TEST_F(HandlerSpecTest, DISABLED_Ex6_27b_InvalidTagShorthands) {
  Parse(ex6_27b);
  FAIL() << "not implemented yet";
}

TEST_F(HandlerSpecTest, Ex6_28_NonSpecificTags) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "12"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "12"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "12"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex6_28);
}

TEST_F(HandlerSpecTest, Ex6_29_NodeAnchors) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "First occurrence"));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "Value"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Second occurrence"));
  EXPECT_CALL(handler, OnAlias(_, 1));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex6_29);
}

TEST_F(HandlerSpecTest, Ex7_1_AliasNodes) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "First occurrence"));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "Foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Second occurrence"));
  EXPECT_CALL(handler, OnAlias(_, 1));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Override anchor"));
  EXPECT_CALL(handler, OnScalar(_, "?", 2, "Bar"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Reuse anchor"));
  EXPECT_CALL(handler, OnAlias(_, 2));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex7_1);
}

TEST_F(HandlerSpecTest, Ex7_2_EmptyNodes) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag:yaml.org,2002:str", 0, ""));
  EXPECT_CALL(handler, OnScalar(_, "tag:yaml.org,2002:str", 0, ""));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex7_2);
}

TEST_F(HandlerSpecTest, Ex7_3_CompletelyEmptyNodes) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnNull(_, 0));
  EXPECT_CALL(handler, OnNull(_, 0));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex7_3);
}

TEST_F(HandlerSpecTest, Ex7_4_DoubleQuotedImplicitKeys) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "implicit block key"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "implicit flow key"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "value"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex7_4);
}

TEST_F(HandlerSpecTest, Ex7_5_DoubleQuotedLineBreaks) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(
      handler,
      OnScalar(_, "!", 0,
               "folded to a space,\nto a line feed, or \t \tnon-content"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex7_5);
}

TEST_F(HandlerSpecTest, Ex7_6_DoubleQuotedLines) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(
      handler,
      OnScalar(_, "!", 0, " 1st non-empty\n2nd non-empty 3rd non-empty "));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex7_6);
}

TEST_F(HandlerSpecTest, Ex7_7_SingleQuotedCharacters) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "here's to \"quotes\""));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex7_7);
}

TEST_F(HandlerSpecTest, Ex7_8_SingleQuotedImplicitKeys) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "implicit block key"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "implicit flow key"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "value"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex7_8);
}

TEST_F(HandlerSpecTest, Ex7_9_SingleQuotedLines) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(
      handler,
      OnScalar(_, "!", 0, " 1st non-empty\n2nd non-empty 3rd non-empty "));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex7_9);
}

TEST_F(HandlerSpecTest, Ex7_10_PlainCharacters) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "::vector"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, ": - ()"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "Up, up, and away!"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "-123"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "http://example.com/foo#bar"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "::vector"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, ": - ()"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "Up, up, and away!"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "-123"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "http://example.com/foo#bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex7_10);
}

TEST_F(HandlerSpecTest, Ex7_11_PlainImplicitKeys) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "implicit block key"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "implicit flow key"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "value"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex7_11);
}

TEST_F(HandlerSpecTest, Ex7_12_PlainLines) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 0,
                                "1st non-empty\n2nd non-empty 3rd non-empty"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex7_12);
}

TEST_F(HandlerSpecTest, Ex7_13_FlowSequence) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "one"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "two"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "three"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "four"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex7_13);
}

TEST_F(HandlerSpecTest, Ex7_14_FlowSequenceEntries) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "double quoted"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "single quoted"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "plain text"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "nested"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "single"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "pair"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex7_14);
}

TEST_F(HandlerSpecTest, Ex7_15_FlowMappings) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "one"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "two"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "three"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "four"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "five"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "six"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "seven"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "eight"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex7_15);
}

TEST_F(HandlerSpecTest, Ex7_16_FlowMappingEntries) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "explicit"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "entry"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "implicit"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "entry"));
  EXPECT_CALL(handler, OnNull(_, 0));
  EXPECT_CALL(handler, OnNull(_, 0));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex7_16);
}

TEST_F(HandlerSpecTest, Ex7_17_FlowMappingSeparateValues) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "unquoted"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "separate"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "http://foo.com"));
  EXPECT_CALL(handler, OnNull(_, 0));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "omitted value"));
  EXPECT_CALL(handler, OnNull(_, 0));
  EXPECT_CALL(handler, OnNull(_, 0));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "omitted key"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex7_17);
}

TEST_F(HandlerSpecTest, Ex7_18_FlowMappingAdjacentValues) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "adjacent"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "value"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "readable"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "value"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "empty"));
  EXPECT_CALL(handler, OnNull(_, 0));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex7_18);
}

TEST_F(HandlerSpecTest, Ex7_19_SinglePairFlowMappings) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex7_19);
}

TEST_F(HandlerSpecTest, Ex7_20_SinglePairExplicitEntry) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo bar"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "baz"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex7_20);
}

TEST_F(HandlerSpecTest, Ex7_21_SinglePairImplicitEntries) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "YAML"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "separate"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Default));
  EXPECT_CALL(handler, OnNull(_, 0));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "empty key entry"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "JSON"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "like"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "adjacent"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex7_21);
}

TEST_F(HandlerSpecTest, Ex7_22_InvalidImplicitKeys) {
  EXPECT_THROW_PARSER_EXCEPTION(IgnoreParse(ex7_22), ErrorMsg::END_OF_SEQ_FLOW);
}

TEST_F(HandlerSpecTest, Ex7_23_FlowContent) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "a"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "b"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Flow));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "a"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "b"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "a"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "b"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "c"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex7_23);
}

TEST_F(HandlerSpecTest, Ex7_24_FlowNodes) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "tag:yaml.org,2002:str", 0, "a"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "b"));
  EXPECT_CALL(handler, OnScalar(_, "!", 1, "c"));
  EXPECT_CALL(handler, OnAlias(_, 1));
  EXPECT_CALL(handler, OnScalar(_, "tag:yaml.org,2002:str", 0, ""));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex7_24);
}

TEST_F(HandlerSpecTest, Ex8_1_BlockScalarHeader) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "literal\n"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, " folded\n"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "keep\n\n"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, " strip"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex8_1);
}

TEST_F(HandlerSpecTest, Ex8_2_BlockIndentationHeader) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "detected\n"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "\n\n# detected\n"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, " explicit\n"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "\t\ndetected\n"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex8_2);
}

TEST_F(HandlerSpecTest, Ex8_3a_InvalidBlockScalarIndentationIndicators) {
  EXPECT_THROW_PARSER_EXCEPTION(IgnoreParse(ex8_3a), ErrorMsg::END_OF_SEQ);
}

TEST_F(HandlerSpecTest, Ex8_3b_InvalidBlockScalarIndentationIndicators) {
  EXPECT_THROW_PARSER_EXCEPTION(IgnoreParse(ex8_3b), ErrorMsg::END_OF_SEQ);
}

TEST_F(HandlerSpecTest, Ex8_3c_InvalidBlockScalarIndentationIndicators) {
  EXPECT_THROW_PARSER_EXCEPTION(IgnoreParse(ex8_3c), ErrorMsg::END_OF_SEQ);
}

TEST_F(HandlerSpecTest, Ex8_4_ChompingFinalLineBreak) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "strip"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "text"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "clip"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "text\n"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "keep"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "text\n"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex8_4);
}

TEST_F(HandlerSpecTest, DISABLED_Ex8_5_ChompingTrailingLines) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "strip"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "# text"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "clip"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "# text\n"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "keep"));
  // NOTE: I believe this is a bug in the YAML spec -
  // it should be "# text\n\n"
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "# text\n"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex8_5);
}

TEST_F(HandlerSpecTest, Ex8_6_EmptyScalarChomping) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "strip"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, ""));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "clip"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, ""));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "keep"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "\n"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex8_6);
}

TEST_F(HandlerSpecTest, Ex8_7_LiteralScalar) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "literal\n\ttext\n"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex8_7);
}

TEST_F(HandlerSpecTest, Ex8_8_LiteralContent) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "\n\nliteral\n \n\ntext\n"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex8_8);
}

TEST_F(HandlerSpecTest, Ex8_9_FoldedScalar) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "folded text\n"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex8_9);
}

TEST_F(HandlerSpecTest, Ex8_10_FoldedLines) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler,
              OnScalar(_, "!", 0,
                       "\nfolded line\nnext line\n  * bullet\n\n  * list\n  * "
                       "lines\n\nlast line\n"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex8_10);
}

TEST_F(HandlerSpecTest, Ex8_11_MoreIndentedLines) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler,
              OnScalar(_, "!", 0,
                       "\nfolded line\nnext line\n  * bullet\n\n  * list\n  * "
                       "lines\n\nlast line\n"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex8_11);
}

TEST_F(HandlerSpecTest, Ex8_12_EmptySeparationLines) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler,
              OnScalar(_, "!", 0,
                       "\nfolded line\nnext line\n  * bullet\n\n  * list\n  * "
                       "lines\n\nlast line\n"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex8_12);
}

TEST_F(HandlerSpecTest, Ex8_13_FinalEmptyLines) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler,
              OnScalar(_, "!", 0,
                       "\nfolded line\nnext line\n  * bullet\n\n  * list\n  * "
                       "lines\n\nlast line\n"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex8_13);
}

TEST_F(HandlerSpecTest, Ex8_14_BlockSequence) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "block sequence"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "one"));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "two"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "three"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex8_14);
}

TEST_F(HandlerSpecTest, Ex8_15_BlockSequenceEntryTypes) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnNull(_, 0));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "block node\n"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "one"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "two"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "one"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "two"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex8_15);
}

TEST_F(HandlerSpecTest, Ex8_16_BlockMappings) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "block mapping"));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "key"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "value"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex8_16);
}

TEST_F(HandlerSpecTest, Ex8_17_ExplicitBlockMappingEntries) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "explicit key"));
  EXPECT_CALL(handler, OnNull(_, 0));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "block key\n"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "one"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "two"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex8_17);
}

TEST_F(HandlerSpecTest, Ex8_18_ImplicitBlockMappingEntries) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "plain key"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "in-line value"));
  EXPECT_CALL(handler, OnNull(_, 0));
  EXPECT_CALL(handler, OnNull(_, 0));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "quoted key"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "entry"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex8_18);
}

TEST_F(HandlerSpecTest, Ex8_19_CompactBlockMappings) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "sun"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "yellow"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "earth"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "blue"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "moon"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "white"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex8_19);
}

TEST_F(HandlerSpecTest, Ex8_20_BlockNodeTypes) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "flow in block"));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "Block scalar\n"));
  EXPECT_CALL(handler,
              OnMapStart(_, "tag:yaml.org,2002:map", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex8_20);
}

TEST_F(HandlerSpecTest, DISABLED_Ex8_21_BlockScalarNodes) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "literal"));
  // NOTE: I believe this is a bug in the YAML spec
  // - it should be "value\n"
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "value"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "folded"));
  EXPECT_CALL(handler, OnScalar(_, "!foo", 0, "value"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex8_21);
}

TEST_F(HandlerSpecTest, Ex8_22_BlockCollectionNodes) {
  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "sequence"));
  EXPECT_CALL(handler, OnSequenceStart(_, "tag:yaml.org,2002:seq", 0,
                                       EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "entry"));
  EXPECT_CALL(handler, OnSequenceStart(_, "tag:yaml.org,2002:seq", 0,
                                       EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "nested"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "mapping"));
  EXPECT_CALL(handler,
              OnMapStart(_, "tag:yaml.org,2002:map", 0, EmitterStyle::Block));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(ex8_22);
}
}
}
