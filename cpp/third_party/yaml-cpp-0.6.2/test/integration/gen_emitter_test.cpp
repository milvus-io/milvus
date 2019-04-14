#include "handler_test.h"
#include "yaml-cpp/yaml.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

using ::testing::_;

namespace YAML {
namespace {

typedef HandlerTest GenEmitterTest;

TEST_F(GenEmitterTest, testbf4e63edf2258c91fb88) {
  Emitter out;
  out << Comment("comment");
  out << BeginDoc;
  out << "foo";
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test8c2aa26989357a4c8d2d) {
  Emitter out;
  out << BeginDoc;
  out << Comment("comment");
  out << "foo";
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testf8818f97591e2c51179c) {
  Emitter out;
  out << BeginDoc;
  out << "foo";
  out << Comment("comment");
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test2b9d697f1ec84bdc484f) {
  Emitter out;
  out << BeginDoc;
  out << "foo";
  out << EndDoc;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test969d8cf1535db02242b4) {
  Emitter out;
  out << Comment("comment");
  out << BeginDoc;
  out << "foo";

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test4d16d2c638f0b1131d42) {
  Emitter out;
  out << BeginDoc;
  out << Comment("comment");
  out << "foo";

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test3bdad9a4ffa67cc4201b) {
  Emitter out;
  out << BeginDoc;
  out << "foo";
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testa57103d877a04b0da3c9) {
  Emitter out;
  out << BeginDoc;
  out << "foo";
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testf838cbd6db90346652d6) {
  Emitter out;
  out << Comment("comment");
  out << BeginDoc;
  out << "foo\n";
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "foo\n"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, teste65456c6070d7ed9b292) {
  Emitter out;
  out << BeginDoc;
  out << Comment("comment");
  out << "foo\n";
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "foo\n"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test365273601c89ebaeec61) {
  Emitter out;
  out << BeginDoc;
  out << "foo\n";
  out << Comment("comment");
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "foo\n"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test92d67b382f78c6a58c2a) {
  Emitter out;
  out << BeginDoc;
  out << "foo\n";
  out << EndDoc;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "foo\n"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test49e0bb235c344722e0df) {
  Emitter out;
  out << Comment("comment");
  out << BeginDoc;
  out << "foo\n";

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "foo\n"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test3010c495cd1c61d1ccf2) {
  Emitter out;
  out << BeginDoc;
  out << Comment("comment");
  out << "foo\n";

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "foo\n"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test22e48c3bc91b32853688) {
  Emitter out;
  out << BeginDoc;
  out << "foo\n";
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "foo\n"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test03e42bee2a2c6ffc1dd8) {
  Emitter out;
  out << BeginDoc;
  out << "foo\n";
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "foo\n"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test9662984f64ea0b79b267) {
  Emitter out;
  out << Comment("comment");
  out << BeginDoc;
  out << VerbatimTag("tag");
  out << "foo";
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testf3867ffaec6663c515ff) {
  Emitter out;
  out << BeginDoc;
  out << Comment("comment");
  out << VerbatimTag("tag");
  out << "foo";
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testfd8783233e21636f7f12) {
  Emitter out;
  out << BeginDoc;
  out << VerbatimTag("tag");
  out << "foo";
  out << Comment("comment");
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test3fc20508ecea0f4cb165) {
  Emitter out;
  out << BeginDoc;
  out << VerbatimTag("tag");
  out << "foo";
  out << EndDoc;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, teste120c09230c813be6c30) {
  Emitter out;
  out << Comment("comment");
  out << BeginDoc;
  out << VerbatimTag("tag");
  out << "foo";

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test835d37d226cbacaa4b2d) {
  Emitter out;
  out << BeginDoc;
  out << Comment("comment");
  out << VerbatimTag("tag");
  out << "foo";

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test7a26848396e9291bf1f1) {
  Emitter out;
  out << BeginDoc;
  out << VerbatimTag("tag");
  out << "foo";
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test34a821220a5e1441f553) {
  Emitter out;
  out << BeginDoc;
  out << VerbatimTag("tag");
  out << "foo";
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test53e5179db889a79c3ea2) {
  Emitter out;
  out << Comment("comment");
  out << BeginDoc;
  out << Anchor("anchor");
  out << "foo";
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testb8450c68977e0df66c5b) {
  Emitter out;
  out << BeginDoc;
  out << Comment("comment");
  out << Anchor("anchor");
  out << "foo";
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, teste0277d1ed537e53294b4) {
  Emitter out;
  out << BeginDoc;
  out << Anchor("anchor");
  out << "foo";
  out << Comment("comment");
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testd6ebe62492bf8757ddde) {
  Emitter out;
  out << BeginDoc;
  out << Anchor("anchor");
  out << "foo";
  out << EndDoc;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test56c67a81a5989623dad7) {
  Emitter out;
  out << Comment("comment");
  out << BeginDoc;
  out << Anchor("anchor");
  out << "foo";

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testea4c45819b88c22d02b6) {
  Emitter out;
  out << BeginDoc;
  out << Comment("comment");
  out << Anchor("anchor");
  out << "foo";

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testfa05ed7573dd54074344) {
  Emitter out;
  out << BeginDoc;
  out << Anchor("anchor");
  out << "foo";
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test52431165a20aa2a085dc) {
  Emitter out;
  out << BeginDoc;
  out << Anchor("anchor");
  out << "foo";
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test2e1bf781941755fc5944) {
  Emitter out;
  out << Comment("comment");
  out << "foo";
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test5405b9f863e524bb3e81) {
  Emitter out;
  out << Comment("comment");
  out << "foo";
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test0a7d85109d068170e547) {
  Emitter out;
  out << "foo";
  out << Comment("comment");
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testba8dc6889d6983fb0f05) {
  Emitter out;
  out << "foo";
  out << EndDoc;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testd8743fc1225fef185b69) {
  Emitter out;
  out << Comment("comment");
  out << "foo";

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testc2f808fe5fb8b2970b89) {
  Emitter out;
  out << Comment("comment");
  out << "foo";

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test984d0572a31be4451efc) {
  Emitter out;
  out << "foo";
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testa3883cf6b7f84c32ba99) {
  Emitter out;
  out << "foo";
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test1fe1f2d242b3a00c5f83) {
  Emitter out;
  out << Comment("comment");
  out << "foo\n";
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "foo\n"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test80e82792ed68bb0cadbc) {
  Emitter out;
  out << Comment("comment");
  out << "foo\n";
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "foo\n"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test6756b87f08499449fd53) {
  Emitter out;
  out << "foo\n";
  out << Comment("comment");
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "foo\n"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test7d768a7e214b2e791928) {
  Emitter out;
  out << "foo\n";
  out << EndDoc;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "foo\n"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test73470e304962e94c82ee) {
  Emitter out;
  out << Comment("comment");
  out << "foo\n";

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "foo\n"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test220fcaca9f58ed63ab66) {
  Emitter out;
  out << Comment("comment");
  out << "foo\n";

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "foo\n"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test7e4c037d370d52aa4da4) {
  Emitter out;
  out << "foo\n";
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "foo\n"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test79a2ffc6c8161726f1ed) {
  Emitter out;
  out << "foo\n";
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "!", 0, "foo\n"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test2a634546fd8c4b92ad18) {
  Emitter out;
  out << Comment("comment");
  out << VerbatimTag("tag");
  out << "foo";
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test84a311c6ca4fe200eff5) {
  Emitter out;
  out << Comment("comment");
  out << VerbatimTag("tag");
  out << "foo";
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testef05b48cc1f9318b612f) {
  Emitter out;
  out << VerbatimTag("tag");
  out << "foo";
  out << Comment("comment");
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testa77250518abd6e019ab8) {
  Emitter out;
  out << VerbatimTag("tag");
  out << "foo";
  out << EndDoc;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test3e9c6f05218917c77c62) {
  Emitter out;
  out << Comment("comment");
  out << VerbatimTag("tag");
  out << "foo";

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test7392dd9d6829b8569e16) {
  Emitter out;
  out << Comment("comment");
  out << VerbatimTag("tag");
  out << "foo";

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test8b3e535afd61211d988f) {
  Emitter out;
  out << VerbatimTag("tag");
  out << "foo";
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testa88d36caa958ac21e487) {
  Emitter out;
  out << VerbatimTag("tag");
  out << "foo";
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test0afc4387fea5a0ad574d) {
  Emitter out;
  out << Comment("comment");
  out << Anchor("anchor");
  out << "foo";
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test6e02b45ba1f87d0b17fa) {
  Emitter out;
  out << Comment("comment");
  out << Anchor("anchor");
  out << "foo";
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test1ecee6697402f1ced486) {
  Emitter out;
  out << Anchor("anchor");
  out << "foo";
  out << Comment("comment");
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testf778d3e7e1fd4bc81ac8) {
  Emitter out;
  out << Anchor("anchor");
  out << "foo";
  out << EndDoc;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testce2ddd97c4f7b7cad993) {
  Emitter out;
  out << Comment("comment");
  out << Anchor("anchor");
  out << "foo";

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test9801aff946ce11347b78) {
  Emitter out;
  out << Comment("comment");
  out << Anchor("anchor");
  out << "foo";

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test02ae081b4d9719668378) {
  Emitter out;
  out << Anchor("anchor");
  out << "foo";
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test1c75e643ba55491e9d58) {
  Emitter out;
  out << Anchor("anchor");
  out << "foo";
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testa834d5e30e0fde106520) {
  Emitter out;
  out << Comment("comment");
  out << BeginDoc;
  out << BeginSeq;
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test26c5da2b48377ba6d9c3) {
  Emitter out;
  out << BeginDoc;
  out << Comment("comment");
  out << BeginSeq;
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, teste5df2b4f5b7ed31f5843) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << Comment("comment");
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test7da7e14ccc523f8ef682) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << EndSeq;
  out << Comment("comment");
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test5dc6d560b8a5defab6a6) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << EndSeq;
  out << EndDoc;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test5c2184c2ae1d09d7e486) {
  Emitter out;
  out << Comment("comment");
  out << BeginDoc;
  out << BeginSeq;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testcb7ca22e0d0c01cdfd6c) {
  Emitter out;
  out << BeginDoc;
  out << Comment("comment");
  out << BeginSeq;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test764d56ed21bef8c8ed1d) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << Comment("comment");
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testff49b69cd78b76f680f4) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testb8bbef15bc67a7e8a4f1) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testfe636c029a18a80da2bc) {
  Emitter out;
  out << Comment("comment");
  out << BeginDoc;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, teste61e3e6d8f0e748525e6) {
  Emitter out;
  out << BeginDoc;
  out << Comment("comment");
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test410c9f00cef2fbe84d3f) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << Comment("comment");
  out << "foo";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test2438e75da6ea8f014749) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << "foo";
  out << Comment("comment");
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test0eb1e1c40f31df48889a) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << Comment("comment");
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test7d9b8575590f008b6dcf) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndDoc;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test65e209ba68cc24d8b595) {
  Emitter out;
  out << Comment("comment");
  out << BeginDoc;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test507a8de1564951289f94) {
  Emitter out;
  out << BeginDoc;
  out << Comment("comment");
  out << BeginSeq;
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testfad3bc4da4b3117cbaac) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << Comment("comment");
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test56e0e2370b233fb813d0) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << "foo";
  out << Comment("comment");
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test0a4ab861864d922b0ef3) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testdfefc6ca543a9fdd2ab4) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test812eee6d9f35f45d4c74) {
  Emitter out;
  out << Comment("comment");
  out << BeginDoc;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << "foo";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test40cad31d965f6c4799b2) {
  Emitter out;
  out << BeginDoc;
  out << Comment("comment");
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << "foo";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testcd959e61fe82eb217264) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << Comment("comment");
  out << VerbatimTag("tag");
  out << "foo";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test6d9e41274914945389ad) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << "foo";
  out << Comment("comment");
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testab6b07362d96686c11d0) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << "foo";
  out << EndSeq;
  out << Comment("comment");
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testd4c0512efd628009c912) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << "foo";
  out << EndSeq;
  out << EndDoc;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test2774792a756fc53a2ca9) {
  Emitter out;
  out << Comment("comment");
  out << BeginDoc;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test9f03b22824667c3b1189) {
  Emitter out;
  out << BeginDoc;
  out << Comment("comment");
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test24dd60172cf27492a619) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << Comment("comment");
  out << VerbatimTag("tag");
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testa48e3f886fe68e9b48f5) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << "foo";
  out << Comment("comment");
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testfc1258212ed3c60e6e0c) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << "foo";
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test31c3570f387278586efc) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << "foo";
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test0d2e56437573e1a68e06) {
  Emitter out;
  out << Comment("comment");
  out << BeginDoc;
  out << BeginSeq;
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test87f2bba12c122a5ef611) {
  Emitter out;
  out << BeginDoc;
  out << Comment("comment");
  out << BeginSeq;
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testce2b83660fc3c47881d9) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << Comment("comment");
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test1192cd9aee9977c43f91) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << Anchor("anchor");
  out << "foo";
  out << Comment("comment");
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test5ec743a4a5f5d9f250e6) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;
  out << Comment("comment");
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test989ce0d2a8d41f94e563) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;
  out << EndDoc;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test16b16527ce6d2047b885) {
  Emitter out;
  out << Comment("comment");
  out << BeginDoc;
  out << BeginSeq;
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test190462c295b7c1b9be90) {
  Emitter out;
  out << BeginDoc;
  out << Comment("comment");
  out << BeginSeq;
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, teste179dd43311786d15564) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << Comment("comment");
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test448c118909ba10e1e5c2) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << Anchor("anchor");
  out << "foo";
  out << Comment("comment");
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test2aa064c8455651bbbccf) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test68e7564605ae7013b858) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test11fa4cf5f845696bf8c9) {
  Emitter out;
  out << Comment("comment");
  out << BeginDoc;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test2b2cd9f7d03b725c77f7) {
  Emitter out;
  out << BeginDoc;
  out << Comment("comment");
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test846d35c91609d010038a) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << Comment("comment");
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test0295ace15e4d18d3efff) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << Comment("comment");
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test31f01031d12ec0623b22) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;
  out << Comment("comment");
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test9dfa64a8b15cb1e411f3) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;
  out << EndDoc;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test2e707476963eb0157f56) {
  Emitter out;
  out << Comment("comment");
  out << BeginDoc;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test455d60cf99b6ecf73d46) {
  Emitter out;
  out << BeginDoc;
  out << Comment("comment");
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testa370b09ea89f5df32d03) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << Comment("comment");
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test325554fe67b486e0ac43) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << Comment("comment");
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test0e511740421ef2391918) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test48e6238da58855f75d2d) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test985ccda3f305aebbe7c1) {
  Emitter out;
  out << Comment("comment");
  out << BeginDoc;
  out << BeginSeq;
  out << "foo";
  out << "bar";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test48a885b1b5192b7d6c42) {
  Emitter out;
  out << BeginDoc;
  out << Comment("comment");
  out << BeginSeq;
  out << "foo";
  out << "bar";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test2d59de662bffd75bdd4e) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << Comment("comment");
  out << "foo";
  out << "bar";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test1876b50ad981242e1b5e) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << "foo";
  out << Comment("comment");
  out << "bar";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test3be092fd7c3394e57a02) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << "foo";
  out << "bar";
  out << Comment("comment");
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testdaaf6202df0524d94ba2) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << "foo";
  out << "bar";
  out << EndSeq;
  out << Comment("comment");
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test0f9c4973bc77d8baa80b) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << "foo";
  out << "bar";
  out << EndSeq;
  out << EndDoc;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testfb738d9af8c3f5d0d89b) {
  Emitter out;
  out << Comment("comment");
  out << BeginDoc;
  out << BeginSeq;
  out << "foo";
  out << "bar";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test7ba35bde1bf4471d19fd) {
  Emitter out;
  out << BeginDoc;
  out << Comment("comment");
  out << BeginSeq;
  out << "foo";
  out << "bar";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test2fc98e907a537e17f435) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << Comment("comment");
  out << "foo";
  out << "bar";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test1c2cbe9d3ad5bca645f1) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << "foo";
  out << Comment("comment");
  out << "bar";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test999bff6d585448f6f195) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << "foo";
  out << "bar";
  out << Comment("comment");
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test8972a583224ae4553b81) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << "foo";
  out << "bar";
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test77d2c64329579c726156) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << "foo";
  out << "bar";
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testf60e4d264223ca1d8ed0) {
  Emitter out;
  out << Comment("comment");
  out << BeginDoc;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test954d6b81ce04cb78b6a9) {
  Emitter out;
  out << BeginDoc;
  out << Comment("comment");
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test24e333326d100b0fb309) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << Comment("comment");
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test996f3742f7296a3dfc08) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << Comment("comment");
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test7c795dad3124d186fe05) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << Comment("comment");
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testc2b03c01b73703fad10f) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndSeq;
  out << Comment("comment");
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testab877782f99ac0c76a54) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndSeq;
  out << EndDoc;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test784517e5fb6049b5eeb8) {
  Emitter out;
  out << Comment("comment");
  out << BeginDoc;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testbb2a1aa3c8fb0b8c763f) {
  Emitter out;
  out << BeginDoc;
  out << Comment("comment");
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testb360035c1b0988456eb8) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << Comment("comment");
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test547ca5f2968b36666091) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << Comment("comment");
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testcfb6b5ffda618b49a876) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << Comment("comment");
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test0f42ce2ba53fb3e486f9) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test93e437d10dbd42ac83bb) {
  Emitter out;
  out << BeginDoc;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test4267b4f75e3d9582d981) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test62c1e6598c987e3d28a2) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testbcf0ccd5a35fe73714b0) {
  Emitter out;
  out << BeginSeq;
  out << Comment("comment");
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testa4c54e8a23771f781411) {
  Emitter out;
  out << BeginSeq;
  out << EndSeq;
  out << Comment("comment");
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testad0a081525db9f568450) {
  Emitter out;
  out << BeginSeq;
  out << EndSeq;
  out << EndDoc;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test1c24a291665d1cc4404c) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testcb331bf2e56388df2f1a) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test122fe9269df7f8cb80f7) {
  Emitter out;
  out << BeginSeq;
  out << Comment("comment");
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test6a09300420248adaddd4) {
  Emitter out;
  out << BeginSeq;
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testf3bd03889c1e8dceb798) {
  Emitter out;
  out << BeginSeq;
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testce882fb9271babb66dc6) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test9c237cf40d8848a18afd) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testcadbaf942d41242c21fc) {
  Emitter out;
  out << BeginSeq;
  out << Comment("comment");
  out << "foo";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testfc6e5fb862b4ce920622) {
  Emitter out;
  out << BeginSeq;
  out << "foo";
  out << Comment("comment");
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test4ecc85dc0cb57f446540) {
  Emitter out;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << Comment("comment");
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test4e844b231f7985238b21) {
  Emitter out;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndDoc;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test8ec201c2f040093428a7) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test92469ed608395adc2620) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test8a9d541f2dc9c468cbaa) {
  Emitter out;
  out << BeginSeq;
  out << Comment("comment");
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test6a9392e1353590d282e2) {
  Emitter out;
  out << BeginSeq;
  out << "foo";
  out << Comment("comment");
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test87d8b037171651caa126) {
  Emitter out;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test1b1bc972209abdacc529) {
  Emitter out;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test552483e6a5fe7eee1cc5) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << "foo";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test43d39704b3b5e188182a) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << "foo";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test7318d9b71ec1a8f05621) {
  Emitter out;
  out << BeginSeq;
  out << Comment("comment");
  out << VerbatimTag("tag");
  out << "foo";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test77302e5c536183072945) {
  Emitter out;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << "foo";
  out << Comment("comment");
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test5b740c51d614c5cedf1c) {
  Emitter out;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << "foo";
  out << EndSeq;
  out << Comment("comment");
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testb66838e46cf76ce30c95) {
  Emitter out;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << "foo";
  out << EndSeq;
  out << EndDoc;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test592cabeb320cc3d6a4a6) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testdcc6e5388f6c2058954f) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test1ebbb7521464e6cc5da7) {
  Emitter out;
  out << BeginSeq;
  out << Comment("comment");
  out << VerbatimTag("tag");
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test8ccc7ae170ad81b12996) {
  Emitter out;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << "foo";
  out << Comment("comment");
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test6421c3c3ed7a65699e0b) {
  Emitter out;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << "foo";
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test096939c5c407fed2178a) {
  Emitter out;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << "foo";
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test1e79c736bdaf36bcc331) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testa932875b266f62a32a15) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test7211a0d0a7d7b8957fbd) {
  Emitter out;
  out << BeginSeq;
  out << Comment("comment");
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testfe2ed84c5a19bea4324a) {
  Emitter out;
  out << BeginSeq;
  out << Anchor("anchor");
  out << "foo";
  out << Comment("comment");
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test05fe79e124bcadf04952) {
  Emitter out;
  out << BeginSeq;
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;
  out << Comment("comment");
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test6deaaa4620537aec93bb) {
  Emitter out;
  out << BeginSeq;
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;
  out << EndDoc;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testd8f4b7fd570238af0ac6) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test6aa495401fa882fd6ef0) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test3388236bb529c50f5341) {
  Emitter out;
  out << BeginSeq;
  out << Comment("comment");
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test79f4266262dffb3f8346) {
  Emitter out;
  out << BeginSeq;
  out << Anchor("anchor");
  out << "foo";
  out << Comment("comment");
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, teste858dd76c42bbd281706) {
  Emitter out;
  out << BeginSeq;
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test7487786b0131b1337e71) {
  Emitter out;
  out << BeginSeq;
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testa9b9110a8d2175a57e7c) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, teste99a7f40fb4d939e2da7) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test0e253895bd9cff694f9a) {
  Emitter out;
  out << BeginSeq;
  out << Comment("comment");
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testf0a8ffd3a895826b093c) {
  Emitter out;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << Comment("comment");
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testdae2c374518234f70e9f) {
  Emitter out;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;
  out << Comment("comment");
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testc0dae4cd70b3f409cbb4) {
  Emitter out;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;
  out << EndDoc;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testc26f714fb6f9b1ee6cf9) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test9644620dcb35431e2969) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test1cfceae5c8e4a64a43ae) {
  Emitter out;
  out << BeginSeq;
  out << Comment("comment");
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test64f296d213a7ddc7738c) {
  Emitter out;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << Comment("comment");
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testb645b7ae7c95adc70e6f) {
  Emitter out;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test3a6fb33f542118758a78) {
  Emitter out;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test98b9868c139b816dcc00) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << "foo";
  out << "bar";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test69526d609eb86d3b7917) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << "foo";
  out << "bar";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testb569fe86774c96603a89) {
  Emitter out;
  out << BeginSeq;
  out << Comment("comment");
  out << "foo";
  out << "bar";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test03793bbef87d15c4ec74) {
  Emitter out;
  out << BeginSeq;
  out << "foo";
  out << Comment("comment");
  out << "bar";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test508ca3314339f0dfb5c8) {
  Emitter out;
  out << BeginSeq;
  out << "foo";
  out << "bar";
  out << Comment("comment");
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test7c8a731b32213075b25a) {
  Emitter out;
  out << BeginSeq;
  out << "foo";
  out << "bar";
  out << EndSeq;
  out << Comment("comment");
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testb2a840af01cc1074e80f) {
  Emitter out;
  out << BeginSeq;
  out << "foo";
  out << "bar";
  out << EndSeq;
  out << EndDoc;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test8e372218ea9564c579b6) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << "foo";
  out << "bar";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test9135f186de91cf9e7538) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << "foo";
  out << "bar";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test81be571777fd06470d73) {
  Emitter out;
  out << BeginSeq;
  out << Comment("comment");
  out << "foo";
  out << "bar";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test7614d4928827481a2d8e) {
  Emitter out;
  out << BeginSeq;
  out << "foo";
  out << Comment("comment");
  out << "bar";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, teste21f9902a4a5e3c73628) {
  Emitter out;
  out << BeginSeq;
  out << "foo";
  out << "bar";
  out << Comment("comment");
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test879d7957916fb5526b7e) {
  Emitter out;
  out << BeginSeq;
  out << "foo";
  out << "bar";
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test8e4e2b3618c7fe67384b) {
  Emitter out;
  out << BeginSeq;
  out << "foo";
  out << "bar";
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test5e8672f96ce5a11b846a) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test26c021c876f1ef91cff4) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test04da690e5c32b7bf1dc3) {
  Emitter out;
  out << BeginSeq;
  out << Comment("comment");
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test4bf435d6bad0ca7f8016) {
  Emitter out;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << Comment("comment");
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test38a40c04ceadf66cb77a) {
  Emitter out;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << Comment("comment");
  out << EndSeq;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test73f543da75154971fa58) {
  Emitter out;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndSeq;
  out << Comment("comment");
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test9c2a6b8e53a71cc271f1) {
  Emitter out;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndSeq;
  out << EndDoc;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test76e7370aa293790ca0b6) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test530430c212524fd2d917) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testdc001467ce9060c3e34f) {
  Emitter out;
  out << BeginSeq;
  out << Comment("comment");
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, teste650915c87ea88e85b7d) {
  Emitter out;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << Comment("comment");
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testc288c80940cbb3539620) {
  Emitter out;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << Comment("comment");
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test18256634c494bbf715ac) {
  Emitter out;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testfaecbb520871c68e74c2) {
  Emitter out;
  out << BeginSeq;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testd3b1e7d0410f03b69666) {
  Emitter out;
  out << Comment("comment");
  out << BeginDoc;
  out << BeginMap;
  out << EndMap;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test2654cfdffd9be4c24917) {
  Emitter out;
  out << BeginDoc;
  out << Comment("comment");
  out << BeginMap;
  out << EndMap;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testad5fd5e6d524dd2c4907) {
  Emitter out;
  out << BeginDoc;
  out << BeginMap;
  out << Comment("comment");
  out << EndMap;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testb1b59e0e48f25bf90045) {
  Emitter out;
  out << BeginDoc;
  out << BeginMap;
  out << EndMap;
  out << Comment("comment");
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test6a1cd91064a302335507) {
  Emitter out;
  out << BeginDoc;
  out << BeginMap;
  out << EndMap;
  out << EndDoc;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test7542cfaba20c029d74d2) {
  Emitter out;
  out << Comment("comment");
  out << BeginDoc;
  out << BeginMap;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test7c64ec9fde647f95ae64) {
  Emitter out;
  out << BeginDoc;
  out << Comment("comment");
  out << BeginMap;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testbba41212c116596476f3) {
  Emitter out;
  out << BeginDoc;
  out << BeginMap;
  out << Comment("comment");
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test66a3e0c8ba0580adcce7) {
  Emitter out;
  out << BeginDoc;
  out << BeginMap;
  out << EndMap;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test201f78790b978c826f38) {
  Emitter out;
  out << BeginDoc;
  out << BeginMap;
  out << EndMap;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test85630834239ca1a21b5f) {
  Emitter out;
  out << Comment("comment");
  out << BeginDoc;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test6e4aaceae87fea59c777) {
  Emitter out;
  out << BeginDoc;
  out << Comment("comment");
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test430815076958c69dc3ae) {
  Emitter out;
  out << BeginDoc;
  out << BeginMap;
  out << Comment("comment");
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test699eb267a6012eb16178) {
  Emitter out;
  out << BeginDoc;
  out << BeginMap;
  out << "foo";
  out << Comment("comment");
  out << "bar";
  out << EndMap;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testf51a095fb67ab6d7a2c9) {
  Emitter out;
  out << BeginDoc;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << Comment("comment");
  out << EndMap;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test9c926b68cb956ee2c349) {
  Emitter out;
  out << BeginDoc;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << Comment("comment");
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testeafd3facc48783e0e3a5) {
  Emitter out;
  out << BeginDoc;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndDoc;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testd702fb9ab5607d2ac819) {
  Emitter out;
  out << Comment("comment");
  out << BeginDoc;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, teste442f82a0e6799f2610c) {
  Emitter out;
  out << BeginDoc;
  out << Comment("comment");
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test60c91197fb0bb0c6abc3) {
  Emitter out;
  out << BeginDoc;
  out << BeginMap;
  out << Comment("comment");
  out << "foo";
  out << "bar";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testb51cf53880c7efae7f00) {
  Emitter out;
  out << BeginDoc;
  out << BeginMap;
  out << "foo";
  out << Comment("comment");
  out << "bar";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test015d48eead15f53dda15) {
  Emitter out;
  out << BeginDoc;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << Comment("comment");
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testa6ef6e851b7c9bb8695a) {
  Emitter out;
  out << BeginDoc;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test77f35be9920c06becdb3) {
  Emitter out;
  out << BeginDoc;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testdabd5e4aaaf01a76ac53) {
  Emitter out;
  out << Comment("comment");
  out << BeginDoc;
  out << BeginMap;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndMap;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testfdfbe118771b6de1e966) {
  Emitter out;
  out << BeginDoc;
  out << Comment("comment");
  out << BeginMap;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndMap;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test4153b34f6f6392539a57) {
  Emitter out;
  out << BeginDoc;
  out << BeginMap;
  out << Comment("comment");
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndMap;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test73cfd1e2fec4af9bcd24) {
  Emitter out;
  out << BeginDoc;
  out << BeginMap;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << Comment("comment");
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndMap;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test9a25bda309eadfbe8323) {
  Emitter out;
  out << BeginDoc;
  out << BeginMap;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << Comment("comment");
  out << EndMap;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test7600d72d8bd5c481b7d2) {
  Emitter out;
  out << BeginDoc;
  out << BeginMap;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndMap;
  out << Comment("comment");
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, teste03fdb7c4a58b419523d) {
  Emitter out;
  out << BeginDoc;
  out << BeginMap;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndMap;
  out << EndDoc;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testcfc06e7baf582f834f85) {
  Emitter out;
  out << Comment("comment");
  out << BeginDoc;
  out << BeginMap;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test355c684d237adcf5d852) {
  Emitter out;
  out << BeginDoc;
  out << Comment("comment");
  out << BeginMap;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testbc2e12df3c174bd6b7e3) {
  Emitter out;
  out << BeginDoc;
  out << BeginMap;
  out << Comment("comment");
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test47220f053e2788618f28) {
  Emitter out;
  out << BeginDoc;
  out << BeginMap;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << Comment("comment");
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testa71dd9a65f4e9563d114) {
  Emitter out;
  out << BeginDoc;
  out << BeginMap;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << Comment("comment");
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test188c5ac5c1d6e2174110) {
  Emitter out;
  out << BeginDoc;
  out << BeginMap;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndMap;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testdc89b13ed8a4694f0296) {
  Emitter out;
  out << BeginDoc;
  out << BeginMap;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndMap;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test91fe618a569a60fa75d0) {
  Emitter out;
  out << Comment("comment");
  out << BeginMap;
  out << EndMap;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test1f18c55487cda597929f) {
  Emitter out;
  out << Comment("comment");
  out << BeginMap;
  out << EndMap;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testa74f5aa5747c1ec09ac2) {
  Emitter out;
  out << BeginMap;
  out << Comment("comment");
  out << EndMap;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testda71284d14e9f5902fe5) {
  Emitter out;
  out << BeginMap;
  out << EndMap;
  out << Comment("comment");
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testb041a1dda939d84dd6ed) {
  Emitter out;
  out << BeginMap;
  out << EndMap;
  out << EndDoc;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test0f39f734136f83edaab4) {
  Emitter out;
  out << Comment("comment");
  out << BeginMap;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, teste8eed1f3ab25f5395d7b) {
  Emitter out;
  out << Comment("comment");
  out << BeginMap;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test3c7e3bbca86317884080) {
  Emitter out;
  out << BeginMap;
  out << Comment("comment");
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testdc6b30ad8f00369e0597) {
  Emitter out;
  out << BeginMap;
  out << EndMap;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testa905ff4b380f9fbbb630) {
  Emitter out;
  out << BeginMap;
  out << EndMap;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test7143b34a608a3c7dc4bf) {
  Emitter out;
  out << Comment("comment");
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test3f56e373d59464d85912) {
  Emitter out;
  out << Comment("comment");
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test8790906cd88c45aba794) {
  Emitter out;
  out << BeginMap;
  out << Comment("comment");
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test7eef04e795fe3d520c6f) {
  Emitter out;
  out << BeginMap;
  out << "foo";
  out << Comment("comment");
  out << "bar";
  out << EndMap;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test3378c3dc3604de62c507) {
  Emitter out;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << Comment("comment");
  out << EndMap;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, teste9742c9cbd88de777d38) {
  Emitter out;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << Comment("comment");
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testb74cac7a45f30159f880) {
  Emitter out;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndDoc;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testa4475a6e0ca5a46213db) {
  Emitter out;
  out << Comment("comment");
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test7aa5535404499fd9e6fe) {
  Emitter out;
  out << Comment("comment");
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test75a57ca05218b76d7aa1) {
  Emitter out;
  out << BeginMap;
  out << Comment("comment");
  out << "foo";
  out << "bar";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test973f12b1e7cdd047a330) {
  Emitter out;
  out << BeginMap;
  out << "foo";
  out << Comment("comment");
  out << "bar";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test75086003fa734ba2e64c) {
  Emitter out;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << Comment("comment");
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testef9ab805717a642f5d50) {
  Emitter out;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testbbe1014c67cbcd9bff8f) {
  Emitter out;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testa0fae19726738031a50d) {
  Emitter out;
  out << Comment("comment");
  out << BeginMap;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndMap;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test8cf4c10b22758615f5dc) {
  Emitter out;
  out << Comment("comment");
  out << BeginMap;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndMap;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test777130969e538d6086d6) {
  Emitter out;
  out << BeginMap;
  out << Comment("comment");
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndMap;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test5ab6ba08277cd6a6a349) {
  Emitter out;
  out << BeginMap;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << Comment("comment");
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndMap;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test4e6b76c47d712e10f181) {
  Emitter out;
  out << BeginMap;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << Comment("comment");
  out << EndMap;
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test0ccc3ac76a8810b05ab3) {
  Emitter out;
  out << BeginMap;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndMap;
  out << Comment("comment");
  out << EndDoc;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test3def4b737140116db9f9) {
  Emitter out;
  out << BeginMap;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndMap;
  out << EndDoc;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test9ca096634f9d9cec9b36) {
  Emitter out;
  out << Comment("comment");
  out << BeginMap;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testbf2b7d7182dae8038726) {
  Emitter out;
  out << Comment("comment");
  out << BeginMap;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testa3528cc9ec7b90fc1d8d) {
  Emitter out;
  out << BeginMap;
  out << Comment("comment");
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testf5be0253b15048a37e3a) {
  Emitter out;
  out << BeginMap;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << Comment("comment");
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, teste338a8f7f7aa9b63cddb) {
  Emitter out;
  out << BeginMap;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << Comment("comment");
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test3628377ede539d44f975) {
  Emitter out;
  out << BeginMap;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndMap;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test6eb2c2967a88d82d83d9) {
  Emitter out;
  out << BeginMap;
  out << VerbatimTag("tag");
  out << Anchor("anchor");
  out << "foo";
  out << VerbatimTag("tag");
  out << Anchor("other");
  out << "bar";
  out << EndMap;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "tag", 1, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "tag", 2, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test03515aba5b291049c262) {
  Emitter out;
  out << Comment("comment");
  out << BeginMap;
  out << "foo";
  out << "foo";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test20eea0df7f54b919cb92) {
  Emitter out;
  out << Comment("comment");
  out << BeginMap;
  out << "foo";
  out << "foo";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test6c6ab6f66ce2828d26cd) {
  Emitter out;
  out << BeginMap;
  out << Comment("comment");
  out << "foo";
  out << "foo";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test7f415ecadae422f7b0f5) {
  Emitter out;
  out << BeginMap;
  out << "foo";
  out << Comment("comment");
  out << "foo";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test0797d220a5428528ec87) {
  Emitter out;
  out << BeginMap;
  out << "foo";
  out << "foo";
  out << Comment("comment");
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test6e29f19c88864559a43e) {
  Emitter out;
  out << BeginMap;
  out << "foo";
  out << "foo";
  out << EndMap;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test0fe24accf63e33a71494) {
  Emitter out;
  out << BeginMap;
  out << "foo";
  out << "foo";
  out << EndMap;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test6cc44b957e440b365fbc) {
  Emitter out;
  out << Comment("comment");
  out << BeginMap;
  out << "foo";
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test5de19a26e7af5459141b) {
  Emitter out;
  out << Comment("comment");
  out << BeginMap;
  out << "foo";
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testea71fb07f12c70473a81) {
  Emitter out;
  out << BeginMap;
  out << Comment("comment");
  out << "foo";
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testa70d56c89e5219a106b6) {
  Emitter out;
  out << BeginMap;
  out << "foo";
  out << Comment("comment");
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test9eac2d394b9528ccd982) {
  Emitter out;
  out << BeginMap;
  out << "foo";
  out << BeginSeq;
  out << Comment("comment");
  out << "foo";
  out << EndSeq;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test25b8b26feb1a8e5e50f9) {
  Emitter out;
  out << BeginMap;
  out << "foo";
  out << BeginSeq;
  out << "foo";
  out << Comment("comment");
  out << EndSeq;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testc9498e8d4545fb7ba842) {
  Emitter out;
  out << BeginMap;
  out << "foo";
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << Comment("comment");
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testaf3f79c8ce6b65a71427) {
  Emitter out;
  out << BeginMap;
  out << "foo";
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndMap;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test921c297e990d295cd067) {
  Emitter out;
  out << BeginMap;
  out << "foo";
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndMap;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testd61d8ef3753e51e671a8) {
  Emitter out;
  out << Comment("comment");
  out << BeginMap;
  out << "foo";
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test8fa800a208cdaa3e20d7) {
  Emitter out;
  out << Comment("comment");
  out << BeginMap;
  out << "foo";
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testdf78d6aa9976557adf35) {
  Emitter out;
  out << BeginMap;
  out << Comment("comment");
  out << "foo";
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testf13bfc9be619d52da2b9) {
  Emitter out;
  out << BeginMap;
  out << "foo";
  out << Comment("comment");
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testd69830b6ed705995e364) {
  Emitter out;
  out << BeginMap;
  out << "foo";
  out << BeginMap;
  out << Comment("comment");
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, teste41087bf41e163aa315d) {
  Emitter out;
  out << BeginMap;
  out << "foo";
  out << BeginMap;
  out << "foo";
  out << Comment("comment");
  out << "bar";
  out << EndMap;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testf46aa6778b48f4a04e21) {
  Emitter out;
  out << BeginMap;
  out << "foo";
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << Comment("comment");
  out << EndMap;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, teste017eaffcd10bfedf757) {
  Emitter out;
  out << BeginMap;
  out << "foo";
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << Comment("comment");
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test1cd2c3e073dac695dfdb) {
  Emitter out;
  out << BeginMap;
  out << "foo";
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndMap;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testd2eb393135a7a84aea37) {
  Emitter out;
  out << BeginMap;
  out << "foo";
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndMap;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test79b4dcb868412729afed) {
  Emitter out;
  out << Comment("comment");
  out << BeginMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << "foo";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test379fd004b428ddfbabe2) {
  Emitter out;
  out << Comment("comment");
  out << BeginMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << "foo";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test5c13b45235bb1b7e36a9) {
  Emitter out;
  out << BeginMap;
  out << Comment("comment");
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << "foo";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test95b9c0c89b4cd2841446) {
  Emitter out;
  out << BeginMap;
  out << BeginSeq;
  out << Comment("comment");
  out << "foo";
  out << EndSeq;
  out << "foo";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test14810a0d0234bee070a2) {
  Emitter out;
  out << BeginMap;
  out << BeginSeq;
  out << "foo";
  out << Comment("comment");
  out << EndSeq;
  out << "foo";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test8447a364c30880a42ca1) {
  Emitter out;
  out << BeginMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << Comment("comment");
  out << "foo";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test09fbd11d82a3777ac376) {
  Emitter out;
  out << BeginMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << "foo";
  out << Comment("comment");
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testcbb2d4bd62f1ef2f66ad) {
  Emitter out;
  out << BeginMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << "foo";
  out << EndMap;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testa0ecef367a1f54e08d00) {
  Emitter out;
  out << BeginMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << "foo";
  out << EndMap;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test89c1ba14891887445d1a) {
  Emitter out;
  out << Comment("comment");
  out << BeginMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test68436e821e03f93754e6) {
  Emitter out;
  out << Comment("comment");
  out << BeginMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test9d56b9e336a032014977) {
  Emitter out;
  out << BeginMap;
  out << Comment("comment");
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test38701040630c71bff30d) {
  Emitter out;
  out << BeginMap;
  out << BeginSeq;
  out << Comment("comment");
  out << "foo";
  out << EndSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testa5ee191882708767d03a) {
  Emitter out;
  out << BeginMap;
  out << BeginSeq;
  out << "foo";
  out << Comment("comment");
  out << EndSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test7f055e278180eb0f8c0d) {
  Emitter out;
  out << BeginMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << Comment("comment");
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testb17a6726041573ad5d90) {
  Emitter out;
  out << BeginMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginSeq;
  out << Comment("comment");
  out << "foo";
  out << EndSeq;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test0fe7dcb77bf033a58a7d) {
  Emitter out;
  out << BeginMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginSeq;
  out << "foo";
  out << Comment("comment");
  out << EndSeq;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test4df73e774cb1a7044859) {
  Emitter out;
  out << BeginMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << Comment("comment");
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test519ee9f4db0c2cc3cb22) {
  Emitter out;
  out << BeginMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndMap;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test86fdfe330b8b39c502e3) {
  Emitter out;
  out << BeginMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndMap;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testf1f415e2941ace087ef1) {
  Emitter out;
  out << Comment("comment");
  out << BeginMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test61947172d8a42f6daa33) {
  Emitter out;
  out << Comment("comment");
  out << BeginMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testad7ddc91f2603dcd8af4) {
  Emitter out;
  out << BeginMap;
  out << Comment("comment");
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test3ded81db555ef0e2dd4c) {
  Emitter out;
  out << BeginMap;
  out << BeginSeq;
  out << Comment("comment");
  out << "foo";
  out << EndSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testb1da8147f5442f38a0af) {
  Emitter out;
  out << BeginMap;
  out << BeginSeq;
  out << "foo";
  out << Comment("comment");
  out << EndSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testdb0f2d85399318eb7957) {
  Emitter out;
  out << BeginMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << Comment("comment");
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test456b6f61fd4fb5b4b516) {
  Emitter out;
  out << BeginMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginMap;
  out << Comment("comment");
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test016098295ec6d033a470) {
  Emitter out;
  out << BeginMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginMap;
  out << "foo";
  out << Comment("comment");
  out << "bar";
  out << EndMap;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test1cbee1bf6ca60fa72aa5) {
  Emitter out;
  out << BeginMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << Comment("comment");
  out << EndMap;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testaf9b292d92d4ec50429c) {
  Emitter out;
  out << BeginMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << Comment("comment");
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test3aded59e694359fa222e) {
  Emitter out;
  out << BeginMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndMap;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test6a79d05b97bf18ab27e9) {
  Emitter out;
  out << BeginMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndMap;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test96727e93c95d3dcb5a2b) {
  Emitter out;
  out << Comment("comment");
  out << BeginMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << "foo";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test19387cf6dcd0618b416f) {
  Emitter out;
  out << Comment("comment");
  out << BeginMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << "foo";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test755733f3c3c00263610c) {
  Emitter out;
  out << BeginMap;
  out << Comment("comment");
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << "foo";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test426a9d79d3e0421c9a57) {
  Emitter out;
  out << BeginMap;
  out << BeginMap;
  out << Comment("comment");
  out << "foo";
  out << "bar";
  out << EndMap;
  out << "foo";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testefad231fdbf0147b5a46) {
  Emitter out;
  out << BeginMap;
  out << BeginMap;
  out << "foo";
  out << Comment("comment");
  out << "bar";
  out << EndMap;
  out << "foo";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test03772c261806afb7c9cc) {
  Emitter out;
  out << BeginMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << Comment("comment");
  out << EndMap;
  out << "foo";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test65734300dcf959922c18) {
  Emitter out;
  out << BeginMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << Comment("comment");
  out << "foo";
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test815ef14ef8792dc2de1d) {
  Emitter out;
  out << BeginMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << "foo";
  out << Comment("comment");
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test5266cf3b9eb70be75944) {
  Emitter out;
  out << BeginMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << "foo";
  out << EndMap;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test199b993f2f9302c1c0c8) {
  Emitter out;
  out << BeginMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << "foo";
  out << EndMap;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test448f8a30d377c0c01b26) {
  Emitter out;
  out << Comment("comment");
  out << BeginMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testa4a3cfe5a74aa90708a5) {
  Emitter out;
  out << Comment("comment");
  out << BeginMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test6de6ac6c26a8d098b529) {
  Emitter out;
  out << BeginMap;
  out << Comment("comment");
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testc9e7f10371f284e94d20) {
  Emitter out;
  out << BeginMap;
  out << BeginMap;
  out << Comment("comment");
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test382360950d64ced8a0cc) {
  Emitter out;
  out << BeginMap;
  out << BeginMap;
  out << "foo";
  out << Comment("comment");
  out << "bar";
  out << EndMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test52b289c54900ca0a098e) {
  Emitter out;
  out << BeginMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << Comment("comment");
  out << EndMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test7266716f412852506771) {
  Emitter out;
  out << BeginMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << Comment("comment");
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testd2ac7d6f40cc75a3c695) {
  Emitter out;
  out << BeginMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginSeq;
  out << Comment("comment");
  out << "foo";
  out << EndSeq;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test368597fd8bcc0d3aaff0) {
  Emitter out;
  out << BeginMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginSeq;
  out << "foo";
  out << Comment("comment");
  out << EndSeq;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test9201309cd67ab579b883) {
  Emitter out;
  out << BeginMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << Comment("comment");
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test30e18cbdd5d04a552625) {
  Emitter out;
  out << BeginMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndMap;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testa659d36137a8f34911a6) {
  Emitter out;
  out << BeginMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndMap;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, teste0b8302185ee814d6552) {
  Emitter out;
  out << Comment("comment");
  out << BeginMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test66793e09aa910bd924ca) {
  Emitter out;
  out << Comment("comment");
  out << BeginMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test9beaa5ceb1fcfef2a897) {
  Emitter out;
  out << BeginMap;
  out << Comment("comment");
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test8ccc36c941ec6aa59536) {
  Emitter out;
  out << BeginMap;
  out << BeginMap;
  out << Comment("comment");
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test3c8ea05243d098395929) {
  Emitter out;
  out << BeginMap;
  out << BeginMap;
  out << "foo";
  out << Comment("comment");
  out << "bar";
  out << EndMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testd819abb35a3ff74e764a) {
  Emitter out;
  out << BeginMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << Comment("comment");
  out << EndMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test416c49d00378b626ad21) {
  Emitter out;
  out << BeginMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << Comment("comment");
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test5cd5ecef5b2a297afe21) {
  Emitter out;
  out << BeginMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginMap;
  out << Comment("comment");
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test9d8ea78df40a20f07e96) {
  Emitter out;
  out << BeginMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginMap;
  out << "foo";
  out << Comment("comment");
  out << "bar";
  out << EndMap;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, teste7bdb2f35bec40c207ee) {
  Emitter out;
  out << BeginMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << Comment("comment");
  out << EndMap;
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, teste6d6da3a612849d32b79) {
  Emitter out;
  out << BeginMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << Comment("comment");
  out << EndMap;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test9af66f2efca487828902) {
  Emitter out;
  out << BeginMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndMap;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testb05b2fc716f368b6bdfc) {
  Emitter out;
  out << BeginMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndMap;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testd0713381b33b72abdb58) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << "foo";
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testc6739cb3a9de4a5c41a7) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << "foo";
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test2b4fa669c6b56cd6a117) {
  Emitter out;
  out << BeginSeq;
  out << Comment("comment");
  out << "foo";
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test29ae2ef656132c069d22) {
  Emitter out;
  out << BeginSeq;
  out << "foo";
  out << Comment("comment");
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testd3e2ad21ab7d0890c412) {
  Emitter out;
  out << BeginSeq;
  out << "foo";
  out << "foo";
  out << Comment("comment");
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test632b0fb6570fa3b1b682) {
  Emitter out;
  out << BeginSeq;
  out << "foo";
  out << "foo";
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testc9bcfd496ae3382d546b) {
  Emitter out;
  out << BeginSeq;
  out << "foo";
  out << "foo";
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test43fd860d560c9137add4) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << "foo";
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test0e47183ab4a56c3a6d36) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << "foo";
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testf165d91c1dce024c9774) {
  Emitter out;
  out << BeginSeq;
  out << Comment("comment");
  out << "foo";
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test7592a1f019ddb2b9c3f4) {
  Emitter out;
  out << BeginSeq;
  out << "foo";
  out << Comment("comment");
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test758d285e89a9de8070a7) {
  Emitter out;
  out << BeginSeq;
  out << "foo";
  out << BeginSeq;
  out << Comment("comment");
  out << "foo";
  out << EndSeq;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test5888499c52285a1b61de) {
  Emitter out;
  out << BeginSeq;
  out << "foo";
  out << BeginSeq;
  out << "foo";
  out << Comment("comment");
  out << EndSeq;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test0115edbe7dd05e84f108) {
  Emitter out;
  out << BeginSeq;
  out << "foo";
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << Comment("comment");
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testcbbbde92f35597a9e476) {
  Emitter out;
  out << BeginSeq;
  out << "foo";
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testa74ec451d449e2a89442) {
  Emitter out;
  out << BeginSeq;
  out << "foo";
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test60faf9f5ac584b2d5a5f) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << "foo";
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testa34103141c1aec5e8fa8) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << "foo";
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testa2a2b83cb72c22fc4ac2) {
  Emitter out;
  out << BeginSeq;
  out << Comment("comment");
  out << "foo";
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test1f7ddcc3e5263ec983c5) {
  Emitter out;
  out << BeginSeq;
  out << "foo";
  out << Comment("comment");
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test638672d9ee378d2b59ec) {
  Emitter out;
  out << BeginSeq;
  out << "foo";
  out << BeginMap;
  out << Comment("comment");
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testffa1014d27bcb4821993) {
  Emitter out;
  out << BeginSeq;
  out << "foo";
  out << BeginMap;
  out << "foo";
  out << Comment("comment");
  out << "bar";
  out << EndMap;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, tested2f908c978d8908cde2) {
  Emitter out;
  out << BeginSeq;
  out << "foo";
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << Comment("comment");
  out << EndMap;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test75c9a342b4ff9dcddd76) {
  Emitter out;
  out << BeginSeq;
  out << "foo";
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << Comment("comment");
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testb3cf7afef788a5168172) {
  Emitter out;
  out << BeginSeq;
  out << "foo";
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test32c5731914c4f623f8cb) {
  Emitter out;
  out << BeginSeq;
  out << "foo";
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testc4e4b895f5a3615e6c92) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test024f81ab1e1f375d06c8) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testf73efa1189e8e9bab50c) {
  Emitter out;
  out << BeginSeq;
  out << Comment("comment");
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test074882061d59e9381988) {
  Emitter out;
  out << BeginSeq;
  out << BeginSeq;
  out << Comment("comment");
  out << "foo";
  out << EndSeq;
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testb81b19396e93a0e29157) {
  Emitter out;
  out << BeginSeq;
  out << BeginSeq;
  out << "foo";
  out << Comment("comment");
  out << EndSeq;
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testd4776155c00bb794aec9) {
  Emitter out;
  out << BeginSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << Comment("comment");
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test0d3813f2674c45d54be7) {
  Emitter out;
  out << BeginSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << "foo";
  out << Comment("comment");
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testaee95b4f9b1161f6fd14) {
  Emitter out;
  out << BeginSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << "foo";
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test3f7b4338eae92e7a575f) {
  Emitter out;
  out << BeginSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << "foo";
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test13a3de779d0756db2635) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test6cc39fc73a32cb816c15) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test33c4f4a8854d92180427) {
  Emitter out;
  out << BeginSeq;
  out << Comment("comment");
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test1e5612a63642939843fc) {
  Emitter out;
  out << BeginSeq;
  out << BeginSeq;
  out << Comment("comment");
  out << "foo";
  out << EndSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test774fccdfa9e9ea982df2) {
  Emitter out;
  out << BeginSeq;
  out << BeginSeq;
  out << "foo";
  out << Comment("comment");
  out << EndSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testecdd8159ca0150ac9084) {
  Emitter out;
  out << BeginSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << Comment("comment");
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test8081bb348b980caf237e) {
  Emitter out;
  out << BeginSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginSeq;
  out << Comment("comment");
  out << "foo";
  out << EndSeq;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test5dd9e7d23a6868f8da14) {
  Emitter out;
  out << BeginSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginSeq;
  out << "foo";
  out << Comment("comment");
  out << EndSeq;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test5cfe1848cf1771ee5a62) {
  Emitter out;
  out << BeginSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << Comment("comment");
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test1a78787fd377fecaa98a) {
  Emitter out;
  out << BeginSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test3659c5e006def41d781a) {
  Emitter out;
  out << BeginSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testcffd2b80f823bc7b88bc) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test3646d0fd40876cdc7d90) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testce3c9ef9e0c642d65146) {
  Emitter out;
  out << BeginSeq;
  out << Comment("comment");
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test666f93032b0297bd6f81) {
  Emitter out;
  out << BeginSeq;
  out << BeginSeq;
  out << Comment("comment");
  out << "foo";
  out << EndSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test7939e2697c6f4d8b91e9) {
  Emitter out;
  out << BeginSeq;
  out << BeginSeq;
  out << "foo";
  out << Comment("comment");
  out << EndSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test3c153927514f7330e37d) {
  Emitter out;
  out << BeginSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << Comment("comment");
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test59b085e35e59562026da) {
  Emitter out;
  out << BeginSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginMap;
  out << Comment("comment");
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test367f38cb15951f58f294) {
  Emitter out;
  out << BeginSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginMap;
  out << "foo";
  out << Comment("comment");
  out << "bar";
  out << EndMap;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testbb3feccd2d65d126c9e4) {
  Emitter out;
  out << BeginSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << Comment("comment");
  out << EndMap;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test4c5079959ae8b908aee3) {
  Emitter out;
  out << BeginSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << Comment("comment");
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testfa82743c73819854df98) {
  Emitter out;
  out << BeginSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, teste354627da20a33a0ec48) {
  Emitter out;
  out << BeginSeq;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test202a2c088ee50d08f182) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testc7277e5979e7eaf4d016) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, teste2b7015fc02e6a5cb416) {
  Emitter out;
  out << BeginSeq;
  out << Comment("comment");
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test30e30b89df20ef693d7b) {
  Emitter out;
  out << BeginSeq;
  out << BeginMap;
  out << Comment("comment");
  out << "foo";
  out << "bar";
  out << EndMap;
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test5cab866f8818d65f5cfe) {
  Emitter out;
  out << BeginSeq;
  out << BeginMap;
  out << "foo";
  out << Comment("comment");
  out << "bar";
  out << EndMap;
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test6c2133597d99c14720d0) {
  Emitter out;
  out << BeginSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << Comment("comment");
  out << EndMap;
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test243b3bb00a830d4d7c5c) {
  Emitter out;
  out << BeginSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << Comment("comment");
  out << "foo";
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test7fcdb9d71a9717069b9f) {
  Emitter out;
  out << BeginSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << "foo";
  out << Comment("comment");
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test0ce9909c470ae5b1d3ae) {
  Emitter out;
  out << BeginSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << "foo";
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testc077fd929177c75e3072) {
  Emitter out;
  out << BeginSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << "foo";
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test3b500af23a2b660e0330) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test6d037fb77412a7b06f24) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testa153abd07d892e352889) {
  Emitter out;
  out << BeginSeq;
  out << Comment("comment");
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testf328e682c5d872fbb7fa) {
  Emitter out;
  out << BeginSeq;
  out << BeginMap;
  out << Comment("comment");
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testb59b29e1f0612f8333bf) {
  Emitter out;
  out << BeginSeq;
  out << BeginMap;
  out << "foo";
  out << Comment("comment");
  out << "bar";
  out << EndMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test0d2de653c36fff7e875f) {
  Emitter out;
  out << BeginSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << Comment("comment");
  out << EndMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test18454002edb2d8ce5dff) {
  Emitter out;
  out << BeginSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << Comment("comment");
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test3869b95fb927574c1cfa) {
  Emitter out;
  out << BeginSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginSeq;
  out << Comment("comment");
  out << "foo";
  out << EndSeq;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test5f9d2836779171944bd5) {
  Emitter out;
  out << BeginSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginSeq;
  out << "foo";
  out << Comment("comment");
  out << EndSeq;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test00ca741ed5db71e7197c) {
  Emitter out;
  out << BeginSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << Comment("comment");
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test63b2dee62c4656039c0d) {
  Emitter out;
  out << BeginSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test6fca12b5bf897f3b933f) {
  Emitter out;
  out << BeginSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginSeq;
  out << "foo";
  out << EndSeq;
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testf047a3a7cd147dc69553) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testf191f92a0c15e5624272) {
  Emitter out;
  out << Comment("comment");
  out << BeginSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test874f18e2970f54d92f34) {
  Emitter out;
  out << BeginSeq;
  out << Comment("comment");
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test7e94b62bc08fb63b7062) {
  Emitter out;
  out << BeginSeq;
  out << BeginMap;
  out << Comment("comment");
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test50d40871122b935a2937) {
  Emitter out;
  out << BeginSeq;
  out << BeginMap;
  out << "foo";
  out << Comment("comment");
  out << "bar";
  out << EndMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testb6a8d9eb931455caa603) {
  Emitter out;
  out << BeginSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << Comment("comment");
  out << EndMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test2f23bfff4a1d201b1f37) {
  Emitter out;
  out << BeginSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << Comment("comment");
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test41ef23e040f0db2c06c2) {
  Emitter out;
  out << BeginSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginMap;
  out << Comment("comment");
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, testb8c7b55b5607df008fbe) {
  Emitter out;
  out << BeginSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginMap;
  out << "foo";
  out << Comment("comment");
  out << "bar";
  out << EndMap;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test21daf188179c0c0c7bef) {
  Emitter out;
  out << BeginSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << Comment("comment");
  out << EndMap;
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test8ca560e6b97325079e8f) {
  Emitter out;
  out << BeginSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << Comment("comment");
  out << EndSeq;

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test7fe0ce86b72c7c1ed7de) {
  Emitter out;
  out << BeginSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}

TEST_F(GenEmitterTest, test8cc25a6c1aea65ad7de1) {
  Emitter out;
  out << BeginSeq;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << BeginMap;
  out << "foo";
  out << "bar";
  out << EndMap;
  out << EndSeq;
  out << Comment("comment");

  EXPECT_CALL(handler, OnDocumentStart(_));
  EXPECT_CALL(handler, OnSequenceStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnMapStart(_, "?", 0, _));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "foo"));
  EXPECT_CALL(handler, OnScalar(_, "?", 0, "bar"));
  EXPECT_CALL(handler, OnMapEnd());
  EXPECT_CALL(handler, OnSequenceEnd());
  EXPECT_CALL(handler, OnDocumentEnd());
  Parse(out.c_str());
}
}
}
