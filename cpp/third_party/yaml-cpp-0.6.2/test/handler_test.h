#include "mock_event_handler.h"
#include "yaml-cpp/yaml.h"  // IWYU pragma: keep

#include "gmock/gmock.h"
#include "gtest/gtest.h"

using ::testing::InSequence;
using ::testing::NiceMock;
using ::testing::StrictMock;

namespace YAML {
class HandlerTest : public ::testing::Test {
 protected:
  void Parse(const std::string& example) {
    std::stringstream stream(example);
    Parser parser(stream);
    while (parser.HandleNextDocument(handler)) {
    }
  }

  void IgnoreParse(const std::string& example) {
    std::stringstream stream(example);
    Parser parser(stream);
    while (parser.HandleNextDocument(nice_handler)) {
    }
  }

  InSequence sequence;
  StrictMock<MockEventHandler> handler;
  NiceMock<MockEventHandler> nice_handler;
};
}
