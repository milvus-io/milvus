#include "prometheus/detail/utils.h"

#include <gmock/gmock.h>
#include <map>

namespace prometheus {

namespace {

TEST(UtilsTest, hash_labels_1) {
  std::map<std::string, std::string> labels;
  labels.insert(std::make_pair<std::string, std::string>("key1", "value1"));
  labels.insert(std::make_pair<std::string, std::string>("key2", "vaule2"));
  auto value1 = detail::hash_labels(labels);
  auto value2 = detail::hash_labels(labels);

  EXPECT_EQ(value1, value2);
}

TEST(UtilsTest, hash_labels_2) {
  std::map<std::string, std::string> labels1{{"aa", "bb"}};
  std::map<std::string, std::string> labels2{{"a", "abb"}};
  EXPECT_NE(detail::hash_labels(labels1), detail::hash_labels(labels2));
}

TEST(UtilsTest, hash_label_3) {
  std::map<std::string, std::string> labels1{{"a", "a"}};
  std::map<std::string, std::string> labels2{{"aa", ""}};
  EXPECT_NE(detail::hash_labels(labels1), detail::hash_labels(labels2));
}

}  // namespace

}  // namespace prometheus
