#include <algorithm>
#include <cstdlib>

#include "benchmark_helpers.h"

std::string GenerateRandomString(size_t length) {
  auto randchar = []() -> char {
    const char charset[] = "abcdefghijklmnopqrstuvwxyz";
    const size_t max_index = (sizeof(charset) - 1);
    return charset[rand() % max_index];
  };
  std::string str(length, 0);
  std::generate_n(str.begin(), length, randchar);
  return str;
}

std::map<std::string, std::string> GenerateRandomLabels(
    std::size_t number_of_pairs) {
  const auto label_character_count = 10;
  auto label_pairs = std::map<std::string, std::string>{};
  for (std::size_t i = 0; i < number_of_pairs; i++) {
    label_pairs.insert({GenerateRandomString(label_character_count),
                        GenerateRandomString(label_character_count)});
  }
  return label_pairs;
}
