#pragma once

#include <map>
#include <string>

std::string GenerateRandomString(size_t length);
std::map<std::string, std::string> GenerateRandomLabels(
    std::size_t number_of_labels);
