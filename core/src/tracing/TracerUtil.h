#pragma once

#include <string>

class TracerUtil {

 public:

    static void InitGlobal(const std::string& config_path = "");

 private:

    static void LoadConfig(const std::string& config_path);
};