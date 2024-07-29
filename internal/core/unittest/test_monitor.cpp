// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <string>
#include <vector>
#include <gtest/gtest.h>

#include "monitor/monitor_c.h"
#include "monitor/prometheus_client.h"

using namespace std;

vector<string>
split(const string& str,
      const string& delim) {  //将分割后的子字符串存储在vector中
    vector<string> res;
    if ("" == str)
        return res;

    string strs = str + delim;
    size_t pos;
    size_t size = strs.size();

    for (int i = 0; i < size; ++i) {
        pos = strs.find(delim, i);
        if (pos < size) {
            string s = strs.substr(i, pos - i);
            res.push_back(s);
            i = pos + delim.size() - 1;
        }
    }
    return res;
}

class MonitorTest : public testing::Test {
 public:
    MonitorTest() {
    }
    ~MonitorTest() {
    }
    virtual void
    SetUp() {
    }
};

TEST_F(MonitorTest, GetCoreMetrics) {
    auto metricsChars = GetCoreMetrics();
    string helpPrefix = "# HELP ";
    string familyName = "";
    char* p;
    const char* delim = "\n";
    p = strtok(metricsChars, delim);
    while (p) {
        char* currentLine = p;
        p = strtok(NULL, delim);
        if (strncmp(currentLine, "# HELP ", 7) == 0) {
            familyName = "";
            continue;
        } else if (strncmp(currentLine, "# TYPE ", 7) == 0) {
            std::vector<string> res = split(currentLine, " ");
            EXPECT_EQ(4, res.size());
            familyName = res[2];
            EXPECT_EQ(true,
                      res[3] == "gauge" || res[3] == "counter" ||
                          res[3] == "histogram");
            continue;
        }
        EXPECT_EQ(true, familyName.length() > 0);
        EXPECT_EQ(
            0, strncmp(currentLine, familyName.c_str(), familyName.length()));
    }
}