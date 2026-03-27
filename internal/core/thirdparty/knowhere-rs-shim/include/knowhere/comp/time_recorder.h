#pragma once

#include <string>

namespace knowhere {

class TimeRecorder {
 public:
    TimeRecorder(std::string, int = 0) {
    }

    void
    RecordSection(const std::string&) {
    }

    void
    ElapseFromBegin(const std::string&) {
    }
};

}  // namespace knowhere
