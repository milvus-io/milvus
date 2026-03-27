#pragma once

#include <cstdint>

namespace knowhere {

class Version {
 public:
    constexpr explicit Version(int64_t version = kCurrentVersion)
        : version_(version) {
    }

    constexpr int64_t
    VersionNumber() const {
        return version_;
    }

    static constexpr Version
    GetCurrentVersion() {
        return Version(kCurrentVersion);
    }

    static constexpr Version
    GetMinimalVersion() {
        return Version(kCurrentVersion);
    }

    static constexpr Version
    GetMaximumVersion() {
        return Version(kCurrentVersion);
    }

    static constexpr bool
    VersionSupport(const Version&) {
        return true;
    }

 private:
    static constexpr int64_t kCurrentVersion = 1;
    int64_t version_;
};

}  // namespace knowhere
