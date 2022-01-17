//
// Copyright (C) 2015-2020 Yahoo Japan Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#pragma once

#include <ostream>
#include <string>

#ifndef NGT_VERSION
#define NGT_VERSION "-"
#endif
#ifndef NGT_BUILD_DATE
#define NGT_BUILD_DATE "-"
#endif
#ifndef NGT_GIT_HASH
#define NGT_GIT_HASH "-"
#endif
#ifndef NGT_GIT_DATE
#define NGT_GIT_DATE "-"
#endif
#ifndef NGT_GIT_TAG
#define NGT_GIT_TAG "-"
#endif

namespace NGT {
class Version {
 public:
    static void
    get(std::ostream& os);
    static const std::string
    getVersion();
    static const std::string
    getBuildDate();
    static const std::string
    getGitHash();
    static const std::string
    getGitDate();
    static const std::string
    getGitTag();
    static const std::string
    get();
};

};  // namespace NGT

#ifdef NGT_VERSION_FOR_HEADER
#include "Version.cpp"
#endif
