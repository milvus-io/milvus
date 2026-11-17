// Copyright 2026 Zilliz
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#pragma once

#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/json/JsonSerializer.h>

namespace milvus::storage::native_sts {

inline bool
IsNonBlank(const Aws::String& value) {
    return value.find_first_not_of(" \t\r\n\f\v") != Aws::String::npos;
}

// STS credentials require all three fields and an explicit, usable expiration.
// AWSCredentials::IsEmpty() only checks whether BOTH keys are empty, and its
// default expiration means "never expires", which is invalid for STS results.
inline bool
IsUsable(const Aws::Auth::AWSCredentials& credentials) {
    const auto expiration = credentials.GetExpiration();
    return IsNonBlank(credentials.GetAWSAccessKeyId()) &&
           IsNonBlank(credentials.GetAWSSecretKey()) &&
           IsNonBlank(credentials.GetSessionToken()) &&
           expiration.WasParseSuccessful() &&
           expiration > Aws::Utils::DateTime::Now() &&
           expiration != Aws::Auth::AWSCredentials{}.GetExpiration();
}

inline Aws::Utils::Json::JsonView
ObjectField(Aws::Utils::Json::JsonView object, const char* name) {
    if (!object.IsObject())
        return {};
    const auto fields = object.GetAllObjects();
    const auto found = fields.find(name);
    return found != fields.end() && found->second.IsObject()
               ? found->second
               : Aws::Utils::Json::JsonView{};
}

inline Aws::String
StringField(Aws::Utils::Json::JsonView object, const char* name) {
    if (!object.IsObject())
        return {};
    const auto fields = object.GetAllObjects();
    const auto found = fields.find(name);
    if (found == fields.end() || !found->second.IsString())
        return {};
    return Aws::Utils::StringUtils::Trim(found->second.AsString().c_str());
}

}  // namespace milvus::storage::native_sts
