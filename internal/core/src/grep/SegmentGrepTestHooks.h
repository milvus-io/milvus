// Copyright (C) 2026 Zilliz. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#pragma once

#include <functional>

namespace milvus::grep {
// Thread-local observer for deterministic cancellation tests. Called before
// each input batch's cancellation check. Returns the previous observer so the
// caller can restore it without affecting scans on other threads.
std::function<void()>
SetSegmentGrepBatchObserverForTesting(std::function<void()> observer);
}  // namespace milvus::grep
