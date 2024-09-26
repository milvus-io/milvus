// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cassert>
#include <memory>
#include <string>
#include <vector>

#include "common/Vector.h"
#include "exec/QueryContext.h"

namespace milvus {
namespace exec {

class ExprSet;
class EvalCtx {
 public:
    EvalCtx(ExecContext* exec_ctx, ExprSet* expr_set, RowVector* row)
        : exec_ctx_(exec_ctx), expr_set_(expr_set), row_(row) {
        assert(exec_ctx_ != nullptr);
        assert(expr_set_ != nullptr);
        // assert(row_ != nullptr);
    }

    explicit EvalCtx(ExecContext* exec_ctx)
        : exec_ctx_(exec_ctx), expr_set_(nullptr), row_(nullptr) {
    }

    ExecContext*
    get_exec_context() {
        return exec_ctx_;
    }

    std::shared_ptr<QueryConfig>
    get_query_config() {
        return exec_ctx_->get_query_config();
    }

 private:
    ExecContext* exec_ctx_;
    ExprSet* expr_set_;
    RowVector* row_;
    bool input_no_nulls_;
};

}  // namespace exec
}  // namespace milvus