////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "faiss/Index.h"

#include "Operand.h"
#include "Index.h"

namespace zilliz {
namespace vecwise {
namespace engine {

class IndexBuilder {
public:
    explicit IndexBuilder(const Operand_ptr &opd);

    Index_ptr build_all(const long &nb,
                        const float* xb,
                        const long* ids,
                        const long &nt = 0,
                        const float* xt = nullptr);

    Index_ptr build_all(const long &nb,
                        const std::vector<float> &xb,
                        const std::vector<long> &ids,
                        const long &nt = 0,
                        const std::vector<float> &xt = std::vector<float>());

    void train(const long &nt,
               const std::vector<float> &xt);

    Index_ptr add(const long &nb,
                  const std::vector<float> &xb,
                  const std::vector<long> &ids);

    void set_build_option(const Operand_ptr &opd);


private:
    Operand_ptr opd_ = nullptr;
};

using IndexBuilderPtr = std::shared_ptr<IndexBuilder>;

extern IndexBuilderPtr GetIndexBuilder(const Operand_ptr &opd);

}
}
}
