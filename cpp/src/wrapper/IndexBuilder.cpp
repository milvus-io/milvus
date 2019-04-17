////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include "mutex"

#include "IndexBuilder.h"

namespace zilliz {
namespace vecwise {
namespace engine {

using std::vector;

// todo(linxj): use ResourceMgr instead
static std::mutex cpu_resource;

IndexBuilder::IndexBuilder(const Operand_ptr &opd) {
    opd_ = opd;
}

Index_ptr IndexBuilder::build_all(const long &nb,
                                  const float* xb,
                                  const long* ids,
                                  const long &nt,
                                  const float* xt) {
    std::shared_ptr<faiss::Index> index = nullptr;
    index.reset(faiss::index_factory(opd_->d, opd_->index_type.c_str()));

    {
        // currently only cpu resources are used.
        std::lock_guard<std::mutex> lk(cpu_resource);
        if (!index->is_trained) {
            nt == 0 || xt == nullptr ? index->train(nb, xb)
                                  : index->train(nt, xt);
        }
        index->add_with_ids(nb, xb, ids); // todo(linxj): support add_with_idmap
    }

    return std::make_shared<Index>(index);

}

Index_ptr IndexBuilder::build_all(const long &nb, const vector<float> &xb,
                                  const vector<long> &ids,
                                  const long &nt, const vector<float> &xt) {
    return build_all(nb, xb.data(), ids.data(), nt, xt.data());
}

// Be Factory pattern later
IndexBuilderPtr GetIndexBuilder(const Operand_ptr &opd) {
    return std::make_shared<IndexBuilder>(opd);
}

}
}
}
