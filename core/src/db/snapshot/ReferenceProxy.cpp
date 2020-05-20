#include "ReferenceProxy.h"
#include <assert.h>
#include <iostream>


void ReferenceProxy::Ref() {
    refcnt_ += 1;
    /* std::cout << this << " refcnt = " << refcnt_ << std::endl; */
}

void ReferenceProxy::UnRef() {
    if (refcnt_ == 0) return;
    refcnt_ -= 1;
    /* std::cout << this << " refcnt = " << refcnt_ << std::endl; */
    if (refcnt_ == 0) {
        for (auto& cb : on_no_ref_cbs_) {
            cb();
        }
    }
}

void ReferenceProxy::RegisterOnNoRefCB(OnNoRefCBF cb) {
    on_no_ref_cbs_.emplace_back(cb);
}

ReferenceProxy::~ReferenceProxy() {
    /* OnDeRef(); */
}
