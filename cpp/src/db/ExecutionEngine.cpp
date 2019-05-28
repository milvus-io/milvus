/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "ExecutionEngine.h"

#include <easylogging++.h>

namespace zilliz {
namespace vecwise {
namespace engine {

template<typename Derived>
Status ExecutionEngine<Derived>::AddWithIds(const std::vector<float>& vectors, const std::vector<long>& vector_ids) {
    long n1 = (long)vectors.size();
    long n2 = (long)vector_ids.size();
    if (n1 != n2) {
        LOG(ERROR) << "vectors size is not equal to the size of vector_ids: " << n1 << "!=" << n2;
        return Status::Error("Error: AddWithIds");
    }
    return AddWithIds(n1, vectors.data(), vector_ids.data());
}

template<typename Derived>
Status ExecutionEngine<Derived>::AddWithIds(long n, const float *xdata, const long *xids) {
    return static_cast<Derived*>(this)->AddWithIds(n, xdata, xids);
}

template<typename Derived>
size_t ExecutionEngine<Derived>::Count() const {
    return static_cast<Derived*>(this)->Count();
}

template<typename Derived>
size_t ExecutionEngine<Derived>::Size() const {
    return static_cast<Derived*>(this)->Size();
}

template<typename Derived>
size_t ExecutionEngine<Derived>::PhysicalSize() const {
    return static_cast<Derived*>(this)->PhysicalSize();
}

template<typename Derived>
Status ExecutionEngine<Derived>::Serialize() {
    return static_cast<Derived*>(this)->Serialize();
}

template<typename Derived>
Status ExecutionEngine<Derived>::Load() {
    return static_cast<Derived*>(this)->Load();
}

template<typename Derived>
Status ExecutionEngine<Derived>::Merge(const std::string& location) {
    return static_cast<Derived*>(this)->Merge(location);
}

template<typename Derived>
Status ExecutionEngine<Derived>::Search(long n,
                          const float *data,
                          long k,
                          float *distances,
                          long *labels) const {
    return static_cast<Derived*>(this)->Search(n, data, k, distances, labels);
}

template<typename Derived>
Status ExecutionEngine<Derived>::Cache() {
    return static_cast<Derived*>(this)->Cache();
}

template<typename Derived>
std::shared_ptr<Derived> ExecutionEngine<Derived>::BuildIndex(const std::string& location) {
    return static_cast<Derived*>(this)->BuildIndex(location);
}


} // namespace engine
} // namespace vecwise
} // namespace zilliz
