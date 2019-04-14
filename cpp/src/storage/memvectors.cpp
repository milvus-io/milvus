#include <IndexFlat.h>
#include <MetaIndexes.h>
#include <index_io.h>

#include "memvectors.h"


namespace vecengine {

MemVectors::MemVectors(size_t dimension_, const std::string& file_location_) :
    _file_location(file_location_),
    _pIdGenerator(new SimpleIDGenerator()),
    _dimension(dimension_),
    _pInnerIndex(new faiss::IndexFlat(_dimension)),
    _pIdMapIndex = new faiss::IndexIDMap(_pInnerIndex) {
}

IDNumbers&& MemVectors::add(size_t n, const float* vectors) {
    IDNumbers&& ids = _pIdGenerator->getNextIDNumbers(n);
    _pIdMapIndex->add_with_ids(n, vectors, pIds, &ids[0]);
    return ids;
}

size_t MemVectors::total() const {
    return _pIdMapIndex->ntotal;
}

size_t MemVectors::approximate_size() const {
    return total() * _dimension;
}

void MemVectors::serialize() {
    faiss::write_index(_pIdMapIndex, _file_location);
}

MemVectors::~MemVectors() {
    if (_pIdGenerator != nullptr) {
        delete _pIdGenerator;
        _pIdGenerator = nullptr;
    }
    if (_pIdMapIndex != nullptr) {
        delete _pIdMapIndex;
        _pIdMapIndex = nullptr;
    }
    if (_pInnerIndex != nullptr) {
        delete _pInnerIndex;
        _pInnerIndex = nullptr;
    }
}

/*
 * MemManager
 */

MemVectors* MemManager::get_mem_by_group(const std::string& group_id_) {
    auto memIt = _memMap.find(group_id_);
    if memIt != _memMap.end() {
        return &(memIt->second);
    }
    // PXU TODO:
    // 1. Read Group meta info
    // 2. Initalize MemVectors base meta info
    return nullptr;
    /* GroupMetaInfo info; */
    /* bool succ = env->getGroupMeta(group_id, &info); */
    /* if (!succ) { */
    /*     return nullptr; */
    /* } */
    /* _memMap[group_id] = MemVectors(info.dimension, info.next_file_location); */
    /* return &(_memMap[group_id]); */
}

IDNumbers&& MemManager::add_vectors_no_lock(const std::string& group_id_,
        size_t n,
        const float* vectors) {
    auto mem = get_group_mem(group_id_);
    if (mem == nullptr) {
        return IDNumbers();
    }
    return mem->add(n, vectors);
}


} // namespace vecengine
