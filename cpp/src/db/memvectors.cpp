#include <IndexFlat.h>
#include <MetaIndexes.h>
#include <index_io.h>

#include "memvectors.h"
#include "db_meta.h"


namespace vecengine {

MemVectors::MemVectors(size_t dimension_, const std::string& file_location_) :
    _file_location(file_location_),
    _pIdGenerator(new SimpleIDGenerator()),
    _dimension(dimension_),
    _pInnerIndex(new faiss::IndexFlat(_dimension)),
    _pIdMapIndex = new faiss::IndexIDMap(_pInnerIndex) {
}

void MemVectors::add(size_t n_, const float* vectors_, IDNumbers& vector_ids_) {
    vector_ids_ = _pIdGenerator->getNextIDNumbers(n_);
    _pIdMapIndex->add_with_ids(n_, vectors_, &vector_ids_[0]);
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

    GroupSchema group_info;
    Status status = _pMeta->get_group(group_id_, group_info);
    if (!status.ok()) {
        return nullptr;
    }
    _memMap[group_id] = MemVectors(group_info.dimension, group_info.next_file_location);
    return &(_memMap[group_id]);
}

Status MemManager::add_vectors(const std::string& group_id_,
        size_t n_,
        const float* vectors_,
        IDNumbers& vector_ids_) {
    // PXU TODO
    return add_vectors_no_lock(group_id_, n_, vectors_, vector_ids_);
}

Status MemManager::add_vectors_no_lock(const std::string& group_id_,
        size_t n,
        const float* vectors,
        IDNumbers& vector_ids_) {
    auto mem = get_mem_by_group(group_id_);
    if (mem == nullptr) {
        return Status::NotFound("Group " + group_id_ " not found!");
    }
    return mem->add(n, vectors, vector_ids_);
}


} // namespace vecengine
