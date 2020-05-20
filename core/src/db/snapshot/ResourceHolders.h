#pragma once
#include "ResourceTypes.h"
#include "ScopedResource.h"
#include "Resources.h"
#include "BaseHolders.h"
#include <string>
#include <map>
#include <vector>
#include <memory>
#include <condition_variable>
#include <mutex>
#include <thread>


class CollectionsHolder : public ResourceHolder<Collection, CollectionsHolder> {
public:
    using BaseT = ResourceHolder<Collection, CollectionsHolder>;
    using ResourcePtr = typename BaseT::ResourcePtr;
    using NameMapT = std::map<std::string, ResourcePtr>;

    ScopedT GetCollection(const std::string& name, bool scoped = true);

    bool Add(ResourcePtr resource) override;
    bool Release(ID_TYPE id) override;
    bool Release(const std::string& name);

private:
    ResourcePtr Load(const std::string& name) override;

    NameMapT name_map_;
};

class SchemaCommitsHolder : public ResourceHolder<SchemaCommit, SchemaCommitsHolder> {};

class FieldCommitsHolder : public ResourceHolder<FieldCommit, FieldCommitsHolder> {};

class FieldsHolder : public ResourceHolder<Field, FieldsHolder> {};

class FieldElementsHolder : public ResourceHolder<FieldElement, FieldElementsHolder> {};

class CollectionCommitsHolder : public ResourceHolder<CollectionCommit, CollectionCommitsHolder> {};

class PartitionsHolder : public ResourceHolder<Partition, PartitionsHolder> {};

class PartitionCommitsHolder : public ResourceHolder<PartitionCommit, PartitionCommitsHolder> {};

class SegmentsHolder : public ResourceHolder<Segment, SegmentsHolder> {};

class SegmentCommitsHolder : public ResourceHolder<SegmentCommit, SegmentCommitsHolder> {};

class SegmentFilesHolder : public ResourceHolder<SegmentFile, SegmentFilesHolder> {};
