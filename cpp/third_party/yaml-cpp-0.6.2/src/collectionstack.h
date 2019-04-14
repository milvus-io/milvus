#ifndef COLLECTIONSTACK_H_62B23520_7C8E_11DE_8A39_0800200C9A66
#define COLLECTIONSTACK_H_62B23520_7C8E_11DE_8A39_0800200C9A66

#if defined(_MSC_VER) ||                                            \
    (defined(__GNUC__) && (__GNUC__ == 3 && __GNUC_MINOR__ >= 4) || \
     (__GNUC__ >= 4))  // GCC supports "pragma once" correctly since 3.4
#pragma once
#endif

#include <stack>
#include <cassert>

namespace YAML {
struct CollectionType {
  enum value { NoCollection, BlockMap, BlockSeq, FlowMap, FlowSeq, CompactMap };
};

class CollectionStack {
 public:
  CollectionType::value GetCurCollectionType() const {
    if (collectionStack.empty())
      return CollectionType::NoCollection;
    return collectionStack.top();
  }

  void PushCollectionType(CollectionType::value type) {
    collectionStack.push(type);
  }
  void PopCollectionType(CollectionType::value type) {
    assert(type == GetCurCollectionType());
    collectionStack.pop();
  }

 private:
  std::stack<CollectionType::value> collectionStack;
};
}

#endif  // COLLECTIONSTACK_H_62B23520_7C8E_11DE_8A39_0800200C9A66
