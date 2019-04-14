#ifndef NODE_NODEBUILDER_H_62B23520_7C8E_11DE_8A39_0800200C9A66
#define NODE_NODEBUILDER_H_62B23520_7C8E_11DE_8A39_0800200C9A66

#if defined(_MSC_VER) ||                                            \
    (defined(__GNUC__) && (__GNUC__ == 3 && __GNUC_MINOR__ >= 4) || \
     (__GNUC__ >= 4))  // GCC supports "pragma once" correctly since 3.4
#pragma once
#endif

#include <vector>

#include "yaml-cpp/anchor.h"
#include "yaml-cpp/emitterstyle.h"
#include "yaml-cpp/eventhandler.h"
#include "yaml-cpp/node/ptr.h"

namespace YAML {
namespace detail {
class node;
}  // namespace detail
struct Mark;
}  // namespace YAML

namespace YAML {
class Node;

class NodeBuilder : public EventHandler {
 public:
  NodeBuilder();
  virtual ~NodeBuilder();

  Node Root();

  virtual void OnDocumentStart(const Mark& mark);
  virtual void OnDocumentEnd();

  virtual void OnNull(const Mark& mark, anchor_t anchor);
  virtual void OnAlias(const Mark& mark, anchor_t anchor);
  virtual void OnScalar(const Mark& mark, const std::string& tag,
                        anchor_t anchor, const std::string& value);

  virtual void OnSequenceStart(const Mark& mark, const std::string& tag,
                               anchor_t anchor, EmitterStyle::value style);
  virtual void OnSequenceEnd();

  virtual void OnMapStart(const Mark& mark, const std::string& tag,
                          anchor_t anchor, EmitterStyle::value style);
  virtual void OnMapEnd();

 private:
  detail::node& Push(const Mark& mark, anchor_t anchor);
  void Push(detail::node& node);
  void Pop();
  void RegisterAnchor(anchor_t anchor, detail::node& node);

 private:
  detail::shared_memory_holder m_pMemory;
  detail::node* m_pRoot;

  typedef std::vector<detail::node*> Nodes;
  Nodes m_stack;
  Nodes m_anchors;

  typedef std::pair<detail::node*, bool> PushedKey;
  std::vector<PushedKey> m_keys;
  std::size_t m_mapDepth;
};
}

#endif  // NODE_NODEBUILDER_H_62B23520_7C8E_11DE_8A39_0800200C9A66
