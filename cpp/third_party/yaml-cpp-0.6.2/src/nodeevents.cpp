#include "nodeevents.h"
#include "yaml-cpp/eventhandler.h"
#include "yaml-cpp/mark.h"
#include "yaml-cpp/node/detail/node.h"
#include "yaml-cpp/node/detail/node_iterator.h"
#include "yaml-cpp/node/node.h"
#include "yaml-cpp/node/type.h"

namespace YAML {
void NodeEvents::AliasManager::RegisterReference(const detail::node& node) {
  m_anchorByIdentity.insert(std::make_pair(node.ref(), _CreateNewAnchor()));
}

anchor_t NodeEvents::AliasManager::LookupAnchor(
    const detail::node& node) const {
  AnchorByIdentity::const_iterator it = m_anchorByIdentity.find(node.ref());
  if (it == m_anchorByIdentity.end())
    return 0;
  return it->second;
}

NodeEvents::NodeEvents(const Node& node)
    : m_pMemory(node.m_pMemory), m_root(node.m_pNode) {
  if (m_root)
    Setup(*m_root);
}

void NodeEvents::Setup(const detail::node& node) {
  int& refCount = m_refCount[node.ref()];
  refCount++;
  if (refCount > 1)
    return;

  if (node.type() == NodeType::Sequence) {
    for (detail::const_node_iterator it = node.begin(); it != node.end(); ++it)
      Setup(**it);
  } else if (node.type() == NodeType::Map) {
    for (detail::const_node_iterator it = node.begin(); it != node.end();
         ++it) {
      Setup(*it->first);
      Setup(*it->second);
    }
  }
}

void NodeEvents::Emit(EventHandler& handler) {
  AliasManager am;

  handler.OnDocumentStart(Mark());
  if (m_root)
    Emit(*m_root, handler, am);
  handler.OnDocumentEnd();
}

void NodeEvents::Emit(const detail::node& node, EventHandler& handler,
                      AliasManager& am) const {
  anchor_t anchor = NullAnchor;
  if (IsAliased(node)) {
    anchor = am.LookupAnchor(node);
    if (anchor) {
      handler.OnAlias(Mark(), anchor);
      return;
    }

    am.RegisterReference(node);
    anchor = am.LookupAnchor(node);
  }

  switch (node.type()) {
    case NodeType::Undefined:
      break;
    case NodeType::Null:
      handler.OnNull(Mark(), anchor);
      break;
    case NodeType::Scalar:
      handler.OnScalar(Mark(), node.tag(), anchor, node.scalar());
      break;
    case NodeType::Sequence:
      handler.OnSequenceStart(Mark(), node.tag(), anchor, node.style());
      for (detail::const_node_iterator it = node.begin(); it != node.end();
           ++it)
        Emit(**it, handler, am);
      handler.OnSequenceEnd();
      break;
    case NodeType::Map:
      handler.OnMapStart(Mark(), node.tag(), anchor, node.style());
      for (detail::const_node_iterator it = node.begin(); it != node.end();
           ++it) {
        Emit(*it->first, handler, am);
        Emit(*it->second, handler, am);
      }
      handler.OnMapEnd();
      break;
  }
}

bool NodeEvents::IsAliased(const detail::node& node) const {
  RefCount::const_iterator it = m_refCount.find(node.ref());
  return it != m_refCount.end() && it->second > 1;
}
}
