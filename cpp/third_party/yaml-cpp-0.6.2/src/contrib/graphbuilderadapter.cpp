#include "graphbuilderadapter.h"
#include "yaml-cpp/contrib/graphbuilder.h"

namespace YAML {
struct Mark;

int GraphBuilderAdapter::ContainerFrame::sequenceMarker;

void GraphBuilderAdapter::OnNull(const Mark &mark, anchor_t anchor) {
  void *pParent = GetCurrentParent();
  void *pNode = m_builder.NewNull(mark, pParent);
  RegisterAnchor(anchor, pNode);

  DispositionNode(pNode);
}

void GraphBuilderAdapter::OnAlias(const Mark &mark, anchor_t anchor) {
  void *pReffedNode = m_anchors.Get(anchor);
  DispositionNode(m_builder.AnchorReference(mark, pReffedNode));
}

void GraphBuilderAdapter::OnScalar(const Mark &mark, const std::string &tag,
                                   anchor_t anchor, const std::string &value) {
  void *pParent = GetCurrentParent();
  void *pNode = m_builder.NewScalar(mark, tag, pParent, value);
  RegisterAnchor(anchor, pNode);

  DispositionNode(pNode);
}

void GraphBuilderAdapter::OnSequenceStart(const Mark &mark,
                                          const std::string &tag,
                                          anchor_t anchor,
                                          EmitterStyle::value /* style */) {
  void *pNode = m_builder.NewSequence(mark, tag, GetCurrentParent());
  m_containers.push(ContainerFrame(pNode));
  RegisterAnchor(anchor, pNode);
}

void GraphBuilderAdapter::OnSequenceEnd() {
  void *pSequence = m_containers.top().pContainer;
  m_containers.pop();

  DispositionNode(pSequence);
}

void GraphBuilderAdapter::OnMapStart(const Mark &mark, const std::string &tag,
                                     anchor_t anchor,
                                     EmitterStyle::value /* style */) {
  void *pNode = m_builder.NewMap(mark, tag, GetCurrentParent());
  m_containers.push(ContainerFrame(pNode, m_pKeyNode));
  m_pKeyNode = NULL;
  RegisterAnchor(anchor, pNode);
}

void GraphBuilderAdapter::OnMapEnd() {
  void *pMap = m_containers.top().pContainer;
  m_pKeyNode = m_containers.top().pPrevKeyNode;
  m_containers.pop();
  DispositionNode(pMap);
}

void *GraphBuilderAdapter::GetCurrentParent() const {
  if (m_containers.empty()) {
    return NULL;
  }
  return m_containers.top().pContainer;
}

void GraphBuilderAdapter::RegisterAnchor(anchor_t anchor, void *pNode) {
  if (anchor) {
    m_anchors.Register(anchor, pNode);
  }
}

void GraphBuilderAdapter::DispositionNode(void *pNode) {
  if (m_containers.empty()) {
    m_pRootNode = pNode;
    return;
  }

  void *pContainer = m_containers.top().pContainer;
  if (m_containers.top().isMap()) {
    if (m_pKeyNode) {
      m_builder.AssignInMap(pContainer, m_pKeyNode, pNode);
      m_pKeyNode = NULL;
    } else {
      m_pKeyNode = pNode;
    }
  } else {
    m_builder.AppendToSequence(pContainer, pNode);
  }
}
}
