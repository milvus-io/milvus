#include <algorithm>
#include <cstdio>
#include <sstream>

#include "collectionstack.h"  // IWYU pragma: keep
#include "scanner.h"
#include "singledocparser.h"
#include "tag.h"
#include "token.h"
#include "yaml-cpp/emitterstyle.h"
#include "yaml-cpp/eventhandler.h"
#include "yaml-cpp/exceptions.h"  // IWYU pragma: keep
#include "yaml-cpp/mark.h"
#include "yaml-cpp/null.h"

namespace YAML {
SingleDocParser::SingleDocParser(Scanner& scanner, const Directives& directives)
    : m_scanner(scanner),
      m_directives(directives),
      m_pCollectionStack(new CollectionStack),
      m_curAnchor(0) {}

SingleDocParser::~SingleDocParser() {}

// HandleDocument
// . Handles the next document
// . Throws a ParserException on error.
void SingleDocParser::HandleDocument(EventHandler& eventHandler) {
  assert(!m_scanner.empty());  // guaranteed that there are tokens
  assert(!m_curAnchor);

  eventHandler.OnDocumentStart(m_scanner.peek().mark);

  // eat doc start
  if (m_scanner.peek().type == Token::DOC_START)
    m_scanner.pop();

  // recurse!
  HandleNode(eventHandler);

  eventHandler.OnDocumentEnd();

  // and finally eat any doc ends we see
  while (!m_scanner.empty() && m_scanner.peek().type == Token::DOC_END)
    m_scanner.pop();
}

void SingleDocParser::HandleNode(EventHandler& eventHandler) {
  // an empty node *is* a possibility
  if (m_scanner.empty()) {
    eventHandler.OnNull(m_scanner.mark(), NullAnchor);
    return;
  }

  // save location
  Mark mark = m_scanner.peek().mark;

  // special case: a value node by itself must be a map, with no header
  if (m_scanner.peek().type == Token::VALUE) {
    eventHandler.OnMapStart(mark, "?", NullAnchor, EmitterStyle::Default);
    HandleMap(eventHandler);
    eventHandler.OnMapEnd();
    return;
  }

  // special case: an alias node
  if (m_scanner.peek().type == Token::ALIAS) {
    eventHandler.OnAlias(mark, LookupAnchor(mark, m_scanner.peek().value));
    m_scanner.pop();
    return;
  }

  std::string tag;
  anchor_t anchor;
  ParseProperties(tag, anchor);

  const Token& token = m_scanner.peek();

  if (token.type == Token::PLAIN_SCALAR && IsNullString(token.value)) {
    eventHandler.OnNull(mark, anchor);
    m_scanner.pop();
    return;
  }

  // add non-specific tags
  if (tag.empty())
    tag = (token.type == Token::NON_PLAIN_SCALAR ? "!" : "?");

  // now split based on what kind of node we should be
  switch (token.type) {
    case Token::PLAIN_SCALAR:
    case Token::NON_PLAIN_SCALAR:
      eventHandler.OnScalar(mark, tag, anchor, token.value);
      m_scanner.pop();
      return;
    case Token::FLOW_SEQ_START:
      eventHandler.OnSequenceStart(mark, tag, anchor, EmitterStyle::Flow);
      HandleSequence(eventHandler);
      eventHandler.OnSequenceEnd();
      return;
    case Token::BLOCK_SEQ_START:
      eventHandler.OnSequenceStart(mark, tag, anchor, EmitterStyle::Block);
      HandleSequence(eventHandler);
      eventHandler.OnSequenceEnd();
      return;
    case Token::FLOW_MAP_START:
      eventHandler.OnMapStart(mark, tag, anchor, EmitterStyle::Flow);
      HandleMap(eventHandler);
      eventHandler.OnMapEnd();
      return;
    case Token::BLOCK_MAP_START:
      eventHandler.OnMapStart(mark, tag, anchor, EmitterStyle::Block);
      HandleMap(eventHandler);
      eventHandler.OnMapEnd();
      return;
    case Token::KEY:
      // compact maps can only go in a flow sequence
      if (m_pCollectionStack->GetCurCollectionType() ==
          CollectionType::FlowSeq) {
        eventHandler.OnMapStart(mark, tag, anchor, EmitterStyle::Flow);
        HandleMap(eventHandler);
        eventHandler.OnMapEnd();
        return;
      }
      break;
    default:
      break;
  }

  if (tag == "?")
    eventHandler.OnNull(mark, anchor);
  else
    eventHandler.OnScalar(mark, tag, anchor, "");
}

void SingleDocParser::HandleSequence(EventHandler& eventHandler) {
  // split based on start token
  switch (m_scanner.peek().type) {
    case Token::BLOCK_SEQ_START:
      HandleBlockSequence(eventHandler);
      break;
    case Token::FLOW_SEQ_START:
      HandleFlowSequence(eventHandler);
      break;
    default:
      break;
  }
}

void SingleDocParser::HandleBlockSequence(EventHandler& eventHandler) {
  // eat start token
  m_scanner.pop();
  m_pCollectionStack->PushCollectionType(CollectionType::BlockSeq);

  while (1) {
    if (m_scanner.empty())
      throw ParserException(m_scanner.mark(), ErrorMsg::END_OF_SEQ);

    Token token = m_scanner.peek();
    if (token.type != Token::BLOCK_ENTRY && token.type != Token::BLOCK_SEQ_END)
      throw ParserException(token.mark, ErrorMsg::END_OF_SEQ);

    m_scanner.pop();
    if (token.type == Token::BLOCK_SEQ_END)
      break;

    // check for null
    if (!m_scanner.empty()) {
      const Token& token = m_scanner.peek();
      if (token.type == Token::BLOCK_ENTRY ||
          token.type == Token::BLOCK_SEQ_END) {
        eventHandler.OnNull(token.mark, NullAnchor);
        continue;
      }
    }

    HandleNode(eventHandler);
  }

  m_pCollectionStack->PopCollectionType(CollectionType::BlockSeq);
}

void SingleDocParser::HandleFlowSequence(EventHandler& eventHandler) {
  // eat start token
  m_scanner.pop();
  m_pCollectionStack->PushCollectionType(CollectionType::FlowSeq);

  while (1) {
    if (m_scanner.empty())
      throw ParserException(m_scanner.mark(), ErrorMsg::END_OF_SEQ_FLOW);

    // first check for end
    if (m_scanner.peek().type == Token::FLOW_SEQ_END) {
      m_scanner.pop();
      break;
    }

    // then read the node
    HandleNode(eventHandler);

    if (m_scanner.empty())
      throw ParserException(m_scanner.mark(), ErrorMsg::END_OF_SEQ_FLOW);

    // now eat the separator (or could be a sequence end, which we ignore - but
    // if it's neither, then it's a bad node)
    Token& token = m_scanner.peek();
    if (token.type == Token::FLOW_ENTRY)
      m_scanner.pop();
    else if (token.type != Token::FLOW_SEQ_END)
      throw ParserException(token.mark, ErrorMsg::END_OF_SEQ_FLOW);
  }

  m_pCollectionStack->PopCollectionType(CollectionType::FlowSeq);
}

void SingleDocParser::HandleMap(EventHandler& eventHandler) {
  // split based on start token
  switch (m_scanner.peek().type) {
    case Token::BLOCK_MAP_START:
      HandleBlockMap(eventHandler);
      break;
    case Token::FLOW_MAP_START:
      HandleFlowMap(eventHandler);
      break;
    case Token::KEY:
      HandleCompactMap(eventHandler);
      break;
    case Token::VALUE:
      HandleCompactMapWithNoKey(eventHandler);
      break;
    default:
      break;
  }
}

void SingleDocParser::HandleBlockMap(EventHandler& eventHandler) {
  // eat start token
  m_scanner.pop();
  m_pCollectionStack->PushCollectionType(CollectionType::BlockMap);

  while (1) {
    if (m_scanner.empty())
      throw ParserException(m_scanner.mark(), ErrorMsg::END_OF_MAP);

    Token token = m_scanner.peek();
    if (token.type != Token::KEY && token.type != Token::VALUE &&
        token.type != Token::BLOCK_MAP_END)
      throw ParserException(token.mark, ErrorMsg::END_OF_MAP);

    if (token.type == Token::BLOCK_MAP_END) {
      m_scanner.pop();
      break;
    }

    // grab key (if non-null)
    if (token.type == Token::KEY) {
      m_scanner.pop();
      HandleNode(eventHandler);
    } else {
      eventHandler.OnNull(token.mark, NullAnchor);
    }

    // now grab value (optional)
    if (!m_scanner.empty() && m_scanner.peek().type == Token::VALUE) {
      m_scanner.pop();
      HandleNode(eventHandler);
    } else {
      eventHandler.OnNull(token.mark, NullAnchor);
    }
  }

  m_pCollectionStack->PopCollectionType(CollectionType::BlockMap);
}

void SingleDocParser::HandleFlowMap(EventHandler& eventHandler) {
  // eat start token
  m_scanner.pop();
  m_pCollectionStack->PushCollectionType(CollectionType::FlowMap);

  while (1) {
    if (m_scanner.empty())
      throw ParserException(m_scanner.mark(), ErrorMsg::END_OF_MAP_FLOW);

    Token& token = m_scanner.peek();
    const Mark mark = token.mark;
    // first check for end
    if (token.type == Token::FLOW_MAP_END) {
      m_scanner.pop();
      break;
    }

    // grab key (if non-null)
    if (token.type == Token::KEY) {
      m_scanner.pop();
      HandleNode(eventHandler);
    } else {
      eventHandler.OnNull(mark, NullAnchor);
    }

    // now grab value (optional)
    if (!m_scanner.empty() && m_scanner.peek().type == Token::VALUE) {
      m_scanner.pop();
      HandleNode(eventHandler);
    } else {
      eventHandler.OnNull(mark, NullAnchor);
    }

    if (m_scanner.empty())
      throw ParserException(m_scanner.mark(), ErrorMsg::END_OF_MAP_FLOW);

    // now eat the separator (or could be a map end, which we ignore - but if
    // it's neither, then it's a bad node)
    Token& nextToken = m_scanner.peek();
    if (nextToken.type == Token::FLOW_ENTRY)
      m_scanner.pop();
    else if (nextToken.type != Token::FLOW_MAP_END)
      throw ParserException(nextToken.mark, ErrorMsg::END_OF_MAP_FLOW);
  }

  m_pCollectionStack->PopCollectionType(CollectionType::FlowMap);
}

// . Single "key: value" pair in a flow sequence
void SingleDocParser::HandleCompactMap(EventHandler& eventHandler) {
  m_pCollectionStack->PushCollectionType(CollectionType::CompactMap);

  // grab key
  Mark mark = m_scanner.peek().mark;
  m_scanner.pop();
  HandleNode(eventHandler);

  // now grab value (optional)
  if (!m_scanner.empty() && m_scanner.peek().type == Token::VALUE) {
    m_scanner.pop();
    HandleNode(eventHandler);
  } else {
    eventHandler.OnNull(mark, NullAnchor);
  }

  m_pCollectionStack->PopCollectionType(CollectionType::CompactMap);
}

// . Single ": value" pair in a flow sequence
void SingleDocParser::HandleCompactMapWithNoKey(EventHandler& eventHandler) {
  m_pCollectionStack->PushCollectionType(CollectionType::CompactMap);

  // null key
  eventHandler.OnNull(m_scanner.peek().mark, NullAnchor);

  // grab value
  m_scanner.pop();
  HandleNode(eventHandler);

  m_pCollectionStack->PopCollectionType(CollectionType::CompactMap);
}

// ParseProperties
// . Grabs any tag or anchor tokens and deals with them.
void SingleDocParser::ParseProperties(std::string& tag, anchor_t& anchor) {
  tag.clear();
  anchor = NullAnchor;

  while (1) {
    if (m_scanner.empty())
      return;

    switch (m_scanner.peek().type) {
      case Token::TAG:
        ParseTag(tag);
        break;
      case Token::ANCHOR:
        ParseAnchor(anchor);
        break;
      default:
        return;
    }
  }
}

void SingleDocParser::ParseTag(std::string& tag) {
  Token& token = m_scanner.peek();
  if (!tag.empty())
    throw ParserException(token.mark, ErrorMsg::MULTIPLE_TAGS);

  Tag tagInfo(token);
  tag = tagInfo.Translate(m_directives);
  m_scanner.pop();
}

void SingleDocParser::ParseAnchor(anchor_t& anchor) {
  Token& token = m_scanner.peek();
  if (anchor)
    throw ParserException(token.mark, ErrorMsg::MULTIPLE_ANCHORS);

  anchor = RegisterAnchor(token.value);
  m_scanner.pop();
}

anchor_t SingleDocParser::RegisterAnchor(const std::string& name) {
  if (name.empty())
    return NullAnchor;

  return m_anchors[name] = ++m_curAnchor;
}

anchor_t SingleDocParser::LookupAnchor(const Mark& mark,
                                       const std::string& name) const {
  Anchors::const_iterator it = m_anchors.find(name);
  if (it == m_anchors.end())
    throw ParserException(mark, ErrorMsg::UNKNOWN_ANCHOR);

  return it->second;
}
}
