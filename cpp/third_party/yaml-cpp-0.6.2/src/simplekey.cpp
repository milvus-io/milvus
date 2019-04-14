#include "scanner.h"
#include "token.h"

namespace YAML {
struct Mark;

Scanner::SimpleKey::SimpleKey(const Mark& mark_, std::size_t flowLevel_)
    : mark(mark_), flowLevel(flowLevel_), pIndent(0), pMapStart(0), pKey(0) {}

void Scanner::SimpleKey::Validate() {
  // Note: pIndent will *not* be garbage here;
  //       we "garbage collect" them so we can
  //       always refer to them
  if (pIndent)
    pIndent->status = IndentMarker::VALID;
  if (pMapStart)
    pMapStart->status = Token::VALID;
  if (pKey)
    pKey->status = Token::VALID;
}

void Scanner::SimpleKey::Invalidate() {
  if (pIndent)
    pIndent->status = IndentMarker::INVALID;
  if (pMapStart)
    pMapStart->status = Token::INVALID;
  if (pKey)
    pKey->status = Token::INVALID;
}

// CanInsertPotentialSimpleKey
bool Scanner::CanInsertPotentialSimpleKey() const {
  if (!m_simpleKeyAllowed)
    return false;

  return !ExistsActiveSimpleKey();
}

// ExistsActiveSimpleKey
// . Returns true if there's a potential simple key at our flow level
//   (there's allowed at most one per flow level, i.e., at the start of the flow
// start token)
bool Scanner::ExistsActiveSimpleKey() const {
  if (m_simpleKeys.empty())
    return false;

  const SimpleKey& key = m_simpleKeys.top();
  return key.flowLevel == GetFlowLevel();
}

// InsertPotentialSimpleKey
// . If we can, add a potential simple key to the queue,
//   and save it on a stack.
void Scanner::InsertPotentialSimpleKey() {
  if (!CanInsertPotentialSimpleKey())
    return;

  SimpleKey key(INPUT.mark(), GetFlowLevel());

  // first add a map start, if necessary
  if (InBlockContext()) {
    key.pIndent = PushIndentTo(INPUT.column(), IndentMarker::MAP);
    if (key.pIndent) {
      key.pIndent->status = IndentMarker::UNKNOWN;
      key.pMapStart = key.pIndent->pStartToken;
      key.pMapStart->status = Token::UNVERIFIED;
    }
  }

  // then add the (now unverified) key
  m_tokens.push(Token(Token::KEY, INPUT.mark()));
  key.pKey = &m_tokens.back();
  key.pKey->status = Token::UNVERIFIED;

  m_simpleKeys.push(key);
}

// InvalidateSimpleKey
// . Automatically invalidate the simple key in our flow level
void Scanner::InvalidateSimpleKey() {
  if (m_simpleKeys.empty())
    return;

  // grab top key
  SimpleKey& key = m_simpleKeys.top();
  if (key.flowLevel != GetFlowLevel())
    return;

  key.Invalidate();
  m_simpleKeys.pop();
}

// VerifySimpleKey
// . Determines whether the latest simple key to be added is valid,
//   and if so, makes it valid.
bool Scanner::VerifySimpleKey() {
  if (m_simpleKeys.empty())
    return false;

  // grab top key
  SimpleKey key = m_simpleKeys.top();

  // only validate if we're in the correct flow level
  if (key.flowLevel != GetFlowLevel())
    return false;

  m_simpleKeys.pop();

  bool isValid = true;

  // needs to be less than 1024 characters and inline
  if (INPUT.line() != key.mark.line || INPUT.pos() - key.mark.pos > 1024)
    isValid = false;

  // invalidate key
  if (isValid)
    key.Validate();
  else
    key.Invalidate();

  return isValid;
}

void Scanner::PopAllSimpleKeys() {
  while (!m_simpleKeys.empty())
    m_simpleKeys.pop();
}
}
