#include <sstream>

#include "emitterutils.h"
#include "indentation.h"  // IWYU pragma: keep
#include "yaml-cpp/emitter.h"
#include "yaml-cpp/emitterdef.h"
#include "yaml-cpp/emittermanip.h"
#include "yaml-cpp/exceptions.h"  // IWYU pragma: keep

namespace YAML {
class Binary;
struct _Null;

Emitter::Emitter() : m_pState(new EmitterState) {}

Emitter::Emitter(std::ostream& stream)
    : m_pState(new EmitterState), m_stream(stream) {}

Emitter::~Emitter() {}

const char* Emitter::c_str() const { return m_stream.str(); }

std::size_t Emitter::size() const { return m_stream.pos(); }

// state checking
bool Emitter::good() const { return m_pState->good(); }

const std::string Emitter::GetLastError() const {
  return m_pState->GetLastError();
}

// global setters
bool Emitter::SetOutputCharset(EMITTER_MANIP value) {
  return m_pState->SetOutputCharset(value, FmtScope::Global);
}

bool Emitter::SetStringFormat(EMITTER_MANIP value) {
  return m_pState->SetStringFormat(value, FmtScope::Global);
}

bool Emitter::SetBoolFormat(EMITTER_MANIP value) {
  bool ok = false;
  if (m_pState->SetBoolFormat(value, FmtScope::Global))
    ok = true;
  if (m_pState->SetBoolCaseFormat(value, FmtScope::Global))
    ok = true;
  if (m_pState->SetBoolLengthFormat(value, FmtScope::Global))
    ok = true;
  return ok;
}

bool Emitter::SetIntBase(EMITTER_MANIP value) {
  return m_pState->SetIntFormat(value, FmtScope::Global);
}

bool Emitter::SetSeqFormat(EMITTER_MANIP value) {
  return m_pState->SetFlowType(GroupType::Seq, value, FmtScope::Global);
}

bool Emitter::SetMapFormat(EMITTER_MANIP value) {
  bool ok = false;
  if (m_pState->SetFlowType(GroupType::Map, value, FmtScope::Global))
    ok = true;
  if (m_pState->SetMapKeyFormat(value, FmtScope::Global))
    ok = true;
  return ok;
}

bool Emitter::SetIndent(std::size_t n) {
  return m_pState->SetIndent(n, FmtScope::Global);
}

bool Emitter::SetPreCommentIndent(std::size_t n) {
  return m_pState->SetPreCommentIndent(n, FmtScope::Global);
}

bool Emitter::SetPostCommentIndent(std::size_t n) {
  return m_pState->SetPostCommentIndent(n, FmtScope::Global);
}

bool Emitter::SetFloatPrecision(std::size_t n) {
  return m_pState->SetFloatPrecision(n, FmtScope::Global);
}

bool Emitter::SetDoublePrecision(std::size_t n) {
  return m_pState->SetDoublePrecision(n, FmtScope::Global);
}

// SetLocalValue
// . Either start/end a group, or set a modifier locally
Emitter& Emitter::SetLocalValue(EMITTER_MANIP value) {
  if (!good())
    return *this;

  switch (value) {
    case BeginDoc:
      EmitBeginDoc();
      break;
    case EndDoc:
      EmitEndDoc();
      break;
    case BeginSeq:
      EmitBeginSeq();
      break;
    case EndSeq:
      EmitEndSeq();
      break;
    case BeginMap:
      EmitBeginMap();
      break;
    case EndMap:
      EmitEndMap();
      break;
    case Key:
    case Value:
      // deprecated (these can be deduced by the parity of nodes in a map)
      break;
    case TagByKind:
      EmitKindTag();
      break;
    case Newline:
      EmitNewline();
      break;
    default:
      m_pState->SetLocalValue(value);
      break;
  }
  return *this;
}

Emitter& Emitter::SetLocalIndent(const _Indent& indent) {
  m_pState->SetIndent(indent.value, FmtScope::Local);
  return *this;
}

Emitter& Emitter::SetLocalPrecision(const _Precision& precision) {
  if (precision.floatPrecision >= 0)
    m_pState->SetFloatPrecision(precision.floatPrecision, FmtScope::Local);
  if (precision.doublePrecision >= 0)
    m_pState->SetDoublePrecision(precision.doublePrecision, FmtScope::Local);
  return *this;
}

// EmitBeginDoc
void Emitter::EmitBeginDoc() {
  if (!good())
    return;

  if (m_pState->CurGroupType() != GroupType::NoType) {
    m_pState->SetError("Unexpected begin document");
    return;
  }

  if (m_pState->HasAnchor() || m_pState->HasTag()) {
    m_pState->SetError("Unexpected begin document");
    return;
  }

  if (m_stream.col() > 0)
    m_stream << "\n";
  m_stream << "---\n";

  m_pState->StartedDoc();
}

// EmitEndDoc
void Emitter::EmitEndDoc() {
  if (!good())
    return;

  if (m_pState->CurGroupType() != GroupType::NoType) {
    m_pState->SetError("Unexpected begin document");
    return;
  }

  if (m_pState->HasAnchor() || m_pState->HasTag()) {
    m_pState->SetError("Unexpected begin document");
    return;
  }

  if (m_stream.col() > 0)
    m_stream << "\n";
  m_stream << "...\n";
}

// EmitBeginSeq
void Emitter::EmitBeginSeq() {
  if (!good())
    return;

  PrepareNode(m_pState->NextGroupType(GroupType::Seq));

  m_pState->StartedGroup(GroupType::Seq);
}

// EmitEndSeq
void Emitter::EmitEndSeq() {
  if (!good())
    return;

  if (m_pState->CurGroupChildCount() == 0)
    m_pState->ForceFlow();

  if (m_pState->CurGroupFlowType() == FlowType::Flow) {
    if (m_stream.comment())
      m_stream << "\n";
    m_stream << IndentTo(m_pState->CurIndent());
    if (m_pState->CurGroupChildCount() == 0)
      m_stream << "[";
    m_stream << "]";
  }

  m_pState->EndedGroup(GroupType::Seq);
}

// EmitBeginMap
void Emitter::EmitBeginMap() {
  if (!good())
    return;

  PrepareNode(m_pState->NextGroupType(GroupType::Map));

  m_pState->StartedGroup(GroupType::Map);
}

// EmitEndMap
void Emitter::EmitEndMap() {
  if (!good())
    return;

  if (m_pState->CurGroupChildCount() == 0)
    m_pState->ForceFlow();

  if (m_pState->CurGroupFlowType() == FlowType::Flow) {
    if (m_stream.comment())
      m_stream << "\n";
    m_stream << IndentTo(m_pState->CurIndent());
    if (m_pState->CurGroupChildCount() == 0)
      m_stream << "{";
    m_stream << "}";
  }

  m_pState->EndedGroup(GroupType::Map);
}

// EmitNewline
void Emitter::EmitNewline() {
  if (!good())
    return;

  PrepareNode(EmitterNodeType::NoType);
  m_stream << "\n";
  m_pState->SetNonContent();
}

bool Emitter::CanEmitNewline() const { return true; }

// Put the stream in a state so we can simply write the next node
// E.g., if we're in a sequence, write the "- "
void Emitter::PrepareNode(EmitterNodeType::value child) {
  switch (m_pState->CurGroupNodeType()) {
    case EmitterNodeType::NoType:
      PrepareTopNode(child);
      break;
    case EmitterNodeType::FlowSeq:
      FlowSeqPrepareNode(child);
      break;
    case EmitterNodeType::BlockSeq:
      BlockSeqPrepareNode(child);
      break;
    case EmitterNodeType::FlowMap:
      FlowMapPrepareNode(child);
      break;
    case EmitterNodeType::BlockMap:
      BlockMapPrepareNode(child);
      break;
    case EmitterNodeType::Property:
    case EmitterNodeType::Scalar:
      assert(false);
      break;
  }
}

void Emitter::PrepareTopNode(EmitterNodeType::value child) {
  if (child == EmitterNodeType::NoType)
    return;

  if (m_pState->CurGroupChildCount() > 0 && m_stream.col() > 0) {
    if (child != EmitterNodeType::NoType)
      EmitBeginDoc();
  }

  switch (child) {
    case EmitterNodeType::NoType:
      break;
    case EmitterNodeType::Property:
    case EmitterNodeType::Scalar:
    case EmitterNodeType::FlowSeq:
    case EmitterNodeType::FlowMap:
      // TODO: if we were writing null, and
      // we wanted it blank, we wouldn't want a space
      SpaceOrIndentTo(m_pState->HasBegunContent(), 0);
      break;
    case EmitterNodeType::BlockSeq:
    case EmitterNodeType::BlockMap:
      if (m_pState->HasBegunNode())
        m_stream << "\n";
      break;
  }
}

void Emitter::FlowSeqPrepareNode(EmitterNodeType::value child) {
  const std::size_t lastIndent = m_pState->LastIndent();

  if (!m_pState->HasBegunNode()) {
    if (m_stream.comment())
      m_stream << "\n";
    m_stream << IndentTo(lastIndent);
    if (m_pState->CurGroupChildCount() == 0)
      m_stream << "[";
    else
      m_stream << ",";
  }

  switch (child) {
    case EmitterNodeType::NoType:
      break;
    case EmitterNodeType::Property:
    case EmitterNodeType::Scalar:
    case EmitterNodeType::FlowSeq:
    case EmitterNodeType::FlowMap:
      SpaceOrIndentTo(
          m_pState->HasBegunContent() || m_pState->CurGroupChildCount() > 0,
          lastIndent);
      break;
    case EmitterNodeType::BlockSeq:
    case EmitterNodeType::BlockMap:
      assert(false);
      break;
  }
}

void Emitter::BlockSeqPrepareNode(EmitterNodeType::value child) {
  const std::size_t curIndent = m_pState->CurIndent();
  const std::size_t nextIndent = curIndent + m_pState->CurGroupIndent();

  if (child == EmitterNodeType::NoType)
    return;

  if (!m_pState->HasBegunContent()) {
    if (m_pState->CurGroupChildCount() > 0 || m_stream.comment()) {
      m_stream << "\n";
    }
    m_stream << IndentTo(curIndent);
    m_stream << "-";
  }

  switch (child) {
    case EmitterNodeType::NoType:
      break;
    case EmitterNodeType::Property:
    case EmitterNodeType::Scalar:
    case EmitterNodeType::FlowSeq:
    case EmitterNodeType::FlowMap:
      SpaceOrIndentTo(m_pState->HasBegunContent(), nextIndent);
      break;
    case EmitterNodeType::BlockSeq:
      m_stream << "\n";
      break;
    case EmitterNodeType::BlockMap:
      if (m_pState->HasBegunContent() || m_stream.comment())
        m_stream << "\n";
      break;
  }
}

void Emitter::FlowMapPrepareNode(EmitterNodeType::value child) {
  if (m_pState->CurGroupChildCount() % 2 == 0) {
    if (m_pState->GetMapKeyFormat() == LongKey)
      m_pState->SetLongKey();

    if (m_pState->CurGroupLongKey())
      FlowMapPrepareLongKey(child);
    else
      FlowMapPrepareSimpleKey(child);
  } else {
    if (m_pState->CurGroupLongKey())
      FlowMapPrepareLongKeyValue(child);
    else
      FlowMapPrepareSimpleKeyValue(child);
  }
}

void Emitter::FlowMapPrepareLongKey(EmitterNodeType::value child) {
  const std::size_t lastIndent = m_pState->LastIndent();

  if (!m_pState->HasBegunNode()) {
    if (m_stream.comment())
      m_stream << "\n";
    m_stream << IndentTo(lastIndent);
    if (m_pState->CurGroupChildCount() == 0)
      m_stream << "{ ?";
    else
      m_stream << ", ?";
  }

  switch (child) {
    case EmitterNodeType::NoType:
      break;
    case EmitterNodeType::Property:
    case EmitterNodeType::Scalar:
    case EmitterNodeType::FlowSeq:
    case EmitterNodeType::FlowMap:
      SpaceOrIndentTo(
          m_pState->HasBegunContent() || m_pState->CurGroupChildCount() > 0,
          lastIndent);
      break;
    case EmitterNodeType::BlockSeq:
    case EmitterNodeType::BlockMap:
      assert(false);
      break;
  }
}

void Emitter::FlowMapPrepareLongKeyValue(EmitterNodeType::value child) {
  const std::size_t lastIndent = m_pState->LastIndent();

  if (!m_pState->HasBegunNode()) {
    if (m_stream.comment())
      m_stream << "\n";
    m_stream << IndentTo(lastIndent);
    m_stream << ":";
  }

  switch (child) {
    case EmitterNodeType::NoType:
      break;
    case EmitterNodeType::Property:
    case EmitterNodeType::Scalar:
    case EmitterNodeType::FlowSeq:
    case EmitterNodeType::FlowMap:
      SpaceOrIndentTo(
          m_pState->HasBegunContent() || m_pState->CurGroupChildCount() > 0,
          lastIndent);
      break;
    case EmitterNodeType::BlockSeq:
    case EmitterNodeType::BlockMap:
      assert(false);
      break;
  }
}

void Emitter::FlowMapPrepareSimpleKey(EmitterNodeType::value child) {
  const std::size_t lastIndent = m_pState->LastIndent();

  if (!m_pState->HasBegunNode()) {
    if (m_stream.comment())
      m_stream << "\n";
    m_stream << IndentTo(lastIndent);
    if (m_pState->CurGroupChildCount() == 0)
      m_stream << "{";
    else
      m_stream << ",";
  }

  switch (child) {
    case EmitterNodeType::NoType:
      break;
    case EmitterNodeType::Property:
    case EmitterNodeType::Scalar:
    case EmitterNodeType::FlowSeq:
    case EmitterNodeType::FlowMap:
      SpaceOrIndentTo(
          m_pState->HasBegunContent() || m_pState->CurGroupChildCount() > 0,
          lastIndent);
      break;
    case EmitterNodeType::BlockSeq:
    case EmitterNodeType::BlockMap:
      assert(false);
      break;
  }
}

void Emitter::FlowMapPrepareSimpleKeyValue(EmitterNodeType::value child) {
  const std::size_t lastIndent = m_pState->LastIndent();

  if (!m_pState->HasBegunNode()) {
    if (m_stream.comment())
      m_stream << "\n";
    m_stream << IndentTo(lastIndent);
    m_stream << ":";
  }

  switch (child) {
    case EmitterNodeType::NoType:
      break;
    case EmitterNodeType::Property:
    case EmitterNodeType::Scalar:
    case EmitterNodeType::FlowSeq:
    case EmitterNodeType::FlowMap:
      SpaceOrIndentTo(
          m_pState->HasBegunContent() || m_pState->CurGroupChildCount() > 0,
          lastIndent);
      break;
    case EmitterNodeType::BlockSeq:
    case EmitterNodeType::BlockMap:
      assert(false);
      break;
  }
}

void Emitter::BlockMapPrepareNode(EmitterNodeType::value child) {
  if (m_pState->CurGroupChildCount() % 2 == 0) {
    if (m_pState->GetMapKeyFormat() == LongKey)
      m_pState->SetLongKey();
    if (child == EmitterNodeType::BlockSeq ||
        child == EmitterNodeType::BlockMap)
      m_pState->SetLongKey();

    if (m_pState->CurGroupLongKey())
      BlockMapPrepareLongKey(child);
    else
      BlockMapPrepareSimpleKey(child);
  } else {
    if (m_pState->CurGroupLongKey())
      BlockMapPrepareLongKeyValue(child);
    else
      BlockMapPrepareSimpleKeyValue(child);
  }
}

void Emitter::BlockMapPrepareLongKey(EmitterNodeType::value child) {
  const std::size_t curIndent = m_pState->CurIndent();
  const std::size_t childCount = m_pState->CurGroupChildCount();

  if (child == EmitterNodeType::NoType)
    return;

  if (!m_pState->HasBegunContent()) {
    if (childCount > 0) {
      m_stream << "\n";
    }
    if (m_stream.comment()) {
      m_stream << "\n";
    }
    m_stream << IndentTo(curIndent);
    m_stream << "?";
  }

  switch (child) {
    case EmitterNodeType::NoType:
      break;
    case EmitterNodeType::Property:
    case EmitterNodeType::Scalar:
    case EmitterNodeType::FlowSeq:
    case EmitterNodeType::FlowMap:
      SpaceOrIndentTo(true, curIndent + 1);
      break;
    case EmitterNodeType::BlockSeq:
    case EmitterNodeType::BlockMap:
      break;
  }
}

void Emitter::BlockMapPrepareLongKeyValue(EmitterNodeType::value child) {
  const std::size_t curIndent = m_pState->CurIndent();

  if (child == EmitterNodeType::NoType)
    return;

  if (!m_pState->HasBegunContent()) {
    m_stream << "\n";
    m_stream << IndentTo(curIndent);
    m_stream << ":";
  }

  switch (child) {
    case EmitterNodeType::NoType:
      break;
    case EmitterNodeType::Property:
    case EmitterNodeType::Scalar:
    case EmitterNodeType::FlowSeq:
    case EmitterNodeType::FlowMap:
    case EmitterNodeType::BlockSeq:
    case EmitterNodeType::BlockMap:
      SpaceOrIndentTo(true, curIndent + 1);
      break;
  }
}

void Emitter::BlockMapPrepareSimpleKey(EmitterNodeType::value child) {
  const std::size_t curIndent = m_pState->CurIndent();
  const std::size_t childCount = m_pState->CurGroupChildCount();

  if (child == EmitterNodeType::NoType)
    return;

  if (!m_pState->HasBegunNode()) {
    if (childCount > 0) {
      m_stream << "\n";
    }
  }

  switch (child) {
    case EmitterNodeType::NoType:
      break;
    case EmitterNodeType::Property:
    case EmitterNodeType::Scalar:
    case EmitterNodeType::FlowSeq:
    case EmitterNodeType::FlowMap:
      SpaceOrIndentTo(m_pState->HasBegunContent(), curIndent);
      break;
    case EmitterNodeType::BlockSeq:
    case EmitterNodeType::BlockMap:
      break;
  }
}

void Emitter::BlockMapPrepareSimpleKeyValue(EmitterNodeType::value child) {
  const std::size_t curIndent = m_pState->CurIndent();
  const std::size_t nextIndent = curIndent + m_pState->CurGroupIndent();

  if (!m_pState->HasBegunNode()) {
    m_stream << ":";
  }

  switch (child) {
    case EmitterNodeType::NoType:
      break;
    case EmitterNodeType::Property:
    case EmitterNodeType::Scalar:
    case EmitterNodeType::FlowSeq:
    case EmitterNodeType::FlowMap:
      SpaceOrIndentTo(true, nextIndent);
      break;
    case EmitterNodeType::BlockSeq:
    case EmitterNodeType::BlockMap:
      m_stream << "\n";
      break;
  }
}

// SpaceOrIndentTo
// . Prepares for some more content by proper spacing
void Emitter::SpaceOrIndentTo(bool requireSpace, std::size_t indent) {
  if (m_stream.comment())
    m_stream << "\n";
  if (m_stream.col() > 0 && requireSpace)
    m_stream << " ";
  m_stream << IndentTo(indent);
}

void Emitter::PrepareIntegralStream(std::stringstream& stream) const {

  switch (m_pState->GetIntFormat()) {
    case Dec:
      stream << std::dec;
      break;
    case Hex:
      stream << "0x";
      stream << std::hex;
      break;
    case Oct:
      stream << "0";
      stream << std::oct;
      break;
    default:
      assert(false);
  }
}

void Emitter::StartedScalar() { m_pState->StartedScalar(); }

// *******************************************************************************************
// overloads of Write

Emitter& Emitter::Write(const std::string& str) {
  if (!good())
    return *this;

  const bool escapeNonAscii = m_pState->GetOutputCharset() == EscapeNonAscii;
  const StringFormat::value strFormat =
      Utils::ComputeStringFormat(str, m_pState->GetStringFormat(),
                                 m_pState->CurGroupFlowType(), escapeNonAscii);

  if (strFormat == StringFormat::Literal)
    m_pState->SetMapKeyFormat(YAML::LongKey, FmtScope::Local);

  PrepareNode(EmitterNodeType::Scalar);

  switch (strFormat) {
    case StringFormat::Plain:
      m_stream << str;
      break;
    case StringFormat::SingleQuoted:
      Utils::WriteSingleQuotedString(m_stream, str);
      break;
    case StringFormat::DoubleQuoted:
      Utils::WriteDoubleQuotedString(m_stream, str, escapeNonAscii);
      break;
    case StringFormat::Literal:
      Utils::WriteLiteralString(m_stream, str,
                                m_pState->CurIndent() + m_pState->GetIndent());
      break;
  }

  StartedScalar();

  return *this;
}

std::size_t Emitter::GetFloatPrecision() const {
  return m_pState->GetFloatPrecision();
}

std::size_t Emitter::GetDoublePrecision() const {
  return m_pState->GetDoublePrecision();
}

const char* Emitter::ComputeFullBoolName(bool b) const {
  const EMITTER_MANIP mainFmt = (m_pState->GetBoolLengthFormat() == ShortBool
                                     ? YesNoBool
                                     : m_pState->GetBoolFormat());
  const EMITTER_MANIP caseFmt = m_pState->GetBoolCaseFormat();
  switch (mainFmt) {
    case YesNoBool:
      switch (caseFmt) {
        case UpperCase:
          return b ? "YES" : "NO";
        case CamelCase:
          return b ? "Yes" : "No";
        case LowerCase:
          return b ? "yes" : "no";
        default:
          break;
      }
      break;
    case OnOffBool:
      switch (caseFmt) {
        case UpperCase:
          return b ? "ON" : "OFF";
        case CamelCase:
          return b ? "On" : "Off";
        case LowerCase:
          return b ? "on" : "off";
        default:
          break;
      }
      break;
    case TrueFalseBool:
      switch (caseFmt) {
        case UpperCase:
          return b ? "TRUE" : "FALSE";
        case CamelCase:
          return b ? "True" : "False";
        case LowerCase:
          return b ? "true" : "false";
        default:
          break;
      }
      break;
    default:
      break;
  }
  return b ? "y" : "n";  // should never get here, but it can't hurt to give
                         // these answers
}

Emitter& Emitter::Write(bool b) {
  if (!good())
    return *this;

  PrepareNode(EmitterNodeType::Scalar);

  const char* name = ComputeFullBoolName(b);
  if (m_pState->GetBoolLengthFormat() == ShortBool)
    m_stream << name[0];
  else
    m_stream << name;

  StartedScalar();

  return *this;
}

Emitter& Emitter::Write(char ch) {
  if (!good())
    return *this;

  PrepareNode(EmitterNodeType::Scalar);
  Utils::WriteChar(m_stream, ch);
  StartedScalar();

  return *this;
}

Emitter& Emitter::Write(const _Alias& alias) {
  if (!good())
    return *this;

  if (m_pState->HasAnchor() || m_pState->HasTag()) {
    m_pState->SetError(ErrorMsg::INVALID_ALIAS);
    return *this;
  }

  PrepareNode(EmitterNodeType::Scalar);

  if (!Utils::WriteAlias(m_stream, alias.content)) {
    m_pState->SetError(ErrorMsg::INVALID_ALIAS);
    return *this;
  }

  StartedScalar();

  return *this;
}

Emitter& Emitter::Write(const _Anchor& anchor) {
  if (!good())
    return *this;

  if (m_pState->HasAnchor()) {
    m_pState->SetError(ErrorMsg::INVALID_ANCHOR);
    return *this;
  }

  PrepareNode(EmitterNodeType::Property);

  if (!Utils::WriteAnchor(m_stream, anchor.content)) {
    m_pState->SetError(ErrorMsg::INVALID_ANCHOR);
    return *this;
  }

  m_pState->SetAnchor();

  return *this;
}

Emitter& Emitter::Write(const _Tag& tag) {
  if (!good())
    return *this;

  if (m_pState->HasTag()) {
    m_pState->SetError(ErrorMsg::INVALID_TAG);
    return *this;
  }

  PrepareNode(EmitterNodeType::Property);

  bool success = false;
  if (tag.type == _Tag::Type::Verbatim)
    success = Utils::WriteTag(m_stream, tag.content, true);
  else if (tag.type == _Tag::Type::PrimaryHandle)
    success = Utils::WriteTag(m_stream, tag.content, false);
  else
    success = Utils::WriteTagWithPrefix(m_stream, tag.prefix, tag.content);

  if (!success) {
    m_pState->SetError(ErrorMsg::INVALID_TAG);
    return *this;
  }

  m_pState->SetTag();

  return *this;
}

void Emitter::EmitKindTag() { Write(LocalTag("")); }

Emitter& Emitter::Write(const _Comment& comment) {
  if (!good())
    return *this;

  PrepareNode(EmitterNodeType::NoType);

  if (m_stream.col() > 0)
    m_stream << Indentation(m_pState->GetPreCommentIndent());
  Utils::WriteComment(m_stream, comment.content,
                      m_pState->GetPostCommentIndent());

  m_pState->SetNonContent();

  return *this;
}

Emitter& Emitter::Write(const _Null& /*null*/) {
  if (!good())
    return *this;

  PrepareNode(EmitterNodeType::Scalar);

  m_stream << "~";

  StartedScalar();

  return *this;
}

Emitter& Emitter::Write(const Binary& binary) {
  Write(SecondaryTag("binary"));

  if (!good())
    return *this;

  PrepareNode(EmitterNodeType::Scalar);
  Utils::WriteBinary(m_stream, binary);
  StartedScalar();

  return *this;
}
}
