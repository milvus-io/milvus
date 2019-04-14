#ifndef EMITTERSTATE_H_62B23520_7C8E_11DE_8A39_0800200C9A66
#define EMITTERSTATE_H_62B23520_7C8E_11DE_8A39_0800200C9A66

#if defined(_MSC_VER) ||                                            \
    (defined(__GNUC__) && (__GNUC__ == 3 && __GNUC_MINOR__ >= 4) || \
     (__GNUC__ >= 4))  // GCC supports "pragma once" correctly since 3.4
#pragma once
#endif

#include "setting.h"
#include "yaml-cpp/emitterdef.h"
#include "yaml-cpp/emittermanip.h"

#include <cassert>
#include <memory>
#include <stack>
#include <stdexcept>
#include <vector>

namespace YAML {
struct FmtScope {
  enum value { Local, Global };
};
struct GroupType {
  enum value { NoType, Seq, Map };
};
struct FlowType {
  enum value { NoType, Flow, Block };
};

class EmitterState {
 public:
  EmitterState();
  ~EmitterState();

  // basic state checking
  bool good() const { return m_isGood; }
  const std::string GetLastError() const { return m_lastError; }
  void SetError(const std::string& error) {
    m_isGood = false;
    m_lastError = error;
  }

  // node handling
  void SetAnchor();
  void SetTag();
  void SetNonContent();
  void SetLongKey();
  void ForceFlow();
  void StartedDoc();
  void EndedDoc();
  void StartedScalar();
  void StartedGroup(GroupType::value type);
  void EndedGroup(GroupType::value type);

  EmitterNodeType::value NextGroupType(GroupType::value type) const;
  EmitterNodeType::value CurGroupNodeType() const;

  GroupType::value CurGroupType() const;
  FlowType::value CurGroupFlowType() const;
  std::size_t CurGroupIndent() const;
  std::size_t CurGroupChildCount() const;
  bool CurGroupLongKey() const;

  std::size_t LastIndent() const;
  std::size_t CurIndent() const { return m_curIndent; }
  bool HasAnchor() const { return m_hasAnchor; }
  bool HasTag() const { return m_hasTag; }
  bool HasBegunNode() const {
    return m_hasAnchor || m_hasTag || m_hasNonContent;
  }
  bool HasBegunContent() const { return m_hasAnchor || m_hasTag; }

  void ClearModifiedSettings();

  // formatters
  void SetLocalValue(EMITTER_MANIP value);

  bool SetOutputCharset(EMITTER_MANIP value, FmtScope::value scope);
  EMITTER_MANIP GetOutputCharset() const { return m_charset.get(); }

  bool SetStringFormat(EMITTER_MANIP value, FmtScope::value scope);
  EMITTER_MANIP GetStringFormat() const { return m_strFmt.get(); }

  bool SetBoolFormat(EMITTER_MANIP value, FmtScope::value scope);
  EMITTER_MANIP GetBoolFormat() const { return m_boolFmt.get(); }

  bool SetBoolLengthFormat(EMITTER_MANIP value, FmtScope::value scope);
  EMITTER_MANIP GetBoolLengthFormat() const { return m_boolLengthFmt.get(); }

  bool SetBoolCaseFormat(EMITTER_MANIP value, FmtScope::value scope);
  EMITTER_MANIP GetBoolCaseFormat() const { return m_boolCaseFmt.get(); }

  bool SetIntFormat(EMITTER_MANIP value, FmtScope::value scope);
  EMITTER_MANIP GetIntFormat() const { return m_intFmt.get(); }

  bool SetIndent(std::size_t value, FmtScope::value scope);
  std::size_t GetIndent() const { return m_indent.get(); }

  bool SetPreCommentIndent(std::size_t value, FmtScope::value scope);
  std::size_t GetPreCommentIndent() const { return m_preCommentIndent.get(); }
  bool SetPostCommentIndent(std::size_t value, FmtScope::value scope);
  std::size_t GetPostCommentIndent() const { return m_postCommentIndent.get(); }

  bool SetFlowType(GroupType::value groupType, EMITTER_MANIP value,
                   FmtScope::value scope);
  EMITTER_MANIP GetFlowType(GroupType::value groupType) const;

  bool SetMapKeyFormat(EMITTER_MANIP value, FmtScope::value scope);
  EMITTER_MANIP GetMapKeyFormat() const { return m_mapKeyFmt.get(); }

  bool SetFloatPrecision(std::size_t value, FmtScope::value scope);
  std::size_t GetFloatPrecision() const { return m_floatPrecision.get(); }
  bool SetDoublePrecision(std::size_t value, FmtScope::value scope);
  std::size_t GetDoublePrecision() const { return m_doublePrecision.get(); }

 private:
  template <typename T>
  void _Set(Setting<T>& fmt, T value, FmtScope::value scope);

  void StartedNode();

 private:
  // basic state ok?
  bool m_isGood;
  std::string m_lastError;

  // other state
  Setting<EMITTER_MANIP> m_charset;
  Setting<EMITTER_MANIP> m_strFmt;
  Setting<EMITTER_MANIP> m_boolFmt;
  Setting<EMITTER_MANIP> m_boolLengthFmt;
  Setting<EMITTER_MANIP> m_boolCaseFmt;
  Setting<EMITTER_MANIP> m_intFmt;
  Setting<std::size_t> m_indent;
  Setting<std::size_t> m_preCommentIndent, m_postCommentIndent;
  Setting<EMITTER_MANIP> m_seqFmt;
  Setting<EMITTER_MANIP> m_mapFmt;
  Setting<EMITTER_MANIP> m_mapKeyFmt;
  Setting<std::size_t> m_floatPrecision;
  Setting<std::size_t> m_doublePrecision;

  SettingChanges m_modifiedSettings;
  SettingChanges m_globalModifiedSettings;

  struct Group {
    explicit Group(GroupType::value type_)
        : type(type_), indent(0), childCount(0), longKey(false) {}

    GroupType::value type;
    FlowType::value flowType;
    std::size_t indent;
    std::size_t childCount;
    bool longKey;

    SettingChanges modifiedSettings;

    EmitterNodeType::value NodeType() const {
      if (type == GroupType::Seq) {
        if (flowType == FlowType::Flow)
          return EmitterNodeType::FlowSeq;
        else
          return EmitterNodeType::BlockSeq;
      } else {
        if (flowType == FlowType::Flow)
          return EmitterNodeType::FlowMap;
        else
          return EmitterNodeType::BlockMap;
      }

      // can't get here
      assert(false);
      return EmitterNodeType::NoType;
    }
  };

  std::vector<std::unique_ptr<Group>> m_groups;
  std::size_t m_curIndent;
  bool m_hasAnchor;
  bool m_hasTag;
  bool m_hasNonContent;
  std::size_t m_docCount;
};

template <typename T>
void EmitterState::_Set(Setting<T>& fmt, T value, FmtScope::value scope) {
  switch (scope) {
    case FmtScope::Local:
      m_modifiedSettings.push(fmt.set(value));
      break;
    case FmtScope::Global:
      fmt.set(value);
      m_globalModifiedSettings.push(
          fmt.set(value));  // this pushes an identity set, so when we restore,
      // it restores to the value here, and not the previous one
      break;
    default:
      assert(false);
  }
}
}

#endif  // EMITTERSTATE_H_62B23520_7C8E_11DE_8A39_0800200C9A66
