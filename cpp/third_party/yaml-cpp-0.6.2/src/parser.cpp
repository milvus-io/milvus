#include <cstdio>
#include <sstream>

#include "directives.h"  // IWYU pragma: keep
#include "scanner.h"     // IWYU pragma: keep
#include "singledocparser.h"
#include "token.h"
#include "yaml-cpp/exceptions.h"  // IWYU pragma: keep
#include "yaml-cpp/parser.h"

namespace YAML {
class EventHandler;

Parser::Parser() {}

Parser::Parser(std::istream& in) { Load(in); }

Parser::~Parser() {}

Parser::operator bool() const {
  return m_pScanner.get() && !m_pScanner->empty();
}

void Parser::Load(std::istream& in) {
  m_pScanner.reset(new Scanner(in));
  m_pDirectives.reset(new Directives);
}

bool Parser::HandleNextDocument(EventHandler& eventHandler) {
  if (!m_pScanner.get())
    return false;

  ParseDirectives();
  if (m_pScanner->empty()) {
    return false;
  }

  SingleDocParser sdp(*m_pScanner, *m_pDirectives);
  sdp.HandleDocument(eventHandler);
  return true;
}

void Parser::ParseDirectives() {
  bool readDirective = false;

  while (1) {
    if (m_pScanner->empty()) {
      break;
    }

    Token& token = m_pScanner->peek();
    if (token.type != Token::DIRECTIVE) {
      break;
    }

    // we keep the directives from the last document if none are specified;
    // but if any directives are specific, then we reset them
    if (!readDirective) {
      m_pDirectives.reset(new Directives);
    }

    readDirective = true;
    HandleDirective(token);
    m_pScanner->pop();
  }
}

void Parser::HandleDirective(const Token& token) {
  if (token.value == "YAML") {
    HandleYamlDirective(token);
  } else if (token.value == "TAG") {
    HandleTagDirective(token);
  }
}

void Parser::HandleYamlDirective(const Token& token) {
  if (token.params.size() != 1) {
    throw ParserException(token.mark, ErrorMsg::YAML_DIRECTIVE_ARGS);
  }

  if (!m_pDirectives->version.isDefault) {
    throw ParserException(token.mark, ErrorMsg::REPEATED_YAML_DIRECTIVE);
  }

  std::stringstream str(token.params[0]);
  str >> m_pDirectives->version.major;
  str.get();
  str >> m_pDirectives->version.minor;
  if (!str || str.peek() != EOF) {
    throw ParserException(
        token.mark, std::string(ErrorMsg::YAML_VERSION) + token.params[0]);
  }

  if (m_pDirectives->version.major > 1) {
    throw ParserException(token.mark, ErrorMsg::YAML_MAJOR_VERSION);
  }

  m_pDirectives->version.isDefault = false;
  // TODO: warning on major == 1, minor > 2?
}

void Parser::HandleTagDirective(const Token& token) {
  if (token.params.size() != 2)
    throw ParserException(token.mark, ErrorMsg::TAG_DIRECTIVE_ARGS);

  const std::string& handle = token.params[0];
  const std::string& prefix = token.params[1];
  if (m_pDirectives->tags.find(handle) != m_pDirectives->tags.end()) {
    throw ParserException(token.mark, ErrorMsg::REPEATED_TAG_DIRECTIVE);
  }

  m_pDirectives->tags[handle] = prefix;
}

void Parser::PrintTokens(std::ostream& out) {
  if (!m_pScanner.get()) {
    return;
  }

  while (1) {
    if (m_pScanner->empty()) {
      break;
    }

    out << m_pScanner->peek() << "\n";
    m_pScanner->pop();
  }
}
}
