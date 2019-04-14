#include <assert.h>
#include <iterator>
#include <sstream>

#include "yaml-cpp/exceptions.h"
#include "yaml-cpp/node/detail/memory.h"
#include "yaml-cpp/node/detail/node.h"  // IWYU pragma: keep
#include "yaml-cpp/node/detail/node_data.h"
#include "yaml-cpp/node/detail/node_iterator.h"
#include "yaml-cpp/node/ptr.h"
#include "yaml-cpp/node/type.h"

namespace YAML {
namespace detail {

std::string node_data::empty_scalar;

node_data::node_data()
    : m_isDefined(false),
      m_mark(Mark::null_mark()),
      m_type(NodeType::Null),
      m_style(EmitterStyle::Default),
      m_seqSize(0) {}

void node_data::mark_defined() {
  if (m_type == NodeType::Undefined)
    m_type = NodeType::Null;
  m_isDefined = true;
}

void node_data::set_mark(const Mark& mark) { m_mark = mark; }

void node_data::set_type(NodeType::value type) {
  if (type == NodeType::Undefined) {
    m_type = type;
    m_isDefined = false;
    return;
  }

  m_isDefined = true;
  if (type == m_type)
    return;

  m_type = type;

  switch (m_type) {
    case NodeType::Null:
      break;
    case NodeType::Scalar:
      m_scalar.clear();
      break;
    case NodeType::Sequence:
      reset_sequence();
      break;
    case NodeType::Map:
      reset_map();
      break;
    case NodeType::Undefined:
      assert(false);
      break;
  }
}

void node_data::set_tag(const std::string& tag) { m_tag = tag; }

void node_data::set_style(EmitterStyle::value style) { m_style = style; }

void node_data::set_null() {
  m_isDefined = true;
  m_type = NodeType::Null;
}

void node_data::set_scalar(const std::string& scalar) {
  m_isDefined = true;
  m_type = NodeType::Scalar;
  m_scalar = scalar;
}

// size/iterator
std::size_t node_data::size() const {
  if (!m_isDefined)
    return 0;

  switch (m_type) {
    case NodeType::Sequence:
      compute_seq_size();
      return m_seqSize;
    case NodeType::Map:
      compute_map_size();
      return m_map.size() - m_undefinedPairs.size();
    default:
      return 0;
  }
  return 0;
}

void node_data::compute_seq_size() const {
  while (m_seqSize < m_sequence.size() && m_sequence[m_seqSize]->is_defined())
    m_seqSize++;
}

void node_data::compute_map_size() const {
  kv_pairs::iterator it = m_undefinedPairs.begin();
  while (it != m_undefinedPairs.end()) {
    kv_pairs::iterator jt = std::next(it);
    if (it->first->is_defined() && it->second->is_defined())
      m_undefinedPairs.erase(it);
    it = jt;
  }
}

const_node_iterator node_data::begin() const {
  if (!m_isDefined)
    return const_node_iterator();

  switch (m_type) {
    case NodeType::Sequence:
      return const_node_iterator(m_sequence.begin());
    case NodeType::Map:
      return const_node_iterator(m_map.begin(), m_map.end());
    default:
      return const_node_iterator();
  }
}

node_iterator node_data::begin() {
  if (!m_isDefined)
    return node_iterator();

  switch (m_type) {
    case NodeType::Sequence:
      return node_iterator(m_sequence.begin());
    case NodeType::Map:
      return node_iterator(m_map.begin(), m_map.end());
    default:
      return node_iterator();
  }
}

const_node_iterator node_data::end() const {
  if (!m_isDefined)
    return const_node_iterator();

  switch (m_type) {
    case NodeType::Sequence:
      return const_node_iterator(m_sequence.end());
    case NodeType::Map:
      return const_node_iterator(m_map.end(), m_map.end());
    default:
      return const_node_iterator();
  }
}

node_iterator node_data::end() {
  if (!m_isDefined)
    return node_iterator();

  switch (m_type) {
    case NodeType::Sequence:
      return node_iterator(m_sequence.end());
    case NodeType::Map:
      return node_iterator(m_map.end(), m_map.end());
    default:
      return node_iterator();
  }
}

// sequence
void node_data::push_back(node& node, shared_memory_holder /* pMemory */) {
  if (m_type == NodeType::Undefined || m_type == NodeType::Null) {
    m_type = NodeType::Sequence;
    reset_sequence();
  }

  if (m_type != NodeType::Sequence)
    throw BadPushback();

  m_sequence.push_back(&node);
}

void node_data::insert(node& key, node& value, shared_memory_holder pMemory) {
  switch (m_type) {
    case NodeType::Map:
      break;
    case NodeType::Undefined:
    case NodeType::Null:
    case NodeType::Sequence:
      convert_to_map(pMemory);
      break;
    case NodeType::Scalar:
      throw BadSubscript();
  }

  insert_map_pair(key, value);
}

// indexing
node* node_data::get(node& key, shared_memory_holder /* pMemory */) const {
  if (m_type != NodeType::Map) {
    return NULL;
  }

  for (node_map::const_iterator it = m_map.begin(); it != m_map.end(); ++it) {
    if (it->first->is(key))
      return it->second;
  }

  return NULL;
}

node& node_data::get(node& key, shared_memory_holder pMemory) {
  switch (m_type) {
    case NodeType::Map:
      break;
    case NodeType::Undefined:
    case NodeType::Null:
    case NodeType::Sequence:
      convert_to_map(pMemory);
      break;
    case NodeType::Scalar:
      throw BadSubscript();
  }

  for (node_map::const_iterator it = m_map.begin(); it != m_map.end(); ++it) {
    if (it->first->is(key))
      return *it->second;
  }

  node& value = pMemory->create_node();
  insert_map_pair(key, value);
  return value;
}

bool node_data::remove(node& key, shared_memory_holder /* pMemory */) {
  if (m_type != NodeType::Map)
    return false;

  for (node_map::iterator it = m_map.begin(); it != m_map.end(); ++it) {
    if (it->first->is(key)) {
      m_map.erase(it);
      return true;
    }
  }

  return false;
}

void node_data::reset_sequence() {
  m_sequence.clear();
  m_seqSize = 0;
}

void node_data::reset_map() {
  m_map.clear();
  m_undefinedPairs.clear();
}

void node_data::insert_map_pair(node& key, node& value) {
  m_map.emplace_back(&key, &value);

  if (!key.is_defined() || !value.is_defined())
    m_undefinedPairs.emplace_back(&key, &value);
}

void node_data::convert_to_map(shared_memory_holder pMemory) {
  switch (m_type) {
    case NodeType::Undefined:
    case NodeType::Null:
      reset_map();
      m_type = NodeType::Map;
      break;
    case NodeType::Sequence:
      convert_sequence_to_map(pMemory);
      break;
    case NodeType::Map:
      break;
    case NodeType::Scalar:
      assert(false);
      break;
  }
}

void node_data::convert_sequence_to_map(shared_memory_holder pMemory) {
  assert(m_type == NodeType::Sequence);

  reset_map();
  for (std::size_t i = 0; i < m_sequence.size(); i++) {
    std::stringstream stream;
    stream << i;

    node& key = pMemory->create_node();
    key.set_scalar(stream.str());
    insert_map_pair(key, *m_sequence[i]);
  }

  reset_sequence();
  m_type = NodeType::Map;
}
}
}
