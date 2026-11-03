//! Bounded encoding using Milvus's shared cgo_msg.proto.

use prost::Message;

use crate::{Error, LiteralCondition};

// cgo_msg.proto also imports the existing index/schema API messages. They are
// generated together so there is no second handwritten copy of the schema.
#[allow(dead_code, clippy::all)]
mod generated {
    include!(concat!(env!("OUT_DIR"), "/grep_proto.rs"));
}

use generated::milvus::proto::cgo as pb;
use pb::literal_condition::Kind;

const MAX_PAYLOAD_BYTES: usize = 16 * 1024;
// Counts protobuf messages, including envelope, Conditions and AlwaysTrue,
// rather than logical condition nodes. C++ trusts the encoder's budget.
const MAX_MESSAGE_DEPTH: usize = 192;

/// Optimization-budget exhaustion encodes True, never a partial tree. Actual
/// allocation errors and malformed internal conditions remain errors.
pub(crate) fn encode(condition: &LiteralCondition) -> Result<Vec<u8>, Error> {
    let mut builder = Builder {
        remaining: MAX_PAYLOAD_BYTES,
    };
    // Root framing requires at least two bytes.
    builder.spend(2);
    let root = builder.convert(condition, 2)?.unwrap_or_else(true_node);
    let mut message = pb::LiteralConditionTree { root: Some(root) };
    // The conversion budget charges minimum tag/length overhead. Large nested
    // messages need longer varints, so check prost's exact size as well.
    if message.encoded_len() > MAX_PAYLOAD_BYTES {
        message.root = Some(true_node());
    }
    let size = message.encoded_len();
    let mut output = Vec::new();
    output.try_reserve_exact(size)?;
    output.resize(size, 0);
    let mut destination = output.as_mut_slice();
    message
        .encode(&mut destination)
        .map_err(|_| Error::Internal("condition protobuf encoded length mismatch"))?;
    if !destination.is_empty() {
        return Err(Error::Internal("condition protobuf did not fill output"));
    }
    Ok(output)
}

fn true_node() -> pb::LiteralCondition {
    pb::LiteralCondition {
        kind: Some(Kind::AlwaysTrue(pb::AlwaysTrue {})),
    }
}

struct Builder {
    remaining: usize,
}

impl Builder {
    fn spend(&mut self, amount: usize) -> bool {
        if amount > self.remaining {
            return false;
        }
        self.remaining -= amount;
        true
    }

    fn convert(
        &mut self,
        condition: &LiteralCondition,
        depth: usize,
    ) -> Result<Option<pb::LiteralCondition>, Error> {
        // Include room for the node's inner message (AlwaysTrue/Conditions).
        // This conservative check also bounds recursive conversion and drop.
        if depth >= MAX_MESSAGE_DEPTH || !self.spend(2) {
            return Ok(None);
        }
        let kind = match condition {
            LiteralCondition::True => return Ok(Some(true_node())),
            LiteralCondition::Literal(bytes) => {
                if bytes.is_empty() {
                    return Err(Error::Internal("empty condition literal"));
                }
                if !self.spend(bytes.len()) {
                    return Ok(None);
                }
                if std::str::from_utf8(bytes).is_err() {
                    return Err(Error::Internal("condition literal is not UTF-8"));
                }
                let mut owned = Vec::new();
                owned.try_reserve_exact(bytes.len())?;
                owned.extend_from_slice(bytes);
                Kind::Literal(owned)
            }
            LiteralCondition::And(children) | LiteralCondition::Or(children) => {
                if children.len() < 2 {
                    return Err(Error::Internal("condition operator requires two children"));
                }
                // Each child costs at least two framing bytes plus a two-byte
                // node. Bound reservation before allocating a potentially wide
                // repeated field, without adding a 127-node protocol limit.
                if children.len() > self.remaining / 4 {
                    return Ok(None);
                }
                let mut encoded = Vec::new();
                encoded.try_reserve_exact(children.len())?;
                for child in children {
                    if !self.spend(2) {
                        return Ok(None);
                    }
                    let Some(child) = self.convert(child, depth + 2)? else {
                        return Ok(None);
                    };
                    encoded.push(child);
                }
                let list = pb::Conditions { children: encoded };
                if matches!(condition, LiteralCondition::And(_)) {
                    Kind::All(list)
                } else {
                    Kind::Any(list)
                }
            }
        };
        Ok(Some(pb::LiteralCondition { kind: Some(kind) }))
    }
}

#[cfg(test)]
mod tests;
