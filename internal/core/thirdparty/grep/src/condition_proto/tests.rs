use super::*;
use crate::{session::ResultOverhead, CompileLimits, MatchOptions, Plan, Session};

fn literal(bytes: &[u8]) -> LiteralCondition {
    LiteralCondition::Literal(bytes.to_vec())
}

fn decode(bytes: &[u8]) -> pb::LiteralConditionTree {
    assert!(!bytes.is_empty() && bytes.len() <= MAX_PAYLOAD_BYTES);
    let message = pb::LiteralConditionTree::decode(bytes).unwrap();
    assert!(message.root.is_some());
    message
}

fn from_proto(node: &pb::LiteralCondition) -> LiteralCondition {
    match node.kind.as_ref().expect("every node must select a kind") {
        Kind::AlwaysTrue(_) => LiteralCondition::True,
        Kind::Literal(bytes) => literal(bytes),
        Kind::All(list) => LiteralCondition::And(list.children.iter().map(from_proto).collect()),
        Kind::Any(list) => LiteralCondition::Or(list.children.iter().map(from_proto).collect()),
    }
}

fn round_trip(condition: &LiteralCondition) {
    let message = decode(&encode(condition).unwrap());
    assert_eq!(&from_proto(message.root.as_ref().unwrap()), condition);
}

fn assert_fallback(condition: &LiteralCondition) {
    let message = decode(&encode(condition).unwrap());
    assert!(matches!(
        message.root.unwrap().kind,
        Some(Kind::AlwaysTrue(_))
    ));
}

#[test]
fn protobuf_preserves_nodes_bytes_and_child_order() {
    for condition in [
        LiteralCondition::True,
        literal(b"a"), // Codec does not impose the extractor's two-byte minimum.
        literal("中\né\0tail".as_bytes()),
        LiteralCondition::And(vec![literal(b"error"), literal(b"timeout")]),
        LiteralCondition::Or(vec![literal(b"error"), literal(b"warn")]),
        LiteralCondition::And(vec![
            LiteralCondition::Or(vec![literal(b"error"), literal(b"warn")]),
            literal(b"timeout"),
            LiteralCondition::True,
        ]),
    ] {
        round_trip(&condition);
    }
}

#[test]
fn codec_does_not_turn_planner_budgets_into_wire_limits() {
    let wide = LiteralCondition::Or(
        (0..200)
            .map(|i| literal(format!("atom{i}").as_bytes()))
            .collect(),
    );
    round_trip(&wide); // More than 127 nodes, but within transport budgets.
    round_trip(&literal(&vec![b'x'; 1024])); // More than a 64-byte extracted atom.
}

#[test]
fn transport_size_exhaustion_encodes_true_instead_of_partial_children() {
    // Exact protobuf size includes root and literal length prefixes.
    let at_limit = literal(&vec![b'x'; MAX_PAYLOAD_BYTES - 6]);
    assert_eq!(encode(&at_limit).unwrap().len(), MAX_PAYLOAD_BYTES);
    round_trip(&at_limit);
    assert_fallback(&literal(&vec![b'x'; MAX_PAYLOAD_BYTES - 5]));
    assert_fallback(&literal(&vec![b'x'; MAX_PAYLOAD_BYTES * 2]));
    let children = vec![literal(b"required"); MAX_PAYLOAD_BYTES];
    assert_fallback(&LiteralCondition::Or(children.clone()));
    assert_fallback(&LiteralCondition::And(children));
}

fn nested(depth: usize) -> LiteralCondition {
    let mut condition = literal(b"needle");
    for _ in 1..depth {
        condition = LiteralCondition::And(vec![LiteralCondition::True, condition]);
    }
    condition
}

#[test]
fn protobuf_message_depth_is_not_logical_tree_depth() {
    let true_payload = encode(&LiteralCondition::True).unwrap();
    // prost's default *decoder* recursion limit is not used in production.
    // C++ relaxes its parser default so these valid encodings are accepted.
    for depth in [64, 70, 95] {
        let payload = encode(&nested(depth)).unwrap();
        assert!(payload.len() <= MAX_PAYLOAD_BYTES);
        assert_ne!(payload, true_payload, "logical depth {depth}");
    }
    assert_fallback(&nested(96));
    assert_fallback(&nested(200));
}

#[test]
fn malformed_internal_conditions_are_not_resource_fallbacks() {
    for condition in [
        literal(b""),
        literal(&[0xff, 0xfe]),
        LiteralCondition::And(vec![]),
        LiteralCondition::Or(vec![literal(b"only")]),
    ] {
        assert!(matches!(encode(&condition), Err(Error::Internal(_))));
    }
}

fn compile(pattern: &str) -> Plan {
    Plan::compile(
        &[pattern.as_bytes()],
        MatchOptions::default(),
        CompileLimits {
            max_regex_bytes: 8 * 1024 * 1024,
            max_dfa_cache_bytes: 1024 * 1024,
        },
    )
    .unwrap()
}

#[test]
fn plan_clones_share_one_owned_payload() {
    let mut pattern = String::from("(error|warn).*timeout");
    let plan = compile(&pattern);
    pattern.clear();
    let clone = plan.clone();
    assert_eq!(
        plan.condition_payload().as_ptr(),
        clone.condition_payload().as_ptr()
    );
    let expected = plan.literal_condition().clone();
    drop(plan);
    let decoded = decode(clone.condition_payload());
    assert_eq!(from_proto(decoded.root.as_ref().unwrap()), expected);
    let mut session = Session::new(&clone);
    drop(clone);
    let result = session
        .execute_accounted(
            &[b"warn -- timeout"],
            1024 * 1024,
            ResultOverhead::default(),
        )
        .unwrap();
    assert_eq!(result.texts[0].lines.len(), 1);
}

fn holds(node: &pb::LiteralCondition, value: &[u8]) -> bool {
    match node.kind.as_ref().unwrap() {
        Kind::AlwaysTrue(_) => true,
        Kind::Literal(bytes) => memchr::memmem::find(value, bytes).is_some(),
        Kind::All(list) => list.children.iter().all(|child| holds(child, value)),
        Kind::Any(list) => list.children.iter().any(|child| holds(child, value)),
    }
}

#[test]
fn serialized_conditions_remain_necessary_for_actual_session_matches() {
    let texts: &[&[u8]] = &[
        b"error timeout",
        b"warn -- timeout",
        b"ERROR timeout",
        b"done",
        b"\0foo",
        "Kfoo".as_bytes(),
    ];
    let mut matches = 0;
    for pattern in [
        "error.*timeout",
        "(error|warn).*timeout",
        "(?i:error).*timeout",
        r"\x00foo",
        "(?i:k)foo",
        "error|.*",
    ] {
        let plan = compile(pattern);
        let message = decode(plan.condition_payload());
        let result = Session::new(&plan)
            .execute_accounted(texts, 1024 * 1024, ResultOverhead::default())
            .unwrap();
        for (text, result) in texts.iter().zip(result.texts.iter()) {
            if !result.lines.is_empty() {
                matches += 1;
                assert!(
                    holds(message.root.as_ref().unwrap(), text),
                    "{pattern}: {text:?}"
                );
            }
        }
    }
    assert!(matches > 10);
    // Extraction fallback is explicitly serialized and still executes Grep.
    let deep = format!("{}needle{}", "(".repeat(80), ")".repeat(80));
    let plan = compile(&deep);
    assert!(matches!(
        decode(plan.condition_payload()).root.unwrap().kind,
        Some(Kind::AlwaysTrue(_))
    ));
    let result = Session::new(&plan)
        .execute_accounted(&[b"needle"], 1024 * 1024, ResultOverhead::default())
        .unwrap();
    assert_eq!(result.texts[0].lines.len(), 1);
}
