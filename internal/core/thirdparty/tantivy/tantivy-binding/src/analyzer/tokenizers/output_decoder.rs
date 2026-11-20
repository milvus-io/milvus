use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

use ort::{
    session::SessionOutputs,
    value::{DynValue, Outlet, TensorElementType, ValueType},
};
use serde::Deserialize;
use tantivy::tokenizer::Token;

use crate::error::{Result, TantivyBindingError};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum TermAction {
    Begin,
    Continue,
    End,
    Single,
    Skip,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OffsetUnit {
    Utf8Byte,
    UnicodeScalar,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OutputDecoderConfig {
    outputs: HashMap<String, String>,
    representation: RepresentationConfig,
    decoder: DecoderConfig,
    projection: ProjectionConfig,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum RepresentationConfig {
    UnitScores {
        output: String,
        alignment: AlignmentConfig,
        distribution: DistributionConfig,
    },
    Segments {
        output: String,
        offset_unit: OffsetUnit,
        columns: SegmentColumns,
    },
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum AlignmentConfig {
    UnicodeScalar,
    Utf8Byte,
    Offsets {
        output: String,
        offset_unit: OffsetUnit,
        columns: OffsetColumns,
    },
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum DistributionConfig {
    Categorical { classes: Vec<String> },
    IndependentBernoulli { heads: Vec<String> },
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum DecoderConfig {
    Argmax,
    LinearChainCrf(Crf),
    StateTable { states: Vec<StateConfig> },
    Identity,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum ProjectionConfig {
    ActionSequence {
        actions: HashMap<String, TermAction>,
        #[serde(default)]
        skip_whitespace: bool,
    },
    TermSpans,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
struct OffsetColumns {
    start: usize,
    end: usize,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
struct SegmentColumns {
    start: usize,
    end: usize,
    position: usize,
    position_length: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StateConfig {
    name: String,
    when: HashMap<String, bool>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Crf {
    start_transitions: Vec<f32>,
    end_transitions: Vec<f32>,
    transitions: Vec<Vec<f32>>,
}

enum Alignment {
    Implicit(OffsetUnit),
    Offsets {
        output: String,
        offset_unit: OffsetUnit,
        columns: OffsetColumns,
    },
}

enum UnitDecoder {
    Categorical {
        actions: Vec<TermAction>,
        algorithm: SequenceAlgorithm,
    },
    IndependentBernoulli {
        channels: usize,
        states: Vec<FactorizedState>,
    },
}

enum SequenceAlgorithm {
    Argmax,
    LinearChainCrf(Crf),
}

struct FactorizedState {
    action: TermAction,
    conditions: Vec<(usize, bool)>,
}

#[derive(Clone, Copy)]
struct UnitSpan {
    from: usize,
    to: usize,
}

enum Decoder {
    UnitScores {
        output: String,
        alignment: Alignment,
        decoder: UnitDecoder,
        skip_whitespace: bool,
    },
    Segments {
        output: String,
        offset_unit: OffsetUnit,
        columns: SegmentColumns,
    },
}

pub(crate) struct OutputDecoder {
    decoder: Decoder,
}

impl OutputDecoder {
    pub(crate) fn from_file(path: &Path) -> Result<Self> {
        let config: OutputDecoderConfig = serde_json::from_slice(&fs::read(path)?)?;
        Self::from_config(config)
    }

    fn from_config(config: OutputDecoderConfig) -> Result<Self> {
        if config.outputs.is_empty()
            || config
                .outputs
                .iter()
                .any(|(role, output)| role.is_empty() || output.is_empty())
        {
            return Err(invalid("unsupported model output decoder config"));
        }

        let mut used_outputs = HashSet::new();
        let decoder = match config.representation {
            RepresentationConfig::UnitScores {
                output,
                alignment,
                distribution,
            } => {
                let output =
                    resolve_output(&config.outputs, &mut used_outputs, &output, "unit scores")?;
                let alignment = match alignment {
                    AlignmentConfig::UnicodeScalar => {
                        Alignment::Implicit(OffsetUnit::UnicodeScalar)
                    }
                    AlignmentConfig::Utf8Byte => Alignment::Implicit(OffsetUnit::Utf8Byte),
                    AlignmentConfig::Offsets {
                        output,
                        offset_unit,
                        columns,
                    } => {
                        if columns.start == columns.end {
                            return Err(invalid("alignment offset columns must be unique"));
                        }
                        if columns.start.max(columns.end) >= i64::MAX as usize {
                            return Err(invalid("alignment offset column is too large"));
                        }
                        Alignment::Offsets {
                            output: resolve_output(
                                &config.outputs,
                                &mut used_outputs,
                                &output,
                                "alignment offsets",
                            )?,
                            offset_unit,
                            columns,
                        }
                    }
                };

                let (decoder, names) = match distribution {
                    DistributionConfig::Categorical { classes } => {
                        validate_names(&classes, 2, "categorical classes")?;
                        let algorithm = match config.decoder {
                            DecoderConfig::Argmax => SequenceAlgorithm::Argmax,
                            DecoderConfig::LinearChainCrf(crf) => {
                                validate_crf(&crf, classes.len())?;
                                SequenceAlgorithm::LinearChainCrf(crf)
                            }
                            _ => {
                                return Err(invalid(
                                    "categorical unit scores require argmax or linear_chain_crf",
                                ));
                            }
                        };
                        let names = classes.clone();
                        (
                            UnitDecoder::Categorical {
                                actions: Vec::new(),
                                algorithm,
                            },
                            names,
                        )
                    }
                    DistributionConfig::IndependentBernoulli { heads } => {
                        validate_names(&heads, 1, "independent Bernoulli heads")?;
                        let DecoderConfig::StateTable { states } = config.decoder else {
                            return Err(invalid(
                                "independent Bernoulli unit scores require a state_table",
                            ));
                        };
                        validate_state_names(&states)?;
                        validate_state_conditions(&states, &heads)?;
                        let names = states
                            .iter()
                            .map(|state| state.name.clone())
                            .collect::<Vec<_>>();
                        let factorized = states
                            .into_iter()
                            .map(|state| {
                                let conditions = heads
                                    .iter()
                                    .enumerate()
                                    .map(|(index, head)| (index, state.when[head]))
                                    .collect();
                                Ok(FactorizedState {
                                    action: TermAction::Skip,
                                    conditions,
                                })
                            })
                            .collect::<Result<Vec<_>>>()?;
                        (
                            UnitDecoder::IndependentBernoulli {
                                channels: heads.len(),
                                states: factorized,
                            },
                            names,
                        )
                    }
                };

                let ProjectionConfig::ActionSequence {
                    actions,
                    skip_whitespace,
                } = config.projection
                else {
                    return Err(invalid("unit scores require an action_sequence projection"));
                };
                let actions = resolve_actions(&names, actions)?;
                let decoder = match decoder {
                    UnitDecoder::Categorical { algorithm, .. } => {
                        UnitDecoder::Categorical { actions, algorithm }
                    }
                    UnitDecoder::IndependentBernoulli {
                        channels,
                        mut states,
                    } => {
                        for (state, action) in states.iter_mut().zip(actions) {
                            state.action = action;
                        }
                        UnitDecoder::IndependentBernoulli { channels, states }
                    }
                };
                Decoder::UnitScores {
                    output,
                    alignment,
                    decoder,
                    skip_whitespace,
                }
            }
            RepresentationConfig::Segments {
                output,
                offset_unit,
                columns,
            } => {
                let DecoderConfig::Identity = config.decoder else {
                    return Err(invalid("segments require the identity decoder"));
                };
                let ProjectionConfig::TermSpans = config.projection else {
                    return Err(invalid("segments require the term_spans projection"));
                };
                let values = [
                    columns.start,
                    columns.end,
                    columns.position,
                    columns.position_length,
                ];
                if values.into_iter().collect::<HashSet<_>>().len() != values.len() {
                    return Err(invalid("segment columns must be unique"));
                }
                if values.iter().any(|&column| column >= i64::MAX as usize) {
                    return Err(invalid("segment column is too large"));
                }
                Decoder::Segments {
                    output: resolve_output(
                        &config.outputs,
                        &mut used_outputs,
                        &output,
                        "segments",
                    )?,
                    offset_unit,
                    columns,
                }
            }
        };

        if used_outputs.len() != config.outputs.len() {
            return Err(invalid("model output bindings contain unused roles"));
        }
        Ok(Self { decoder })
    }

    pub(crate) fn validate_outputs(&self, outputs: &[Outlet]) -> Result<()> {
        match &self.decoder {
            Decoder::UnitScores {
                output,
                alignment,
                decoder,
                ..
            } => {
                let channels = match decoder {
                    UnitDecoder::Categorical { actions, .. } => actions.len(),
                    UnitDecoder::IndependentBernoulli { channels, .. } => *channels,
                };
                let score_shape = output_shape(outputs, output, TensorElementType::Float32, 3)?;
                if score_shape[2] != -1 && score_shape[2] != channels as i64 {
                    return Err(invalid(format!(
                        "model output {output} channel count does not match its representation"
                    )));
                }
                if let Alignment::Offsets {
                    output, columns, ..
                } = alignment
                {
                    let offset_shape = output_shape(outputs, output, TensorElementType::Int64, 3)?;
                    let width = columns.start.max(columns.end) + 1;
                    if offset_shape[2] != -1 && offset_shape[2] < width as i64 {
                        return Err(invalid(format!(
                            "model output {output} is too narrow for alignment offsets"
                        )));
                    }
                    if score_shape[1] != -1
                        && offset_shape[1] != -1
                        && score_shape[1] != offset_shape[1]
                    {
                        return Err(invalid(
                            "unit scores and alignment offsets have different lengths",
                        ));
                    }
                }
                Ok(())
            }
            Decoder::Segments {
                output, columns, ..
            } => {
                let shape = output_shape(outputs, output, TensorElementType::Int64, 3)?;
                let width = [
                    columns.start,
                    columns.end,
                    columns.position,
                    columns.position_length,
                ]
                .into_iter()
                .max()
                .unwrap()
                    + 1;
                if shape[2] != -1 && shape[2] < width as i64 {
                    return Err(invalid(format!(
                        "model output {output} is too narrow for term segments"
                    )));
                }
                Ok(())
            }
        }
    }

    pub(crate) fn decode(
        &self,
        outputs: &SessionOutputs,
        texts: &[&str],
    ) -> std::result::Result<Vec<Vec<Token>>, String> {
        match &self.decoder {
            Decoder::UnitScores {
                output,
                alignment,
                decoder,
                skip_whitespace,
            } => {
                let value = runtime_output(outputs, output)?;
                let (shape, values) = value
                    .try_extract_tensor::<f32>()
                    .map_err(|error| format!("model unit scores are invalid: {error}"))?;
                if shape.len() != 3
                    || shape[0] != texts.len() as i64
                    || shape[1] < 0
                    || shape[2] < 0
                {
                    return Err(format!(
                        "model returned unexpected unit score shape: {shape:?}"
                    ));
                }
                let units = decode_alignments(alignment, outputs, texts, shape[1] as usize)?;
                match decoder {
                    UnitDecoder::Categorical { actions, algorithm } => decode_categorical(
                        shape.as_ref(),
                        values,
                        texts,
                        &units,
                        actions,
                        algorithm,
                        *skip_whitespace,
                    ),
                    UnitDecoder::IndependentBernoulli { channels, states } => decode_independent(
                        shape.as_ref(),
                        values,
                        texts,
                        &units,
                        *channels,
                        states,
                        *skip_whitespace,
                    ),
                }
            }
            Decoder::Segments {
                output,
                offset_unit,
                columns,
            } => {
                let (shape, values) = runtime_output(outputs, output)?
                    .try_extract_tensor::<i64>()
                    .map_err(|error| format!("model segment output is invalid: {error}"))?;
                decode_segments(shape.as_ref(), values, texts, *offset_unit, *columns)
            }
        }
    }
}

fn resolve_output(
    outputs: &HashMap<String, String>,
    used: &mut HashSet<String>,
    role: &str,
    kind: &str,
) -> Result<String> {
    if role.is_empty() || !used.insert(role.to_string()) {
        return Err(invalid(format!("{kind} output binding is invalid")));
    }
    outputs
        .get(role)
        .cloned()
        .ok_or_else(|| invalid(format!("{kind} references unknown output role {role}")))
}

fn validate_names(names: &[String], minimum: usize, kind: &str) -> Result<()> {
    if names.len() < minimum
        || names.iter().any(String::is_empty)
        || names.iter().collect::<HashSet<_>>().len() != names.len()
    {
        return Err(invalid(format!(
            "{kind} must contain at least {minimum} unique names"
        )));
    }
    Ok(())
}

fn validate_state_names(states: &[StateConfig]) -> Result<()> {
    if states.len() < 2
        || states
            .iter()
            .any(|state| state.name.is_empty() || state.when.is_empty())
        || states
            .iter()
            .map(|state| state.name.as_str())
            .collect::<HashSet<_>>()
            .len()
            != states.len()
    {
        return Err(invalid(
            "state table requires at least two uniquely named states with conditions",
        ));
    }
    Ok(())
}

fn validate_state_conditions(states: &[StateConfig], heads: &[String]) -> Result<()> {
    let heads = heads.iter().map(String::as_str).collect::<HashSet<_>>();
    let mut assignments = HashSet::new();
    for state in states {
        if state.when.len() != heads.len()
            || state.when.keys().any(|head| !heads.contains(head.as_str()))
        {
            return Err(invalid(
                "each state table entry must assign every Bernoulli head exactly once",
            ));
        }
        let assignment = heads
            .iter()
            .map(|head| state.when[*head])
            .collect::<Vec<_>>();
        if !assignments.insert(assignment) {
            return Err(invalid("state table contains duplicate head assignments"));
        }
    }
    Ok(())
}

fn resolve_actions(
    names: &[String],
    mut actions: HashMap<String, TermAction>,
) -> Result<Vec<TermAction>> {
    let resolved = names
        .iter()
        .map(|name| {
            actions
                .remove(name)
                .ok_or_else(|| invalid(format!("projection does not define action for {name}")))
        })
        .collect::<Result<Vec<_>>>()?;
    if !actions.is_empty() {
        return Err(invalid("projection defines actions for unknown names"));
    }
    Ok(resolved)
}

fn output_shape(
    outputs: &[Outlet],
    name: &str,
    expected_type: TensorElementType,
    rank: usize,
) -> Result<Vec<i64>> {
    let output = outputs
        .iter()
        .find(|output| output.name() == name)
        .ok_or_else(|| {
            invalid(format!(
                "model tokenizer ONNX does not contain output {name}"
            ))
        })?;
    let ValueType::Tensor { ty, shape, .. } = output.dtype() else {
        return Err(invalid(format!("model output {name} must be a tensor")));
    };
    if *ty != expected_type || shape.len() != rank || (shape[0] != -1 && shape[0] != 1) {
        return Err(invalid(format!(
            "model output {name} type or shape does not match its representation"
        )));
    }
    Ok(shape.iter().copied().collect())
}

fn runtime_output<'a>(
    outputs: &'a SessionOutputs,
    name: &str,
) -> std::result::Result<&'a DynValue, String> {
    outputs
        .get(name)
        .ok_or_else(|| format!("model tokenizer output {name} does not exist"))
}

fn decode_alignments(
    alignment: &Alignment,
    outputs: &SessionOutputs,
    texts: &[&str],
    output_units: usize,
) -> std::result::Result<Vec<Vec<UnitSpan>>, String> {
    match alignment {
        Alignment::Implicit(unit) => texts
            .iter()
            .map(|text| {
                let boundaries = unit_boundaries(text, *unit);
                let units = boundaries
                    .windows(2)
                    .map(|window| UnitSpan {
                        from: window[0],
                        to: window[1],
                    })
                    .collect::<Vec<_>>();
                if units.len() > output_units {
                    return Err("model unit scores are shorter than the input".to_string());
                }
                Ok(units)
            })
            .collect(),
        Alignment::Offsets {
            output,
            offset_unit,
            columns,
        } => {
            let (shape, values) = runtime_output(outputs, output)?
                .try_extract_tensor::<i64>()
                .map_err(|error| format!("model alignment offsets are invalid: {error}"))?;
            let width = columns.start.max(columns.end) + 1;
            if shape.len() != 3
                || shape[0] != texts.len() as i64
                || shape[1] != output_units as i64
                || shape[2] < width as i64
            {
                return Err(format!(
                    "model returned unexpected alignment offset shape: {shape:?}"
                ));
            }
            let row_width = shape[2] as usize;
            texts
                .iter()
                .enumerate()
                .map(|(row, text)| {
                    let boundaries = unit_boundaries(text, *offset_unit);
                    let mut units = Vec::with_capacity(output_units);
                    let mut previous_start = None;
                    for unit in 0..output_units {
                        let base = (row * output_units + unit) * row_width;
                        let start = values[base + columns.start];
                        if start < 0 {
                            break;
                        }
                        let end = values[base + columns.end];
                        if end <= start || previous_start.is_some_and(|previous| start < previous) {
                            return Err("model returned invalid alignment offsets".to_string());
                        }
                        let from = *boundaries.get(start as usize).ok_or_else(|| {
                            "model alignment starts outside the input".to_string()
                        })?;
                        let to = *boundaries
                            .get(end as usize)
                            .ok_or_else(|| "model alignment ends outside the input".to_string())?;
                        if text.get(from..to).is_none() {
                            return Err("model alignment is not on UTF-8 boundaries".to_string());
                        }
                        units.push(UnitSpan { from, to });
                        previous_start = Some(start);
                    }
                    Ok(units)
                })
                .collect()
        }
    }
}

fn decode_categorical(
    shape: &[i64],
    values: &[f32],
    texts: &[&str],
    units: &[Vec<UnitSpan>],
    actions: &[TermAction],
    algorithm: &SequenceAlgorithm,
    skip_whitespace: bool,
) -> std::result::Result<Vec<Vec<Token>>, String> {
    validate_sequence_shape(shape, texts.len(), actions.len())?;
    let output_units = shape[1] as usize;
    texts
        .iter()
        .enumerate()
        .map(|(row, text)| {
            let unit_count = units[row].len();
            let start = row * output_units * actions.len();
            let emissions = &values[start..start + unit_count * actions.len()];
            let ids = match algorithm {
                SequenceAlgorithm::Argmax => emissions
                    .chunks_exact(actions.len())
                    .map(argmax)
                    .collect::<Vec<_>>(),
                SequenceAlgorithm::LinearChainCrf(crf) => {
                    viterbi(emissions, unit_count, actions.len(), crf)?
                }
            };
            tokens_from_actions(
                text,
                &units[row],
                &ids.into_iter().map(|id| actions[id]).collect::<Vec<_>>(),
                skip_whitespace,
            )
        })
        .collect()
}

fn decode_independent(
    shape: &[i64],
    values: &[f32],
    texts: &[&str],
    units: &[Vec<UnitSpan>],
    channels: usize,
    states: &[FactorizedState],
    skip_whitespace: bool,
) -> std::result::Result<Vec<Vec<Token>>, String> {
    validate_sequence_shape(shape, texts.len(), channels)?;
    let output_units = shape[1] as usize;
    texts
        .iter()
        .enumerate()
        .map(|(row, text)| {
            let start = row * output_units * channels;
            let actions = values[start..start + units[row].len() * channels]
                .chunks_exact(channels)
                .map(|values| {
                    states
                        .iter()
                        .map(|state| {
                            let score = state
                                .conditions
                                .iter()
                                .map(|(head, expected)| {
                                    log_sigmoid(if *expected {
                                        values[*head]
                                    } else {
                                        -values[*head]
                                    })
                                })
                                .sum::<f32>();
                            (state.action, score)
                        })
                        .max_by(|(_, left), (_, right)| left.total_cmp(right))
                        .unwrap()
                        .0
                })
                .collect::<Vec<_>>();
            tokens_from_actions(text, &units[row], &actions, skip_whitespace)
        })
        .collect()
}

fn validate_sequence_shape(
    shape: &[i64],
    batch: usize,
    channels: usize,
) -> std::result::Result<(), String> {
    if shape.len() != 3 || shape[0] != batch as i64 || shape[1] < 0 || shape[2] != channels as i64 {
        return Err(format!(
            "model returned unexpected unit score shape: {shape:?}"
        ));
    }
    Ok(())
}

fn tokens_from_actions(
    text: &str,
    units: &[UnitSpan],
    actions: &[TermAction],
    skip_whitespace: bool,
) -> std::result::Result<Vec<Token>, String> {
    if units.len() != actions.len() {
        return Err("model actions do not match its alignment".to_string());
    }
    let mut spans = Vec::new();
    let mut open = None;
    for (index, configured_action) in actions.iter().enumerate() {
        let value = text
            .get(units[index].from..units[index].to)
            .ok_or_else(|| "model alignment is not on UTF-8 boundaries".to_string())?;
        let action = if skip_whitespace && value.chars().all(char::is_whitespace) {
            TermAction::Skip
        } else {
            *configured_action
        };
        match action {
            TermAction::Begin => {
                close_unit_span(&mut spans, &mut open, index, units);
                open = Some(index);
            }
            TermAction::Continue => {
                open.get_or_insert(index);
            }
            TermAction::End => {
                let begin = open.take().unwrap_or(index);
                push_unit_span(&mut spans, begin, index + 1, units);
            }
            TermAction::Single => {
                close_unit_span(&mut spans, &mut open, index, units);
                push_unit_span(&mut spans, index, index + 1, units);
            }
            TermAction::Skip => close_unit_span(&mut spans, &mut open, index, units),
        }
    }
    close_unit_span(&mut spans, &mut open, actions.len(), units);
    spans
        .into_iter()
        .enumerate()
        .map(|(position, span)| {
            let value = text
                .get(span.from..span.to)
                .ok_or_else(|| "model term is not on UTF-8 boundaries".to_string())?;
            Ok(Token {
                offset_from: span.from,
                offset_to: span.to,
                position,
                text: value.to_string(),
                position_length: 1,
            })
        })
        .collect()
}

fn close_unit_span(
    spans: &mut Vec<UnitSpan>,
    open: &mut Option<usize>,
    end: usize,
    units: &[UnitSpan],
) {
    if let Some(begin) = open.take() {
        push_unit_span(spans, begin, end, units);
    }
}

fn push_unit_span(spans: &mut Vec<UnitSpan>, begin: usize, end: usize, units: &[UnitSpan]) {
    if begin < end {
        spans.push(UnitSpan {
            from: units[begin].from,
            to: units[end - 1].to,
        });
    }
}

fn decode_segments(
    shape: &[i64],
    values: &[i64],
    texts: &[&str],
    unit: OffsetUnit,
    columns: SegmentColumns,
) -> std::result::Result<Vec<Vec<Token>>, String> {
    let width = [
        columns.start,
        columns.end,
        columns.position,
        columns.position_length,
    ]
    .into_iter()
    .max()
    .unwrap()
        + 1;
    if shape.len() != 3 || shape[0] != texts.len() as i64 || shape[1] < 0 || shape[2] < width as i64
    {
        return Err(format!(
            "model returned unexpected segment shape: {shape:?}"
        ));
    }
    let terms = shape[1] as usize;
    let row_width = shape[2] as usize;
    texts
        .iter()
        .enumerate()
        .map(|(row, text)| {
            let boundaries = unit_boundaries(text, unit);
            let mut tokens = Vec::new();
            let mut previous_position = None;
            for term in 0..terms {
                let base = (row * terms + term) * row_width;
                let start = values[base + columns.start];
                if start < 0 {
                    break;
                }
                let end = values[base + columns.end];
                let position = values[base + columns.position];
                let position_length = values[base + columns.position_length];
                if end <= start
                    || position < 0
                    || position_length <= 0
                    || previous_position.is_some_and(|previous| position < previous)
                {
                    return Err("model returned an invalid term segment".to_string());
                }
                let offset_from = *boundaries
                    .get(start as usize)
                    .ok_or_else(|| "model segment starts outside the input".to_string())?;
                let offset_to = *boundaries
                    .get(end as usize)
                    .ok_or_else(|| "model segment ends outside the input".to_string())?;
                let value = text
                    .get(offset_from..offset_to)
                    .ok_or_else(|| "model segment is not on UTF-8 boundaries".to_string())?;
                tokens.push(Token {
                    offset_from,
                    offset_to,
                    position: position as usize,
                    text: value.to_string(),
                    position_length: position_length as usize,
                });
                previous_position = Some(position);
            }
            Ok(tokens)
        })
        .collect()
}

fn unit_boundaries(text: &str, unit: OffsetUnit) -> Vec<usize> {
    match unit {
        OffsetUnit::Utf8Byte => (0..=text.len()).collect(),
        OffsetUnit::UnicodeScalar => text
            .char_indices()
            .map(|(offset, _)| offset)
            .chain(std::iter::once(text.len()))
            .collect(),
    }
}

fn validate_crf(crf: &Crf, classes: usize) -> Result<()> {
    if crf.start_transitions.len() != classes
        || crf.end_transitions.len() != classes
        || crf.transitions.len() != classes
        || crf
            .transitions
            .iter()
            .any(|transition| transition.len() != classes)
    {
        return Err(invalid("CRF dimensions differ from categorical classes"));
    }
    Ok(())
}

fn viterbi(
    emissions: &[f32],
    units: usize,
    classes: usize,
    crf: &Crf,
) -> std::result::Result<Vec<usize>, String> {
    if units == 0 {
        return Ok(Vec::new());
    }
    let mut score = (0..classes)
        .map(|tag| crf.start_transitions[tag] + emissions[tag])
        .collect::<Vec<_>>();
    let mut history = Vec::with_capacity(units.saturating_sub(1));
    for step in 1..units {
        let values = &emissions[step * classes..(step + 1) * classes];
        let mut next = vec![f32::NEG_INFINITY; classes];
        let mut previous = vec![0; classes];
        for to in 0..classes {
            for from in 0..classes {
                let candidate = score[from] + crf.transitions[from][to] + values[to];
                if candidate > next[to] {
                    next[to] = candidate;
                    previous[to] = from;
                }
            }
        }
        score = next;
        history.push(previous);
    }
    let mut tag = (0..classes)
        .max_by(|left, right| {
            (score[*left] + crf.end_transitions[*left])
                .total_cmp(&(score[*right] + crf.end_transitions[*right]))
        })
        .unwrap();
    let mut labels = vec![tag];
    for previous in history.iter().rev() {
        tag = previous[tag];
        labels.push(tag);
    }
    labels.reverse();
    Ok(labels)
}

fn argmax(values: &[f32]) -> usize {
    values
        .iter()
        .enumerate()
        .max_by(|(_, left), (_, right)| left.total_cmp(right))
        .unwrap()
        .0
}

fn log_sigmoid(value: f32) -> f32 {
    if value >= 0.0 {
        -(-value).exp().ln_1p()
    } else {
        value - value.exp().ln_1p()
    }
}

fn invalid(message: impl Into<String>) -> TantivyBindingError {
    TantivyBindingError::InvalidArgument(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn units(text: &str) -> Vec<UnitSpan> {
        unit_boundaries(text, OffsetUnit::UnicodeScalar)
            .windows(2)
            .map(|window| UnitSpan {
                from: window[0],
                to: window[1],
            })
            .collect()
    }

    #[test]
    fn test_categorical_scores_map_classes_to_terms() {
        let text = "中文分词";
        let values = [
            9.0, 0.0, 0.0, 0.0, 9.0, 0.0, 0.0, 0.0, 0.0, 9.0, 0.0, 0.0, 0.0, 0.0, 9.0, 0.0,
        ];
        let actions = [
            TermAction::Single,
            TermAction::Begin,
            TermAction::End,
            TermAction::Continue,
        ];
        let tokens = decode_categorical(
            &[1, 4, 4],
            &values,
            &[text],
            &[units(text)],
            &actions,
            &SequenceAlgorithm::Argmax,
            false,
        )
        .unwrap()
        .remove(0);
        assert_eq!(
            tokens
                .iter()
                .map(|token| (token.text.as_str(), token.offset_from, token.offset_to))
                .collect::<Vec<_>>(),
            vec![("中", 0, 3), ("文", 3, 6), ("分词", 6, 12)]
        );
    }

    #[test]
    fn test_independent_heads_skip_whitespace() {
        let text = "Hi x";
        let states = [
            FactorizedState {
                action: TermAction::Continue,
                conditions: vec![(0, false)],
            },
            FactorizedState {
                action: TermAction::End,
                conditions: vec![(0, true)],
            },
        ];
        let tokens = decode_independent(
            &[1, 4, 1],
            &[-9.0, 9.0, -9.0, 9.0],
            &[text],
            &[units(text)],
            1,
            &states,
            true,
        )
        .unwrap()
        .remove(0);
        assert_eq!(
            tokens
                .iter()
                .map(|token| token.text.as_str())
                .collect::<Vec<_>>(),
            ["Hi", "x"]
        );
    }

    #[test]
    fn test_explicit_alignment_groups_source_units() {
        let text = "HanLP中文";
        let aligned = [
            UnitSpan { from: 0, to: 3 },
            UnitSpan { from: 3, to: 5 },
            UnitSpan { from: 5, to: 8 },
            UnitSpan { from: 8, to: 11 },
        ];
        let tokens = tokens_from_actions(
            text,
            &aligned,
            &[
                TermAction::Begin,
                TermAction::End,
                TermAction::Single,
                TermAction::Single,
            ],
            false,
        )
        .unwrap();
        assert_eq!(
            tokens
                .iter()
                .map(|token| token.text.as_str())
                .collect::<Vec<_>>(),
            ["HanLP", "中", "文"]
        );
    }

    #[test]
    fn test_segments_preserve_repeated_positions() {
        let columns = SegmentColumns {
            start: 0,
            end: 1,
            position: 2,
            position_length: 3,
        };
        let tokens = decode_segments(
            &[1, 2, 4],
            &[0, 2, 0, 1, 0, 4, 0, 2],
            &["中文分词"],
            OffsetUnit::UnicodeScalar,
            columns,
        )
        .unwrap()
        .remove(0);
        assert_eq!(tokens[0].text, "中文");
        assert_eq!(tokens[1].text, "中文分词");
        assert_eq!(tokens[0].position, tokens[1].position);
    }

    #[test]
    fn test_crf_uses_transition_scores() {
        let crf = Crf {
            start_transitions: vec![0.0, 0.0],
            end_transitions: vec![0.0, 0.0],
            transitions: vec![vec![0.0, 10.0], vec![0.0, 0.0]],
        };
        assert_eq!(viterbi(&[2.0, 0.0, 2.0, 0.0], 2, 2, &crf).unwrap(), [0, 1]);
    }

    #[test]
    fn test_config_rejects_overflowing_columns() {
        for column in [i64::MAX as usize, usize::MAX] {
            let segments = serde_json::json!({
                "outputs": {"terms": "segments"},
                "representation": {
                    "type": "segments", "output": "terms", "offset_unit": "utf8_byte",
                    "columns": {"start": 0, "end": 1, "position": 2, "position_length": column}
                },
                "decoder": {"type": "identity"},
                "projection": {"type": "term_spans"}
            });
            assert!(OutputDecoder::from_config(serde_json::from_value(segments).unwrap()).is_err());
            let scores = serde_json::json!({
                "outputs": {"scores": "logits", "offsets": "offsets"},
                "representation": {
                    "type": "unit_scores", "output": "scores",
                    "alignment": {
                        "type": "offsets", "output": "offsets", "offset_unit": "utf8_byte",
                        "columns": {"start": 0, "end": column}
                    },
                    "distribution": {"type": "categorical", "classes": ["B", "E"]}
                },
                "decoder": {"type": "argmax"},
                "projection": {"type": "action_sequence", "actions": {"B": "begin", "E": "end"}}
            });
            assert!(OutputDecoder::from_config(serde_json::from_value(scores).unwrap()).is_err());
        }
    }

    #[test]
    fn test_config_uses_training_abstractions() {
        let mut config: serde_json::Value = serde_json::from_str(
            r#"{
                "outputs": {"scores": "boundary_logits"},
                "representation": {
                    "type": "unit_scores",
                    "output": "scores",
                    "alignment": {"type": "unicode_scalar"},
                    "distribution": {
                        "type": "categorical",
                        "classes": ["B", "E", "S", "M"]
                    }
                },
                "decoder": {"type": "argmax"},
                "projection": {
                    "type": "action_sequence",
                    "actions": {
                        "B": "begin",
                        "E": "end",
                        "S": "single",
                        "M": "continue"
                    }
                }
            }"#,
        )
        .unwrap();
        assert!(
            OutputDecoder::from_config(serde_json::from_value(config.clone()).unwrap()).is_ok()
        );
        config["format_version"] = serde_json::json!(2);
        assert!(serde_json::from_value::<OutputDecoderConfig>(config).is_err());
    }

    #[test]
    fn test_inline_crf_config_decodes_terms_and_validates_parameters() {
        let config = serde_json::json!({
            "outputs": {"scores": "boundary_logits"},
            "representation": {
                "type": "unit_scores",
                "output": "scores",
                "alignment": {"type": "unicode_scalar"},
                "distribution": {"type": "categorical", "classes": ["B", "E"]}
            },
            "decoder": {
                "type": "linear_chain_crf",
                "start_transitions": [0.0, 0.0],
                "end_transitions": [0.0, 0.0],
                "transitions": [[0.0, 10.0], [0.0, 0.0]]
            },
            "projection": {
                "type": "action_sequence",
                "actions": {"B": "begin", "E": "end"}
            }
        });
        let parsed = serde_json::from_value(config.clone()).unwrap();
        let decoder = OutputDecoder::from_config(parsed).unwrap();
        let Decoder::UnitScores {
            decoder: UnitDecoder::Categorical { actions, algorithm },
            ..
        } = decoder.decoder
        else {
            panic!("expected a categorical CRF decoder");
        };
        let tokens = decode_categorical(
            &[1, 2, 2],
            &[2.0, 0.0, 2.0, 0.0],
            &["中文"],
            &[units("中文")],
            &actions,
            &algorithm,
            false,
        )
        .unwrap()
        .remove(0);
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].text, "中文");
        assert_eq!((tokens[0].offset_from, tokens[0].offset_to), (0, 6));

        for (field, value) in [
            ("start_transitions", serde_json::json!([0.0])),
            ("end_transitions", serde_json::json!([0.0])),
            ("transitions", serde_json::json!([[0.0, 10.0]])),
            ("transitions", serde_json::json!([[0.0], [0.0]])),
        ] {
            let mut invalid = config.clone();
            invalid["decoder"][field] = value;
            let parsed = serde_json::from_value(invalid).unwrap();
            assert!(OutputDecoder::from_config(parsed).is_err(), "{field}");
        }

        for field in ["start_transitions", "end_transitions", "transitions"] {
            let mut invalid = config.clone();
            invalid["decoder"].as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<OutputDecoderConfig>(invalid).is_err());
        }

        let mut legacy = config.clone();
        legacy["decoder"] = serde_json::json!({
            "type": "linear_chain_crf",
            "transitions": "crf.json"
        });
        assert!(serde_json::from_value::<OutputDecoderConfig>(legacy).is_err());

        let mut unknown = config;
        unknown["decoder"]["unexpected"] = serde_json::json!(true);
        assert!(serde_json::from_value::<OutputDecoderConfig>(unknown).is_err());
    }
}
