use std::collections::HashSet;
use std::sync::Arc;

use ort::value::{DynValue, Tensor};
use prost::Message;

use crate::error::{Result, TantivyBindingError};

// Read only initializer fields; ORT remains responsible for the complete ONNX graph.
// Field numbers follow https://github.com/onnx/onnx/blob/v1.19.1/onnx/onnx.proto.
#[derive(Message)]
struct Model {
    #[prost(message, optional, tag = "7")]
    graph: Option<Graph>,
}

#[derive(Message)]
struct Graph {
    #[prost(message, repeated, tag = "5")]
    initializer: Vec<Initializer>,
    #[prost(message, repeated, tag = "11")]
    input: Vec<Input>,
}

#[derive(Message)]
struct Input {
    #[prost(string, tag = "1")]
    name: String,
}

#[derive(Message)]
struct Initializer {
    #[prost(int64, repeated, tag = "1")]
    dims: Vec<i64>,
    #[prost(int32, tag = "2")]
    data_type: i32,
    #[prost(bytes, optional, tag = "3")]
    segment: Option<Vec<u8>>,
    #[prost(float, repeated, tag = "4")]
    float_data: Vec<f32>,
    #[prost(int64, repeated, tag = "7")]
    int64_data: Vec<i64>,
    #[prost(string, tag = "8")]
    name: String,
    #[prost(bytes, tag = "9")]
    raw_data: Vec<u8>,
    #[prost(int32, tag = "14")]
    data_location: i32,
}

fn data<T, const N: usize>(
    raw: &[u8],
    typed: Vec<T>,
    elements: usize,
    decode: fn([u8; N]) -> T,
) -> Option<Vec<T>> {
    if raw.is_empty() {
        return (typed.len() == elements).then_some(typed);
    }
    if elements.checked_mul(N)? != raw.len() {
        return None;
    }
    Some(
        raw.chunks_exact(N)
            .map(|v| decode(v.try_into().unwrap()))
            .collect(),
    )
}

pub(super) fn shared_initializers(model: &[u8]) -> Result<Vec<(String, Arc<DynValue>)>> {
    let invalid = |message| {
        TantivyBindingError::InvalidArgument(format!("invalid model tokenizer ONNX: {message}"))
    };
    let graph = Model::decode(model)
        .map_err(|error| invalid(error.to_string()))?
        .graph
        .ok_or_else(|| invalid("missing graph".to_string()))?;
    let inputs = graph
        .input
        .into_iter()
        .map(|input| input.name)
        .collect::<HashSet<_>>();
    let mut names = HashSet::new();
    let mut weights = Vec::new();
    for tensor in graph.initializer {
        if tensor.name.is_empty() || !names.insert(tensor.name.clone()) {
            return Err(invalid("empty or duplicate initializer name".to_string()));
        }
        // shortcut: share embedded FP32/INT64 weights; extend for quantized/FP16 models
        // when needed. Other types, external data and overridable inputs stay with ORT.
        if !matches!(tensor.data_type, 1 | 7)
            || tensor.data_location != 0
            || tensor.segment.is_some()
            || inputs.contains(&tensor.name)
        {
            continue;
        }
        let shape = tensor
            .dims
            .iter()
            .map(|&dim| usize::try_from(dim))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|_| invalid(format!("negative dimension for {}", tensor.name)))?;
        let elements = shape
            .iter()
            .try_fold(1usize, |size, &dim| size.checked_mul(dim))
            .ok_or_else(|| invalid(format!("initializer {} is too large", tensor.name)))?;
        let bad_data = || {
            invalid(format!(
                "initializer {} data does not match its shape",
                tensor.name
            ))
        };
        let value = match tensor.data_type {
            1 => Tensor::from_array((
                shape,
                data(
                    &tensor.raw_data,
                    tensor.float_data,
                    elements,
                    f32::from_le_bytes,
                )
                .ok_or_else(bad_data)?,
            ))
            .map(|v| v.into_dyn()),
            7 => Tensor::from_array((
                shape,
                data(
                    &tensor.raw_data,
                    tensor.int64_data,
                    elements,
                    i64::from_le_bytes,
                )
                .ok_or_else(bad_data)?,
            ))
            .map(|v| v.into_dyn()),
            _ => unreachable!(),
        }
        .map_err(|error| {
            TantivyBindingError::InternalError(format!(
                "failed to share model tokenizer initializer: {error}"
            ))
        })?;
        weights.push((tensor.name, Arc::new(value)));
    }
    Ok(weights)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initializer_data_formats_and_bounds() {
        assert_eq!(
            data(&[], vec![1.5f32, -2.0], 2, f32::from_le_bytes),
            Some(vec![1.5, -2.0])
        );
        assert_eq!(
            data(&42i64.to_le_bytes(), vec![], 1, i64::from_le_bytes),
            Some(vec![42])
        );
        assert_eq!(data(&[], vec![3i64], 1, i64::from_le_bytes), Some(vec![3]));
        assert_eq!(data(&[], vec![], 0, f32::from_le_bytes), Some(vec![]));
        assert!(data(&[0; 3], vec![], 1, f32::from_le_bytes).is_none());
        assert!(data(&[], vec![1i64], 2, i64::from_le_bytes).is_none());
        assert!(data(&[0], vec![], usize::MAX, i64::from_le_bytes).is_none());
    }

    #[test]
    fn test_invalid_initializer_metadata() {
        assert!(shared_initializers(b"not protobuf").is_err());
        assert!(shared_initializers(&Model::default().encode_to_vec()).is_err());
        for dims in [vec![-1], vec![i64::MAX, 3], vec![2]] {
            let model = Model {
                graph: Some(Graph {
                    initializer: vec![Initializer {
                        name: "weight".into(),
                        data_type: 1,
                        dims,
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            };
            assert!(shared_initializers(&model.encode_to_vec()).is_err());
        }
    }

    #[test]
    fn test_unshared_initializers_and_duplicate_names() {
        let tensors = vec![
            Initializer {
                name: "half".into(),
                data_type: 10,
                ..Default::default()
            },
            Initializer {
                name: "external".into(),
                data_type: 1,
                data_location: 1,
                ..Default::default()
            },
            Initializer {
                name: "overridable".into(),
                data_type: 1,
                ..Default::default()
            },
        ];
        let mut model = Model {
            graph: Some(Graph {
                initializer: tensors,
                input: vec![Input {
                    name: "overridable".into(),
                }],
            }),
        };
        assert!(shared_initializers(&model.encode_to_vec())
            .unwrap()
            .is_empty());
        model.graph.as_mut().unwrap().initializer.push(Initializer {
            name: "half".into(),
            ..Default::default()
        });
        assert!(shared_initializers(&model.encode_to_vec()).is_err());
    }
}
