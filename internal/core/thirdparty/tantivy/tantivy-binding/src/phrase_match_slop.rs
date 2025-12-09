use crate::analyzer::create_analyzer_by_json;
use serde_json::{self, Value};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

#[derive(Eq, PartialEq)]
struct HeapNode {
    value: i64,
    list_index: usize,
    element_index: usize,
}

impl Ord for HeapNode {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap logic: we want the smallest value at the top,
        // but BinaryHeap is a max-heap. So we reverse the comparison.
        other.value.cmp(&self.value)
    }
}

impl PartialOrd for HeapNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub fn compute_phrase_match_slop(
    tokenizer_params: &str,
    query_text: &str,
    data_text: &str,
) -> Result<u32, String> {
    // 1. Parse tokenizer params
    let params_val: Value = serde_json::from_str(tokenizer_params)
        .map_err(|e| format!("Failed to parse tokenizer params: {}", e))?;

    let params_obj = params_val
        .as_object()
        .ok_or("Tokenizer params must be a JSON object")?;

    // 2. Create Analyzer
    let mut analyzer = create_analyzer_by_json(params_obj)
        .map_err(|e| format!("Failed to create analyzer: {:?}", e))?;

    // 3. Tokenize Query
    // We store (term, token_position)
    let mut query_terms: Vec<(String, usize)> = Vec::new();
    {
        let mut stream = analyzer.token_stream(query_text);
        while stream.advance() {
            let token = stream.token();
            query_terms.push((token.text.clone(), token.position));
        }
    }

    if query_terms.is_empty() {
        // If query is empty, slop is technically 0 or undefined.
        // Usually implies match everything.
        return Ok(0);
    }

    // Calculate Max Query Position for Reverse Position Logic
    // Assuming tokens usually come in order, but max() is safer.
    let max_query_pos = query_terms.iter().map(|(_, pos)| *pos).max().unwrap_or(0); // Safe because check empty above

    // 4. Tokenize Data and Store Positions
    // We map Term -> Vec<Position>
    let mut data_positions: HashMap<String, Vec<u32>> = HashMap::new();
    {
        let mut stream = analyzer.token_stream(data_text);
        while stream.advance() {
            let token = stream.token();
            data_positions
                .entry(token.text.clone())
                .or_insert_with(Vec::new)
                .push(token.position as u32);
        }
    }

    // 5. Build Lists for "Smallest Range" Algorithm
    // For each query term, we create a list of calculated values:
    // value = token_position_in_data + reverse_position_in_query
    // New logic: Reverse Pos = max_query_pos - current_query_term_pos
    let mut lists: Vec<Vec<i64>> = Vec::new();

    for (term, q_pos) in query_terms.iter() {
        let reverse_pos = (max_query_pos as i64) - (*q_pos as i64);

        match data_positions.get(term) {
            Some(positions) => {
                // Create the adjusted position list
                let mut list: Vec<i64> = positions
                    .iter()
                    .map(|&pos| pos as i64 + reverse_pos)
                    .collect();

                // Ensure sorted
                list.sort();
                lists.push(list);
            }
            None => {
                // If a query term is missing in data, we cannot form the phrase.
                return Err(format!("Term '{}' not found in data", term));
            }
        }
    }

    // 6. Find Smallest Range covering at least one element from each list
    // We use a Min-Heap to track the current minimum of the selected elements from each list.

    let mut min_heap = BinaryHeap::new();
    let mut current_max = i64::MIN;

    // Initialize heap with the first element of each list
    for (i, list) in lists.iter().enumerate() {
        if list.is_empty() {
            return Err(format!(
                "Term '{}' found but position list is empty",
                query_terms[i].0
            ));
        }
        let val = list[0];
        min_heap.push(HeapNode {
            value: val,
            list_index: i,
            element_index: 0,
        });
        if val > current_max {
            current_max = val;
        }
    }

    let mut min_slop = i64::MAX;

    loop {
        let min_node = match min_heap.pop() {
            Some(n) => n,
            None => break,
        };

        let current_slop = current_max - min_node.value;

        if current_slop < min_slop {
            min_slop = current_slop;
        }

        if min_slop == 0 {
            break;
        }

        let next_idx = min_node.element_index + 1;
        let list_idx = min_node.list_index;

        if next_idx < lists[list_idx].len() {
            let next_val = lists[list_idx][next_idx];
            if next_val > current_max {
                current_max = next_val;
            }

            min_heap.push(HeapNode {
                value: next_val,
                list_index: list_idx,
                element_index: next_idx,
            });
        } else {
            break;
        }
    }

    Ok(min_slop as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_slop_example() {
        let tokenizer_params = r#"{ "type": "standard" }"#;
        let query = "I like swimming and skiing";

        // 10 'x's.
        let padding = "x ".repeat(10);
        let data = format!(
            "{}Skiing and swimming are sports that I like the most.",
            padding
        );

        let result = compute_phrase_match_slop(tokenizer_params, query, &data);
        assert_eq!(result, Ok(10));
    }

    #[test]
    fn test_compute_slop_chinese() {
        let params = "{\"tokenizer\": \"jieba\"}".to_string();
        let query = "中文 测试";
        let data = "测试 的 中文";

        let result = compute_phrase_match_slop(&params, query, data);
        assert_eq!(result, Ok(8));
    }

    #[test]
    fn test_exact_match() {
        let tokenizer_params = r#"{ "type": "standard" }"#;
        let query = "hello world";
        let data = "hello world";
        let result = compute_phrase_match_slop(tokenizer_params, query, data);
        assert_eq!(result, Ok(0));
    }

    #[test]
    fn test_missing_term() {
        let tokenizer_params = r#"{ "type": "standard" }"#;
        let query = "hello world";
        let data = "hello there";
        let result = compute_phrase_match_slop(tokenizer_params, query, data);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_query() {
        let tokenizer_params = r#"{ "type": "standard" }"#;
        let query = "";
        let data = "some data";
        let result = compute_phrase_match_slop(tokenizer_params, query, data);
        assert_eq!(result, Ok(0));
    }

    #[test]
    fn test_repeated_terms_in_data() {
        let tokenizer_params = r#"{ "type": "standard" }"#;
        let query = "A B";
        let data = "A C D E B F G A B";
        let result = compute_phrase_match_slop(tokenizer_params, query, data);
        assert_eq!(result, Ok(0));
    }

    #[test]
    fn test_repeated_terms_in_query() {
        let tokenizer_params = r#"{ "type": "standard" }"#;
        let query = "A B A";
        let data = "A B A";
        let result = compute_phrase_match_slop(tokenizer_params, query, data);
        assert_eq!(result, Ok(0));
    }

    #[test]
    fn test_interleaved_match() {
        let tokenizer_params = r#"{ "type": "standard" }"#;
        let query = "A C";
        let data = "A B C";
        let result = compute_phrase_match_slop(tokenizer_params, query, data);
        assert_eq!(result, Ok(1));
    }
}
