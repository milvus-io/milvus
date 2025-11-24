// Simple position-based reranking (no field data)
/// Reduces scores for items further down the result list
#[no_mangle]
pub extern "C" fn rerank(score: f32, rank: i32) -> f32 {
    // Apply position-based decay
    // Items at rank 0 get full score, rank 10 gets ~60% of score, rank 50 gets ~33%
    let decay_factor = 1.0 / (1.0 + 0.05 * rank as f32);
    score * decay_factor
}

/// Rerank with single numeric field (e.g., popularity score)
#[no_mangle]
pub extern "C" fn rerank_with_popularity(score: f32, rank: i32, popularity: f32) -> f32 {
    // Boost score based on popularity
    let popularity_boost = 1.0 + (popularity.max(0.0).ln_1p() / 10.0);
    let position_decay = 1.0 / (1.0 + 0.05 * rank as f32);
    
    score * popularity_boost * position_decay
}

/// Rerank with timestamp field (time decay)
#[no_mangle]
pub extern "C" fn rerank_with_timestamp(score: f32, rank: i32, timestamp: i64) -> f32 {
    // Assume timestamp is Unix timestamp in milliseconds
    // Current time approximation (you'd pass this as a parameter in real usage)
    let current_time = 1704067200000i64; // Jan 1, 2024
    
    let age_days = (current_time - timestamp) as f32 / (1000.0 * 60.0 * 60.0 * 24.0);
    let time_decay = (-0.01 * age_days.max(0.0)).exp();
    let position_decay = 1.0 / (1.0 + 0.02 * rank as f32);
    
    score * time_decay * position_decay
}

/// Complex rerank with multiple fields
#[no_mangle]
pub extern "C" fn rerank_complex(score: f32, rank: i32, popularity: f32, quality: f32) -> f32 {
    // Combine multiple signals
    let popularity_factor = 1.0 + (popularity.max(0.0).ln_1p() / 20.0);
    let quality_factor = 0.7 + 0.6 * quality.max(0.0).min(1.0);
    let position_factor = 1.0 / (1.0 + 0.04 * rank as f32);
    
    // Non-linear combination
    let combined_boost = (popularity_factor * quality_factor).sqrt();
    
    score * combined_boost * position_factor
}

/// Time-decay reranking example
/// Simulates time decay where rank represents age
#[no_mangle]
pub extern "C" fn time_decay_rerank(score: f32, rank: i32) -> f32 {
    // Exponential decay based on position
    let time_decay = (-0.01 * rank as f32).exp();
    score * time_decay * 1.2 // 20% boost overall
}

/// Logarithmic boost reranking
/// Provides more dramatic changes for top results
#[no_mangle]
pub extern "C" fn log_boost_rerank(score: f32, rank: i32) -> f32 {
    let position_boost = 1.0 + (1.0 / (1.0 + (rank as f32).ln_1p()));
    score * position_boost
}

/// Simple boost - multiply all scores by 1.5
#[no_mangle]
pub extern "C" fn simple_boost(score: f32, _rank: i32) -> f32 {
    score * 1.5
}

/// Non-linear transformation with clamping
#[no_mangle]
pub extern "C" fn nonlinear_rerank(score: f32, rank: i32) -> f32 {
    // Apply sigmoid-like transformation
    let position_weight = 1.0 / (1.0 + (-0.1 * (rank as f32 - 5.0)).exp());
    let transformed = score.powf(0.8) * position_weight;
    
    // Clamp to [0, 1] range
    transformed.max(0.0).min(1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rerank() {
        let score = 0.8;
        assert_eq!(rerank(score, 0), 0.8); // Rank 0 should be unchanged
        assert!(rerank(score, 10) < score); // Higher ranks get lower scores
    }

    #[test]
    fn test_time_decay() {
        let score = 0.9;
        let rank0 = time_decay_rerank(score, 0);
        let rank10 = time_decay_rerank(score, 10);
        assert!(rank0 > rank10); // Older items get lower scores
    }

    #[test]
    fn test_nonlinear() {
        let result = nonlinear_rerank(0.8, 5);
        assert!(result >= 0.0 && result <= 1.0); // Should be clamped
    }
}

