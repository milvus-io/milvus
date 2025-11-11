/// WASM reranker with field data access
/// These functions show how to use additional field data in reranking

/// Simple rerank with single numeric field (e.g., popularity score)
#[no_mangle]
pub extern "C" fn rerank_with_popularity(score: f32, rank: i32, popularity: f32) -> f32 {
    // Boost score based on popularity
    let popularity_boost = 1.0 + (popularity.ln_1p() / 10.0);
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
    let time_decay = (-0.01 * age_days).exp();
    let position_decay = 1.0 / (1.0 + 0.02 * rank as f32);
    
    score * time_decay * position_decay
}

/// Rerank with quality score field
#[no_mangle]
pub extern "C" fn rerank_with_quality(score: f32, rank: i32, quality: f32) -> f32 {
    // Quality should be in [0, 1] range
    let quality_clamped = quality.max(0.0).min(1.0);
    let quality_boost = 0.5 + quality_clamped; // Range [0.5, 1.5]
    let position_decay = 1.0 / (1.0 + 0.03 * rank as f32);
    
    score * quality_boost * position_decay
}

/// Complex rerank with multiple factors
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

/// Rerank with category-based boosting (using integer field)
#[no_mangle]
pub extern "C" fn rerank_with_category(score: f32, rank: i32, category_id: i32) -> f32 {
    // Different boosts for different categories
    let category_boost = match category_id {
        1 => 1.5,  // Premium category
        2 => 1.2,  // Featured category  
        3 => 1.0,  // Standard category
        4 => 0.8,  // Discounted category
        _ => 1.0,  // Unknown category
    };
    
    let position_decay = 1.0 / (1.0 + 0.05 * rank as f32);
    score * category_boost * position_decay
}

/// Example with string field processing (simplified)
/// Note: String handling in WASM is more complex and would typically
/// require memory management functions
#[no_mangle]
pub extern "C" fn rerank_with_string_length(score: f32, rank: i32, text_length: i32) -> f32 {
    // Boost based on text length (assuming longer = more detailed = better)
    let length_factor = if text_length > 0 {
        1.0 + (text_length as f32).ln() / 50.0
    } else {
        1.0
    };
    
    let position_decay = 1.0 / (1.0 + 0.05 * rank as f32);
    score * length_factor.min(2.0) * position_decay // Cap at 2x boost
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rerank_with_popularity() {
        let score = 0.8;
        let rank = 5;
        let popularity = 100.0;
        
        let result = rerank_with_popularity(score, rank, popularity);
        assert!(result > score); // Should boost the score
    }

    #[test]
    fn test_rerank_with_timestamp() {
        let score = 0.7;
        let rank = 3;
        let recent_timestamp = 1704067200000 - 86400000; // 1 day ago
        let old_timestamp = 1704067200000 - 86400000 * 30; // 30 days ago
        
        let recent_result = rerank_with_timestamp(score, rank, recent_timestamp);
        let old_result = rerank_with_timestamp(score, rank, old_timestamp);
        
        assert!(recent_result > old_result); // Recent should score higher
    }

    #[test]
    fn test_rerank_with_category() {
        let score = 0.6;
        let rank = 2;
        
        let premium = rerank_with_category(score, rank, 1);
        let standard = rerank_with_category(score, rank, 3);
        let discounted = rerank_with_category(score, rank, 4);
        
        assert!(premium > standard);
        assert!(standard > discounted);
    }
}
