use regex_automata::{
    meta::{Cache, Regex},
    Input,
};

/// Shares only the compiled regex. Each tokenizer owns at most one search cache,
/// even when successive calls run on different native threads (e.g. via CGO).
/// Token streams borrow this matcher exclusively for their entire lifetime.
pub(super) struct FixedRegex {
    regex: &'static Regex,
    // Allocate on first use, so unused tokenizer prototypes have no cache.
    cache: Option<Box<Cache>>,
}

impl FixedRegex {
    pub(super) fn new(regex: &'static Regex) -> Self {
        Self { regex, cache: None }
    }

    pub(super) fn is_match(&mut self, text: &str) -> bool {
        let regex = self.regex;
        let cache = self
            .cache
            .get_or_insert_with(|| Box::new(regex.create_cache()));
        // Same search mode as regex::Regex::is_match, without its cache pool.
        regex
            .search_half_with(cache, &Input::new(text).earliest(true))
            .is_some()
    }
}

impl Clone for FixedRegex {
    fn clone(&self) -> Self {
        // Do not share or copy mutable search state, including a warm cache.
        Self::new(self.regex)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use once_cell::sync::Lazy;

    static REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"[\p{Han}a-zA-Z0-9]+").unwrap());

    #[test]
    fn clones_have_independent_lazy_caches() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<FixedRegex>();

        let mut original = FixedRegex::new(&REGEX);
        assert!(original.cache.is_none());
        assert!(original.is_match("中文"));
        let mut cloned = original.clone();
        assert!(std::ptr::eq(original.regex, cloned.regex));
        assert!(cloned.cache.is_none());
        assert!(cloned.is_match("ASCII"));
        assert!(!std::ptr::eq(
            original.cache.as_deref().unwrap(),
            cloned.cache.as_deref().unwrap()
        ));
        drop(original);
        assert!(!cloned.is_match("😀"));
    }

    #[test]
    fn cache_moves_with_matcher_between_threads() {
        let mut matcher = FixedRegex::new(&REGEX);
        assert!(matcher.is_match("中文"));
        let cache_address = matcher.cache.as_deref().unwrap() as *const Cache as usize;
        for _ in 0..8 {
            matcher = std::thread::spawn(move || {
                assert!(!matcher.is_match("😀"));
                assert!(matcher.is_match("ABC123"));
                assert_eq!(
                    matcher.cache.as_deref().unwrap() as *const Cache as usize,
                    cache_address
                );
                matcher
            })
            .join()
            .unwrap();
        }
    }
}
