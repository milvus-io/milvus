use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use tantivy_binding::analyzer::create_analyzer;

struct CountingAllocator;

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = System.alloc(layout);
        if !pointer.is_null() {
            LIVE_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        System.dealloc(pointer, layout);
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let replacement = System.realloc(pointer, layout, size);
        if !replacement.is_null() {
            LIVE_BYTES.fetch_add(size, Ordering::Relaxed);
            LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        replacement
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Ok(path) = std::env::var("LINDERA_MEMORY_DICT_ROOT") {
        tantivy_binding::analyzer::set_options(
            &serde_json::json!({"default_dict_path": path}).to_string(),
        )?;
    }
    let mut args = std::env::args().skip(1);
    let count: usize = args.next().unwrap_or_else(|| "1".into()).parse()?;
    let nested = match args.next().as_deref() {
        None | Some("direct") => false,
        Some("nested") => true,
        _ => return Err("mode must be direct or nested".into()),
    };
    if count == 0 || count > 1000 || args.next().is_some() {
        return Err("usage: lindera_memory [1..1000] [direct|nested]".into());
    }
    let params = if nested {
        r#"{"tokenizer":{"type":"language_identifier","analyzers":{"default":{"tokenizer":"standard"},"Japanese":{"tokenizer":{"type":"lindera","dict_kind":"ipadic"}}}}}"#
    } else {
        r#"{"tokenizer":{"type":"lindera","dict_kind":"ipadic"}}"#
    };
    let before = LIVE_BYTES.load(Ordering::Relaxed);
    let mut analyzers = Vec::with_capacity(count);
    for _ in 0..count {
        analyzers.push(create_analyzer(params, "")?);
    }
    let retained = LIVE_BYTES.load(Ordering::Relaxed);
    std::hint::black_box(&analyzers);
    println!(
        "analyzers={count} nested={nested} rust_live_bytes_delta={}",
        retained as i128 - before as i128
    );
    Ok(())
}
