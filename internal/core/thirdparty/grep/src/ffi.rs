//! C ABI ownership, validation and unwind boundaries. Rust matching lives in
//! Plan/Session; no Tantivy, Go or database types enter this module.
#![deny(unsafe_op_in_unsafe_fn)]

use std::{
    ffi::c_char,
    fmt::{self, Write},
    mem::{align_of, size_of},
    panic::{catch_unwind, AssertUnwindSafe},
    ptr, slice,
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex, TryLockError,
    },
};

use crate::{
    session::ResultOverhead, BatchResult, Boundary, CancellationToken, CaseMode, CompileLimits,
    Error, ExecutionLimits, LimitKind, MatchOptions, PatternKind, Plan, Session, TextResult,
};

pub type MgGrepStatus = u32;
pub const MG_GREP_OK: MgGrepStatus = 0;
pub const MG_GREP_INVALID_ARGUMENT: MgGrepStatus = 1;
pub const MG_GREP_INVALID_PATTERN: MgGrepStatus = 2;
pub const MG_GREP_INVALID_UTF8: MgGrepStatus = 3;
pub const MG_GREP_RESOURCE_LIMIT: MgGrepStatus = 4;
pub const MG_GREP_CANCELLED: MgGrepStatus = 5;
pub const MG_GREP_INTERNAL_ERROR: MgGrepStatus = 7;
/// Upstream compiler failure with an ambiguous cause. Never classify by text.
pub const MG_GREP_ENGINE_BUILD_ERROR: MgGrepStatus = 8;
/// Actual allocation failure, distinct from a deterministic policy limit.
pub const MG_GREP_ALLOCATION_ERROR: MgGrepStatus = 9;

pub const MG_GREP_PATTERN_REGEX: u32 = 0;
pub const MG_GREP_PATTERN_LITERAL: u32 = 1;
pub const MG_GREP_CASE_SENSITIVE: u32 = 0;
pub const MG_GREP_CASE_INSENSITIVE: u32 = 1;
pub const MG_GREP_BOUNDARY_NONE: u32 = 0;
pub const MG_GREP_BOUNDARY_WORD: u32 = 1;
pub const MG_GREP_BOUNDARY_LINE: u32 = 2;

/// Optional caller-owned UTF-8 diagnostic; always NUL terminated. Success
/// clears it. Messages may be truncated; branch on status, never on text.
#[repr(C)]
pub struct MgGrepError {
    pub message: [c_char; 512],
}

/// Borrowed input or result-owned output bytes; never a C string.
/// NULL is accepted only with len == 0. Field text may contain NUL.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct MgGrepBytes {
    pub data: *const u8,
    pub len: usize,
}

/// Zero initialization means regex, sensitive, no boundary restriction.
/// Unknown values are rejected. Unicode and LF line semantics are fixed.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct MgGrepMatchOptions {
    pub pattern_kind: u32,
    pub case_mode: u32,
    pub boundary: u32,
}

/// At least one pattern; ordered OR. Empty patterns are valid. No literal
/// LF/NUL in patterns. No CLI string, database identity or compiled state.
#[repr(C)]
pub struct MgGrepSpecView {
    pub patterns: *const MgGrepBytes,
    pub pattern_count: usize,
    pub options: MgGrepMatchOptions,
}

/// Positive service-supplied limits. Engine limits do not bound total RSS.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct MgGrepCompileLimits {
    pub max_regex_bytes: u64,
    pub max_dfa_cache_bytes: u64,
}

/// Positive result-byte budget including core data, C view arrays, handle
/// metadata and text, but excluding allocation overhead and temporary buffers.
/// Execution is controlled by the session cancellation token.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct MgGrepExecutionLimits {
    pub max_result_bytes: u64,
}

/// UTF-8 byte interval [start,end) relative to the owning line.content.
/// Empty matches have start == end; endpoints are character boundaries.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct MgGrepSpan {
    pub start: u64,
    pub end: u64,
}

/// Complete matching line, minus only its terminal LF. Other bytes remain.
/// Every returned line has at least one span. Empty content is {NULL,0}.
#[repr(C)]
pub struct MgGrepLine {
    pub content: MgGrepBytes,
    pub spans: *const MgGrepSpan,
    pub span_count: usize,
}

/// One entry per input. line_count > 0 means selected; lines are in original
/// order. Empty arrays have NULL pointers. No silently truncated results.
#[repr(C)]
pub struct MgGrepTextResult {
    pub lines: *const MgGrepLine,
    pub line_count: usize,
}

#[repr(C)]
pub struct MgGrepBatchView {
    pub texts: *const MgGrepTextResult,
    pub text_count: usize,
}

pub struct MgGrepPlan {
    plan: Plan,
}

pub struct MgGrepSession {
    // Only shared references to this handle are formed by execute and cancel.
    // Mutable engine access is separately synchronized; cancel never locks it.
    inner: Mutex<Session>,
    cancellation: CancellationToken,
    poisoned: AtomicBool,
}

pub struct MgGrepResult {
    view: MgGrepBatchView,
    _data: BatchResult,
    _texts: Box<[MgGrepTextResult]>,
    _lines: Box<[MgGrepLine]>,
    _spans: Box<[MgGrepSpan]>,
}

/// Validate/compile an execution-local typed specification.
/// Required: spec, limits, out_plan. Copies/owns all retained input data.
/// On failure a valid out_plan slot is NULL. The caller owns a successful Plan.
/// # Safety
/// Pointers obey the header's memory/aliasing rules. error is optional.
#[no_mangle]
pub unsafe extern "C" fn mg_grep_compile(
    spec: *const MgGrepSpecView,
    limits: *const MgGrepCompileLimits,
    out_plan: *mut *mut MgGrepPlan,
    error: *mut MgGrepError,
) -> MgGrepStatus {
    boundary(error, None, || {
        // SAFETY: all access goes through checked pointer/extent helpers;
        // validity of non-NULL memory is the caller's contract.
        unsafe {
            reset_output(out_plan)?;
            let limits = compile_limits(*required(limits)?)?;
            let spec = required(spec)?;
            if spec.pattern_count == 0 {
                return Err(invalid("at least one pattern is required"));
            }
            let options = options(spec.options)?;
            let descriptors = checked_slice(spec.patterns, spec.pattern_count)?;
            let patterns = inputs(descriptors, || Ok(()))?;
            let plan = Plan::compile(&patterns, options, limits)?;
            out_plan.write(Box::into_raw(Box::new(MgGrepPlan { plan })));
            Ok(())
        }
    })
}

/// NULL is a no-op. Existing Sessions retain the Plan. Does not wait.
/// # Safety
/// Own this live handle; no session_create may be using it concurrently.
#[no_mangle]
pub unsafe extern "C" fn mg_grep_plan_free(plan: *mut MgGrepPlan) {
    discard_unwind(|| {
        if !plan.is_null() {
            unsafe { drop(Box::from_raw(plan)) };
        }
    });
}

/// Required: plan, out_session. A valid output slot is NULL on failure.
/// # Safety
/// plan is a live handle; out_session/error obey the header's memory rules.
#[no_mangle]
pub unsafe extern "C" fn mg_grep_session_create(
    plan: *const MgGrepPlan,
    out_session: *mut *mut MgGrepSession,
    error: *mut MgGrepError,
) -> MgGrepStatus {
    boundary(error, None, || unsafe {
        reset_output(out_session)?;
        let inner = Session::new(&required(plan)?.plan);
        let session = MgGrepSession {
            cancellation: inner.cancellation_token(),
            inner: Mutex::new(inner),
            poisoned: AtomicBool::new(false),
        };
        out_session.write(Box::into_raw(Box::new(session)));
        Ok(())
    })
}

/// Sticky, idempotent cancellation, including future batches. NULL is a no-op.
/// May run concurrently with execute; does not revoke published results.
/// # Safety
/// Keep the Session alive until this call returns; do not race with free.
#[no_mangle]
pub unsafe extern "C" fn mg_grep_session_cancel(session: *mut MgGrepSession) {
    discard_unwind(|| {
        if let Ok(session) = unsafe { required(session) } {
            session.cancellation.cancel();
        }
    });
}

/// NULL is a no-op. Does not cancel, wait for or join active work.
/// # Safety
/// Own the live handle; all execute/cancel calls must already have returned.
#[no_mangle]
pub unsafe extern "C" fn mg_grep_session_free(session: *mut MgGrepSession) {
    discard_unwind(|| {
        if !session.is_null() {
            unsafe { drop(Box::from_raw(session)) };
        }
    });
}

/// Synchronous batch match. Required: session, limits, out_result. Empty batch
/// accepts texts == NULL, subject to result budget and cancellation. {NULL,0} is empty
/// text, not database NULL. On failure a valid output slot is NULL.
/// # Safety
/// Inputs live through return and obey the header's memory rules. A Session
/// cannot execute concurrent batches; callers synchronize all handle releases.
#[no_mangle]
pub unsafe extern "C" fn mg_grep_execute_batch(
    session: *mut MgGrepSession,
    texts: *const MgGrepBytes,
    text_count: usize,
    limits: *const MgGrepExecutionLimits,
    out_result: *mut *mut MgGrepResult,
    error: *mut MgGrepError,
) -> MgGrepStatus {
    // required never dereferences NULL/misaligned pointers. Address validity
    // remains the caller's obligation. Only a shared handle reference is used.
    let session = unsafe { required(session) };
    let poison = session.as_ref().ok().map(|s| &s.poisoned);
    boundary(error, poison, || unsafe {
        reset_output(out_result)?;
        let session = session?;
        if session.poisoned.load(Ordering::Acquire) {
            return Err(Error::Internal("session is poisoned"));
        }
        let limits = execution_limits(*required(limits)?)?;
        let check = || check_execution(session);
        check()?;
        let mut inner = match session.inner.try_lock() {
            Ok(guard) => guard,
            Err(TryLockError::WouldBlock) => return Err(invalid("concurrent batch execution")),
            Err(TryLockError::Poisoned(_)) => {
                return Err(Error::Internal("session lock is poisoned"))
            }
        };
        let base = text_count
            .checked_mul(size_of::<TextResult>() + size_of::<MgGrepTextResult>())
            .and_then(|n| n.checked_add(size_of::<MgGrepResult>()))
            .ok_or(Error::ResourceLimit(LimitKind::ResultBytes))?;
        if base as u64 > limits.max_result_bytes {
            return Err(Error::ResourceLimit(LimitKind::ResultBytes));
        }
        let descriptors = checked_slice(texts, text_count)?;
        let texts = inputs(descriptors, check)?;
        let data = inner.execute_accounted(
            &texts,
            limits,
            ResultOverhead {
                batch: size_of::<MgGrepResult>() - size_of::<BatchResult>(),
                text: size_of::<MgGrepTextResult>(),
                line: size_of::<MgGrepLine>(),
                span: size_of::<MgGrepSpan>(),
            },
        )?;
        let result = make_result(data, check)?;
        check()?; // Publication linearization includes view construction.
        out_result.write(Box::into_raw(result));
        Ok(())
    })
}

/// NULL yields NULL. All nested pointers remain stable until result_free and
/// are independent of input, Session and Plan lifetimes. Readers may share.
/// # Safety
/// A non-NULL handle is live; synchronize result_free with all readers.
#[no_mangle]
pub unsafe extern "C" fn mg_grep_result_view(
    result: *const MgGrepResult,
) -> *const MgGrepBatchView {
    match unsafe { required(result) } {
        Ok(result) => &result.view,
        Err(_) => ptr::null(),
    }
}

/// NULL is a no-op; frees every view and all owned result data. Does not wait.
/// # Safety
/// Own the live handle and ensure all view readers have finished.
#[no_mangle]
pub unsafe extern "C" fn mg_grep_result_free(result: *mut MgGrepResult) {
    discard_unwind(|| {
        if !result.is_null() {
            unsafe { drop(Box::from_raw(result)) };
        }
    });
}

fn make_result(
    data: BatchResult,
    mut check: impl FnMut() -> Result<(), Error>,
) -> Result<Box<MgGrepResult>, Error> {
    let mut spans = Vec::new();
    for text in &data.texts {
        check()?;
        for line in &text.lines {
            check()?;
            spans.try_reserve(line.spans.len())?;
            for span in &line.spans {
                check()?;
                spans.push(MgGrepSpan {
                    start: span.start,
                    end: span.end,
                });
            }
        }
    }
    let spans = spans.into_boxed_slice();
    let mut lines = Vec::new();
    let mut span_at = 0;
    for text in &data.texts {
        check()?;
        lines.try_reserve(text.lines.len())?;
        for line in &text.lines {
            check()?;
            let n = line.spans.len();
            lines.push(MgGrepLine {
                content: MgGrepBytes {
                    data: nullable(line.content.as_bytes()),
                    len: line.content.len(),
                },
                spans: nullable(&spans[span_at..span_at + n]),
                span_count: n,
            });
            span_at += n;
        }
    }
    let lines = lines.into_boxed_slice();
    let mut texts = Vec::new();
    texts.try_reserve_exact(data.texts.len())?;
    let mut line_at = 0;
    for text in &data.texts {
        check()?;
        let n = text.lines.len();
        texts.push(MgGrepTextResult {
            lines: nullable(&lines[line_at..line_at + n]),
            line_count: n,
        });
        line_at += n;
    }
    let texts = texts.into_boxed_slice();
    Ok(Box::new(MgGrepResult {
        view: MgGrepBatchView {
            texts: nullable(&texts),
            text_count: texts.len(),
        },
        _data: data,
        _texts: texts,
        _lines: lines,
        _spans: spans,
    }))
}

fn nullable<T>(slice: &[T]) -> *const T {
    if slice.is_empty() {
        ptr::null()
    } else {
        slice.as_ptr()
    }
}
fn invalid(message: &'static str) -> Error {
    Error::InvalidConfiguration(message)
}

unsafe fn required<'a, T>(p: *const T) -> Result<&'a T, Error> {
    if p.is_null() || (p as usize) % align_of::<T>() != 0 {
        return Err(invalid("required pointer is NULL or misaligned"));
    }
    Ok(unsafe { &*p })
}

unsafe fn checked_slice<'a, T>(p: *const T, len: usize) -> Result<&'a [T], Error> {
    let bytes = len
        .checked_mul(size_of::<T>())
        .filter(|&n| n <= isize::MAX as usize)
        .ok_or(invalid("slice length overflow"))?;
    if len == 0 {
        return Ok(&[]);
    }
    if p.is_null()
        || (p as usize) % align_of::<T>() != 0
        || (p as usize).checked_add(bytes).is_none()
    {
        return Err(invalid("invalid slice descriptor"));
    }
    Ok(unsafe { slice::from_raw_parts(p, len) })
}

unsafe fn reset_output<T>(out: *mut *mut T) -> Result<(), Error> {
    if out.is_null() || (out as usize) % align_of::<*mut T>() != 0 {
        return Err(invalid("invalid output slot"));
    }
    unsafe {
        out.write(ptr::null_mut());
    }
    Ok(())
}

unsafe fn inputs<'a>(
    descriptors: &[MgGrepBytes],
    mut check: impl FnMut() -> Result<(), Error>,
) -> Result<Vec<&'a [u8]>, Error> {
    // Validate descriptor bounds before allocating the borrowed-slice vector.
    for d in descriptors {
        check()?;
        unsafe {
            checked_slice(d.data, d.len)?;
        }
    }
    let mut values = Vec::new();
    values.try_reserve_exact(descriptors.len())?;
    for d in descriptors {
        check()?;
        values.push(unsafe { checked_slice(d.data, d.len)? });
    }
    Ok(values)
}

fn options(o: MgGrepMatchOptions) -> Result<MatchOptions, Error> {
    Ok(MatchOptions {
        pattern_kind: match o.pattern_kind {
            0 => PatternKind::Regex,
            1 => PatternKind::Literal,
            _ => return Err(invalid("unknown pattern kind")),
        },
        case_mode: match o.case_mode {
            0 => CaseMode::Sensitive,
            1 => CaseMode::Insensitive,
            _ => return Err(invalid("unknown case mode")),
        },
        boundary: match o.boundary {
            0 => Boundary::None,
            1 => Boundary::Word,
            2 => Boundary::Line,
            _ => return Err(invalid("unknown boundary")),
        },
    })
}

fn compile_limits(l: MgGrepCompileLimits) -> Result<CompileLimits, Error> {
    if l.max_regex_bytes == 0 || l.max_dfa_cache_bytes == 0 {
        return Err(invalid("compile limits must be positive"));
    }
    Ok(CompileLimits {
        max_regex_bytes: usize::try_from(l.max_regex_bytes)
            .map_err(|_| invalid("regex limit exceeds size_t"))?,
        max_dfa_cache_bytes: usize::try_from(l.max_dfa_cache_bytes)
            .map_err(|_| invalid("cache limit exceeds size_t"))?,
    })
}
fn execution_limits(l: MgGrepExecutionLimits) -> Result<ExecutionLimits, Error> {
    if l.max_result_bytes == 0 {
        return Err(invalid("result byte budget must be positive"));
    }
    Ok(ExecutionLimits {
        max_result_bytes: l.max_result_bytes,
    })
}
fn check_execution(s: &MgGrepSession) -> Result<(), Error> {
    if s.cancellation.is_cancelled() {
        return Err(Error::Cancelled);
    }
    Ok(())
}

fn status(e: &Error) -> MgGrepStatus {
    match e {
        Error::InvalidConfiguration(_) => MG_GREP_INVALID_ARGUMENT,
        Error::PatternPolicy(_) | Error::Syntax(_) => MG_GREP_INVALID_PATTERN,
        Error::InvalidUtf8 { .. } => MG_GREP_INVALID_UTF8,
        Error::ResourceLimit(_) => MG_GREP_RESOURCE_LIMIT,
        Error::EngineBuild(_) => MG_GREP_ENGINE_BUILD_ERROR,
        Error::Allocation(_) => MG_GREP_ALLOCATION_ERROR,
        Error::Cancelled => MG_GREP_CANCELLED,
        Error::Internal(_) => MG_GREP_INTERNAL_ERROR,
    }
}

fn boundary(
    error: *mut MgGrepError,
    poison: Option<&AtomicBool>,
    f: impl FnOnce() -> Result<(), Error>,
) -> MgGrepStatus {
    match catch_unwind(AssertUnwindSafe(|| {
        unsafe {
            diagnostic(error, format_args!(""));
        }
        match f() {
            Ok(()) => MG_GREP_OK,
            Err(e) => {
                // Includes a panic already caught/poisoned by the Rust core.
                // Retain this state at the handle boundary so later cancel
                // checks cannot mask the permanently failed Session.
                if matches!(e, Error::Internal(_)) {
                    if let Some(poison) = poison {
                        poison.store(true, Ordering::Release);
                    }
                }
                unsafe {
                    diagnostic(error, format_args!("{e}"));
                }
                status(&e)
            }
        }
    })) {
        Ok(status) => status,
        Err(payload) => {
            if let Some(poison) = poison {
                poison.store(true, Ordering::Release);
            }
            // Even a user-defined panic payload's Drop may panic. Never let
            // a second unwind cross the C boundary; leak only on this path.
            std::mem::forget(payload);
            unsafe {
                diagnostic(error, format_args!("internal Rust panic"));
            }
            MG_GREP_INTERNAL_ERROR
        }
    }
}

fn discard_unwind(f: impl FnOnce()) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(f)) {
        std::mem::forget(payload);
    }
}

unsafe fn diagnostic(out: *mut MgGrepError, args: fmt::Arguments<'_>) {
    if out.is_null() {
        return;
    }
    // C callers may provide uninitialized output storage. Initialize it with
    // a raw write before forming a Rust reference to the struct.
    unsafe {
        out.write(MgGrepError { message: [0; 512] });
    }
    let out = unsafe { &mut *out };
    let mut writer = Diagnostic {
        out: &mut out.message,
        used: 0,
        truncated: false,
    };
    let _ = writer.write_fmt(args);
}
struct Diagnostic<'a> {
    out: &'a mut [c_char; 512],
    used: usize,
    truncated: bool,
}
impl fmt::Write for Diagnostic<'_> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        if self.truncated {
            return Ok(());
        }
        let mut n = s.len().min(511 - self.used);
        while !s.is_char_boundary(n) {
            n -= 1;
        }
        for &b in &s.as_bytes()[..n] {
            self.out[self.used] = b as c_char;
            self.used += 1;
        }
        self.out[self.used] = 0;
        self.truncated = n < s.len();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn pod_layout_matches_the_c11_assertions() {
        use std::mem::offset_of;
        assert_eq!(size_of::<MgGrepError>(), 512);
        assert_eq!(
            (size_of::<MgGrepBytes>(), offset_of!(MgGrepBytes, len)),
            (16, 8)
        );
        assert_eq!(
            (
                size_of::<MgGrepMatchOptions>(),
                offset_of!(MgGrepMatchOptions, boundary)
            ),
            (12, 8)
        );
        assert_eq!(
            (
                size_of::<MgGrepSpecView>(),
                offset_of!(MgGrepSpecView, options)
            ),
            (32, 16)
        );
        assert_eq!(
            (
                size_of::<MgGrepCompileLimits>(),
                offset_of!(MgGrepCompileLimits, max_regex_bytes),
                offset_of!(MgGrepCompileLimits, max_dfa_cache_bytes)
            ),
            (16, 0, 8)
        );
        assert_eq!(
            (
                size_of::<MgGrepExecutionLimits>(),
                offset_of!(MgGrepExecutionLimits, max_result_bytes)
            ),
            (8, 0)
        );
        assert_eq!(
            (size_of::<MgGrepSpan>(), offset_of!(MgGrepSpan, end)),
            (16, 8)
        );
        assert_eq!(
            (
                size_of::<MgGrepLine>(),
                offset_of!(MgGrepLine, spans),
                offset_of!(MgGrepLine, span_count)
            ),
            (32, 16, 24)
        );
        assert_eq!(
            (
                size_of::<MgGrepTextResult>(),
                offset_of!(MgGrepTextResult, line_count)
            ),
            (16, 8)
        );
        assert_eq!(
            (
                size_of::<MgGrepBatchView>(),
                offset_of!(MgGrepBatchView, text_count)
            ),
            (16, 8)
        );
    }

    fn compile(pattern: &[u8]) -> *mut MgGrepSession {
        let pattern = MgGrepBytes {
            data: pattern.as_ptr(),
            len: pattern.len(),
        };
        let spec = MgGrepSpecView {
            patterns: &pattern,
            pattern_count: 1,
            options: MgGrepMatchOptions::default(),
        };
        let limits = MgGrepCompileLimits {
            max_regex_bytes: 1_000_000,
            max_dfa_cache_bytes: 1_000_000,
        };
        unsafe {
            let mut plan = ptr::null_mut();
            assert_eq!(
                mg_grep_compile(&spec, &limits, &mut plan, ptr::null_mut()),
                MG_GREP_OK
            );
            let mut session = ptr::null_mut();
            assert_eq!(
                mg_grep_session_create(plan, &mut session, ptr::null_mut()),
                MG_GREP_OK
            );
            mg_grep_plan_free(plan);
            session
        }
    }

    fn limits() -> MgGrepExecutionLimits {
        MgGrepExecutionLimits {
            max_result_bytes: 100_000_000,
        }
    }

    #[test]
    fn utf8_diagnostic_is_terminated_and_never_splits_characters() {
        let mut error = MgGrepError { message: [1; 512] };
        unsafe {
            diagnostic(&mut error, format_args!("{}", "中".repeat(300)));
        }
        let message = unsafe { std::ffi::CStr::from_ptr(error.message.as_ptr()) }
            .to_str()
            .unwrap();
        assert_eq!(message.len(), 510);
        assert_eq!(boundary(&mut error, None, || Ok(())), MG_GREP_OK);
        assert_eq!(error.message[0], 0);
    }

    #[test]
    fn diagnostic_accepts_uninitialized_c_output_storage() {
        let mut error = std::mem::MaybeUninit::<MgGrepError>::uninit();
        assert_eq!(boundary(error.as_mut_ptr(), None, || Ok(())), MG_GREP_OK);
        let error = unsafe { error.assume_init() };
        assert!(error.message.iter().all(|&byte| byte == 0));
    }

    #[test]
    fn panic_boundary_poison_survives_into_exported_execute() {
        let session = compile(b"a");
        let mut error = MgGrepError { message: [0; 512] };
        let poison = unsafe { &(*session).poisoned };
        assert_eq!(
            boundary(&mut error, Some(poison), || panic!(
                "injected conversion panic"
            )),
            MG_GREP_INTERNAL_ERROR
        );
        unsafe {
            let mut result = ptr::dangling_mut();
            assert_eq!(
                mg_grep_execute_batch(session, ptr::null(), 0, &limits(), &mut result, &mut error),
                MG_GREP_INTERNAL_ERROR
            );
            assert!(result.is_null());
            mg_grep_session_free(session);
        }
    }

    #[test]
    fn allocations_have_a_distinct_status() {
        let source = Vec::<u8>::new().try_reserve(usize::MAX).unwrap_err();
        assert_eq!(
            boundary(ptr::null_mut(), None, || Err(Error::Allocation(source))),
            MG_GREP_ALLOCATION_ERROR
        );
        assert_eq!(
            boundary(ptr::null_mut(), None, || Err(Error::ResourceLimit(
                LimitKind::ResultBytes
            ))),
            MG_GREP_RESOURCE_LIMIT
        );
    }

    #[test]
    fn core_internal_failure_stays_poisoned_even_after_cancel() {
        fn send_sync<T: Send + Sync>() {}
        send_sync::<MgGrepSession>();
        let session = compile(b"a");
        unsafe {
            assert_eq!(
                boundary(ptr::null_mut(), Some(&(*session).poisoned), || Err(
                    Error::Internal("core panic")
                )),
                MG_GREP_INTERNAL_ERROR
            );
            mg_grep_session_cancel(session);
            let mut result = ptr::dangling_mut();
            assert_eq!(
                mg_grep_execute_batch(
                    session,
                    ptr::null(),
                    0,
                    &limits(),
                    &mut result,
                    ptr::null_mut()
                ),
                MG_GREP_INTERNAL_ERROR
            );
            assert!(result.is_null());
            mg_grep_session_free(session);
        }
    }

    #[test]
    fn concurrent_execution_is_rejected_without_blocking_cancel() {
        let raw = compile(b"a");
        unsafe {
            let handle = &*raw;
            let guard = handle.inner.lock().unwrap();
            let mut result = ptr::dangling_mut();
            assert_eq!(
                mg_grep_execute_batch(raw, ptr::null(), 0, &limits(), &mut result, ptr::null_mut()),
                MG_GREP_INVALID_ARGUMENT
            );
            assert!(result.is_null());
            mg_grep_session_cancel(raw);
            assert!(handle.cancellation.is_cancelled());
            drop(guard);
            assert_eq!(
                mg_grep_execute_batch(raw, ptr::null(), 0, &limits(), &mut result, ptr::null_mut()),
                MG_GREP_CANCELLED
            );
            mg_grep_session_free(raw);
        }
    }

    #[test]
    fn view_storage_is_charged_before_allocation() {
        let session = compile(b"a");
        let text = MgGrepBytes {
            data: b"a".as_ptr(),
            len: 1,
        };
        let required = size_of::<MgGrepResult>()
            + size_of::<TextResult>()
            + size_of::<MgGrepTextResult>()
            + size_of::<crate::Line>()
            + size_of::<MgGrepLine>()
            + size_of::<crate::Span>()
            + size_of::<MgGrepSpan>()
            + 1;
        let mut limit = limits();
        limit.max_result_bytes = required as u64;
        unsafe {
            let mut result = ptr::null_mut();
            assert_eq!(
                mg_grep_execute_batch(session, &text, 1, &limit, &mut result, ptr::null_mut()),
                MG_GREP_OK
            );
            mg_grep_result_free(result);
            limit.max_result_bytes -= 1;
            assert_eq!(
                mg_grep_execute_batch(session, &text, 1, &limit, &mut result, ptr::null_mut()),
                MG_GREP_RESOURCE_LIMIT
            );
            assert!(result.is_null());
            mg_grep_session_free(session);
        }
    }

    #[test]
    fn descriptor_validation_catches_overflow_and_alignment() {
        unsafe {
            assert!(checked_slice::<u64>(ptr::dangling(), usize::MAX).is_err());
            let aligned = [0u64; 2];
            assert!(required::<u64>((aligned.as_ptr() as *const u8).add(1).cast()).is_err());
            assert!(checked_slice::<u8>((usize::MAX - 1) as *const u8, 4).is_err());
            assert!(checked_slice::<u8>(ptr::null(), 0).unwrap().is_empty());
        }
    }

    #[test]
    fn cancellation_covers_view_conversion() {
        let session = compile(b"a");
        unsafe {
            let handle = &*session;
            mg_grep_session_cancel(session);
            let check = || check_execution(handle);
            assert!(matches!(
                make_result(
                    BatchResult {
                        texts: vec![TextResult::default()].into_boxed_slice()
                    },
                    check
                ),
                Err(Error::Cancelled)
            ));
            mg_grep_session_cancel(session);
            assert!(matches!(check_execution(handle), Err(Error::Cancelled)));
            mg_grep_session_free(session);
        }
    }

    #[test]
    fn cancellation_crosses_the_c_boundary_concurrently() {
        let session = compile(b"a");
        let address = session as usize;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let other = barrier.clone();
        let worker = std::thread::spawn(move || {
            let input = "a\n".repeat(2_000_000);
            let text = MgGrepBytes {
                data: input.as_ptr(),
                len: input.len(),
            };
            let mut result = ptr::null_mut();
            other.wait();
            let status = unsafe {
                mg_grep_execute_batch(
                    address as *mut MgGrepSession,
                    &text,
                    1,
                    &limits(),
                    &mut result,
                    ptr::null_mut(),
                )
            };
            assert!(result.is_null());
            status
        });
        barrier.wait();
        unsafe {
            mg_grep_session_cancel(session);
        }
        assert_eq!(worker.join().unwrap(), MG_GREP_CANCELLED);
        unsafe {
            mg_grep_session_free(session);
        }
    }
}
