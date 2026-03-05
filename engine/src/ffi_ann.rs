use crate::config::DistanceMetric;
use crate::hnsw_index::HnswVectorIndex;
use anyhow::{anyhow, Context, Result};
use std::cell::RefCell;
use std::ffi::c_char;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::slice;

struct AnnIndexHandle {
    index: HnswVectorIndex,
    dimension: usize,
    max_elements: usize,
    built: bool,
    trusted_inputs: bool,
}

thread_local! {
    static LAST_ERROR: RefCell<String> = const { RefCell::new(String::new()) };
}

const ANN_CREATE_FLAG_TRUST_INPUTS: u32 = 1 << 0;

fn set_last_error(message: impl Into<String>) {
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = message.into();
    });
}

fn clear_last_error() {
    LAST_ERROR.with(|slot| slot.borrow_mut().clear());
}

fn get_last_error() -> String {
    LAST_ERROR.with(|slot| slot.borrow().clone())
}

fn ffi_status<F>(func: F) -> i32
where
    F: FnOnce() -> Result<()>,
{
    clear_last_error();
    match catch_unwind(AssertUnwindSafe(func)) {
        Ok(Ok(())) => 0,
        Ok(Err(err)) => {
            set_last_error(err.to_string());
            -1
        }
        Err(_) => {
            set_last_error("panic in kyrodb ann ffi call");
            -2
        }
    }
}

fn metric_from_code(code: u32) -> Result<DistanceMetric> {
    match code {
        0 => Ok(DistanceMetric::Cosine),
        1 => Ok(DistanceMetric::Euclidean),
        2 => Ok(DistanceMetric::InnerProduct),
        _ => Err(anyhow!("invalid distance code: {}", code)),
    }
}

#[no_mangle]
pub extern "C" fn kyrodb_ann_create(
    dimension: u32,
    max_elements: u64,
    distance_code: u32,
    m: u32,
    ef_construction: u32,
    disable_normalization_check: u8,
) -> *mut std::ffi::c_void {
    kyrodb_ann_create_with_flags(
        dimension,
        max_elements,
        distance_code,
        m,
        ef_construction,
        disable_normalization_check,
        0,
    )
}

#[no_mangle]
pub extern "C" fn kyrodb_ann_create_with_flags(
    dimension: u32,
    max_elements: u64,
    distance_code: u32,
    m: u32,
    ef_construction: u32,
    disable_normalization_check: u8,
    flags: u32,
) -> *mut std::ffi::c_void {
    clear_last_error();
    match catch_unwind(AssertUnwindSafe(|| -> Result<*mut std::ffi::c_void> {
        if dimension == 0 {
            anyhow::bail!("dimension must be > 0");
        }
        if max_elements == 0 {
            anyhow::bail!("max_elements must be > 0");
        }

        let distance = metric_from_code(distance_code)?;
        let max_elements_usize = usize::try_from(max_elements)
            .context("max_elements exceeds platform usize capacity")?;
        let disable_normalization_check = disable_normalization_check != 0;
        if (flags & !ANN_CREATE_FLAG_TRUST_INPUTS) != 0 {
            anyhow::bail!("unknown create flags: 0x{:x}", flags);
        }
        let trusted_inputs = (flags & ANN_CREATE_FLAG_TRUST_INPUTS) != 0;

        let index = HnswVectorIndex::new_with_params(
            dimension as usize,
            max_elements_usize,
            distance,
            m as usize,
            ef_construction as usize,
            disable_normalization_check,
        )?;

        let handle = Box::new(AnnIndexHandle {
            index,
            dimension: dimension as usize,
            max_elements: max_elements_usize,
            built: false,
            trusted_inputs,
        });

        Ok(Box::into_raw(handle) as *mut std::ffi::c_void)
    })) {
        Ok(Ok(ptr)) => ptr,
        Ok(Err(err)) => {
            set_last_error(err.to_string());
            ptr::null_mut()
        }
        Err(_) => {
            set_last_error("panic in kyrodb_ann_create_with_flags");
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn kyrodb_ann_free(handle: *mut std::ffi::c_void) {
    if handle.is_null() {
        return;
    }
    let ptr = handle as *mut AnnIndexHandle;
    // SAFETY: Pointer is created by `Box::into_raw` in `kyrodb_ann_create` and freed once.
    unsafe {
        drop(Box::from_raw(ptr));
    }
}

#[no_mangle]
pub unsafe extern "C" fn kyrodb_ann_build_f32(
    handle: *mut std::ffi::c_void,
    vectors: *const f32,
    rows: usize,
    cols: usize,
) -> i32 {
    ffi_status(|| {
        if handle.is_null() {
            anyhow::bail!("null handle");
        }
        if rows == 0 {
            anyhow::bail!("rows must be > 0");
        }
        if vectors.is_null() {
            anyhow::bail!("vectors pointer is null");
        }

        // SAFETY: The pointer was created by `kyrodb_ann_create` and checked for null above.
        // Build path mutates index state and therefore requires exclusive mutable access.
        let state = unsafe { &mut *(handle as *mut AnnIndexHandle) };
        if state.built {
            anyhow::bail!("index already built; create a new handle for each fit");
        }
        if rows > state.max_elements {
            anyhow::bail!(
                "rows exceed index capacity: rows={} max_elements={}",
                rows,
                state.max_elements
            );
        }
        if cols != state.dimension {
            anyhow::bail!(
                "dimension mismatch: expected {} got {}",
                state.dimension,
                cols
            );
        }

        let total = rows
            .checked_mul(cols)
            .ok_or_else(|| anyhow!("rows*cols overflow"))?;

        // SAFETY: Caller guarantees `vectors` points to a contiguous `rows*cols` f32 buffer.
        let flat = unsafe { slice::from_raw_parts(vectors, total) };
        let mut batch: Vec<(&[f32], usize)> = Vec::with_capacity(rows);
        for row in 0..rows {
            let start = row * cols;
            let end = start + cols;
            batch.push((&flat[start..end], row));
        }

        if state.trusted_inputs {
            state.index.parallel_insert_batch_trusted(&batch)?;
        } else {
            state.index.parallel_insert_batch(&batch)?;
        }
        state.built = true;
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn kyrodb_ann_query_f32(
    handle: *const std::ffi::c_void,
    query: *const f32,
    cols: usize,
    k: usize,
    ef_search: usize,
    out_ids: *mut u32,
    out_len: *mut usize,
) -> i32 {
    ffi_status(|| {
        if handle.is_null() {
            anyhow::bail!("null handle");
        }
        if query.is_null() {
            anyhow::bail!("query pointer is null");
        }
        if out_len.is_null() {
            anyhow::bail!("out_len pointer is null");
        }
        if k == 0 {
            anyhow::bail!("k must be > 0");
        }
        if out_ids.is_null() {
            anyhow::bail!("out_ids pointer is null");
        }

        // SAFETY: The pointer was created by `kyrodb_ann_create` and checked for null above.
        // Query path is read-only and must not create aliased mutable references.
        let state = unsafe { &*(handle as *const AnnIndexHandle) };
        if !state.built {
            anyhow::bail!("query before fit/build is not allowed");
        }
        if cols != state.dimension {
            anyhow::bail!(
                "query dimension mismatch: expected {} got {}",
                state.dimension,
                cols
            );
        }

        // SAFETY: Caller guarantees `query` points to `cols` contiguous floats.
        let query_slice = unsafe { slice::from_raw_parts(query, cols) };
        let results = if state.trusted_inputs {
            state.index.knn_search_with_ef_cancel_trusted(
                query_slice,
                k,
                Some(ef_search.max(k)),
                None,
            )?
        } else {
            state
                .index
                .knn_search_with_ef(query_slice, k, Some(ef_search.max(k)))?
        };

        let out_count = results.len().min(k);
        for (idx, item) in results.iter().take(out_count).enumerate() {
            let doc_id = u32::try_from(item.doc_id)
                .with_context(|| format!("doc_id {} does not fit in u32", item.doc_id))?;
            // SAFETY: Caller provides `out_ids` capacity for at least `k` entries.
            unsafe {
                *out_ids.add(idx) = doc_id;
            }
        }
        // SAFETY: `out_len` validated as non-null above.
        unsafe {
            *out_len = out_count;
        }
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn kyrodb_ann_memory_bytes(handle: *const std::ffi::c_void) -> u64 {
    if handle.is_null() {
        return 0;
    }
    // SAFETY: The pointer was created by `kyrodb_ann_create`.
    let state = unsafe { &*(handle as *const AnnIndexHandle) };
    state.index.estimate_memory_bytes() as u64
}

#[no_mangle]
/// Returns the current thread's last FFI error string length in bytes.
///
/// This is paired with [`kyrodb_ann_last_error_copy`], but callers should note
/// there is a TOCTOU window between calls: any subsequent FFI call on the same
/// thread can overwrite the error text before copy.
///
/// Prefer reading errors immediately after a failed call on the same thread.
pub extern "C" fn kyrodb_ann_last_error_len() -> usize {
    get_last_error().len()
}

#[no_mangle]
/// Copies the current thread's last FFI error into `buffer`.
///
/// Return value semantics:
/// - Returns the number of bytes written, excluding the trailing NUL terminator.
/// - If return value equals `capacity - 1`, output may be truncated.
///
/// TOCTOU note:
/// This follows the same two-step pattern as `kyrodb_ann_last_error_len()`;
/// the error text can change if another FFI call updates LAST_ERROR on the
/// same thread between calls.
pub unsafe extern "C" fn kyrodb_ann_last_error_copy(buffer: *mut c_char, capacity: usize) -> usize {
    if buffer.is_null() || capacity == 0 {
        return 0;
    }

    let message = get_last_error();
    let bytes = message.as_bytes();
    let max_payload = capacity.saturating_sub(1);
    let copy_len = bytes.len().min(max_payload);

    // SAFETY: Caller guarantees `buffer` points to writable memory of `capacity` bytes.
    unsafe {
        ptr::copy_nonoverlapping(bytes.as_ptr(), buffer as *mut u8, copy_len);
        *buffer.add(copy_len) = 0;
    }

    copy_len
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distance_code_mapping_is_stable() {
        assert_eq!(metric_from_code(0).unwrap(), DistanceMetric::Cosine);
        assert_eq!(metric_from_code(1).unwrap(), DistanceMetric::Euclidean);
        assert_eq!(metric_from_code(2).unwrap(), DistanceMetric::InnerProduct);
        assert!(metric_from_code(99).is_err());
    }

    #[test]
    fn ffi_build_and_query_round_trip() {
        let handle = kyrodb_ann_create(4, 16, 1, 16, 200, 0);
        assert!(!handle.is_null(), "create failed: {}", get_last_error());

        let data: [f32; 16] = [
            1.0, 0.0, 0.0, 0.0, // id 0
            0.9, 0.1, 0.0, 0.0, // id 1
            0.0, 1.0, 0.0, 0.0, // id 2
            0.0, 0.0, 1.0, 0.0, // id 3
        ];

        // SAFETY: pointers and lengths are valid for the call.
        let rc_build = unsafe { kyrodb_ann_build_f32(handle, data.as_ptr(), 4, 4) };
        assert_eq!(rc_build, 0, "build failed: {}", get_last_error());

        let query = [1.0_f32, 0.0, 0.0, 0.0];
        let mut out_ids = [u32::MAX; 3];
        let mut out_len: usize = 0;
        // SAFETY: pointers and lengths are valid for the call.
        let rc_query = unsafe {
            kyrodb_ann_query_f32(
                handle,
                query.as_ptr(),
                4,
                3,
                16,
                out_ids.as_mut_ptr(),
                &mut out_len as *mut usize,
            )
        };
        assert_eq!(rc_query, 0, "query failed: {}", get_last_error());
        assert!(out_len > 0, "query returned zero results");
        assert_eq!(out_ids[0], 0, "self match should rank first");

        // SAFETY: handle comes from kyrodb_ann_create and is still valid.
        let mem = unsafe { kyrodb_ann_memory_bytes(handle) };
        assert!(mem > 0, "memory estimate should be non-zero after build");

        // SAFETY: handle comes from kyrodb_ann_create and is freed exactly once.
        unsafe { kyrodb_ann_free(handle) };
    }

    #[test]
    fn ffi_create_with_trusted_flag_round_trip() {
        let handle =
            kyrodb_ann_create_with_flags(4, 16, 1, 16, 200, 0, ANN_CREATE_FLAG_TRUST_INPUTS);
        assert!(
            !handle.is_null(),
            "create_with_flags failed: {}",
            get_last_error()
        );

        let data: [f32; 16] = [
            1.0, 0.0, 0.0, 0.0, // id 0
            0.9, 0.1, 0.0, 0.0, // id 1
            0.0, 1.0, 0.0, 0.0, // id 2
            0.0, 0.0, 1.0, 0.0, // id 3
        ];

        // SAFETY: pointers and lengths are valid for the call.
        let rc_build = unsafe { kyrodb_ann_build_f32(handle, data.as_ptr(), 4, 4) };
        assert_eq!(rc_build, 0, "build failed: {}", get_last_error());

        let query = [1.0_f32, 0.0, 0.0, 0.0];
        let mut out_ids = [u32::MAX; 3];
        let mut out_len: usize = 0;
        // SAFETY: pointers and lengths are valid for the call.
        let rc_query = unsafe {
            kyrodb_ann_query_f32(
                handle,
                query.as_ptr(),
                4,
                3,
                16,
                out_ids.as_mut_ptr(),
                &mut out_len as *mut usize,
            )
        };
        assert_eq!(rc_query, 0, "query failed: {}", get_last_error());
        assert!(out_len > 0, "query returned zero results");
        assert_eq!(out_ids[0], 0, "self match should rank first");

        // SAFETY: handle comes from kyrodb_ann_create_with_flags and is freed exactly once.
        unsafe { kyrodb_ann_free(handle) };
    }
}
