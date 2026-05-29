use pyo3::prelude::*;

/// Validates a single MFC queue record.
/// Returns Ok(()) if valid, Err(String) with reason if invalid.
fn validate_record(
    window_id: i64,
    avg_queue_length: f64,
    max_queue_length: f64,
    min_queue_length: f64,
    avg_wait_time_sec: f64,
    sample_count: i64,
    service_type: &str,
) -> Result<(), String> {
    // window_id must be between 1 and 100.
    if window_id < 1 || window_id > 100 {
        return Err(format!("window_id {} out of range [1, 100]", window_id));
    }

    // Queue lengths must be non-negative.
    if avg_queue_length < 0.0 {
        return Err(format!("avg_queue_length {} is negative", avg_queue_length));
    }
    if max_queue_length < 0.0 {
        return Err(format!("max_queue_length {} is negative", max_queue_length));
    }
    if min_queue_length < 0.0 {
        return Err(format!("min_queue_length {} is negative", min_queue_length));
    }

    // min must not exceed max.
    if min_queue_length > max_queue_length {
        return Err(format!(
            "min_queue_length {} > max_queue_length {}",
            min_queue_length, max_queue_length
        ));
    }

    // avg must be between min and max.
    if avg_queue_length < min_queue_length || avg_queue_length > max_queue_length {
        return Err(format!(
            "avg_queue_length {} not in [{}, {}]",
            avg_queue_length, min_queue_length, max_queue_length
        ));
    }

    // Wait time must be non-negative.
    if avg_wait_time_sec < 0.0 {
        return Err(format!("avg_wait_time_sec {} is negative", avg_wait_time_sec));
    }

    // Sample count must be positive.
    if sample_count < 1 {
        return Err(format!("sample_count {} must be >= 1", sample_count));
    }

    // service_type must be one of the known values.
    let valid_services = [
        "passport",
        "registration",
        "social_benefit",
        "tax",
        "property",
    ];
    if !valid_services.contains(&service_type) {
        return Err(format!("unknown service_type: {}", service_type));
    }

    Ok(())
}

/// Python-callable: validate one record, return (is_valid, error_message).
#[pyfunction]
fn validate_queue_record(
    window_id: i64,
    avg_queue_length: f64,
    max_queue_length: f64,
    min_queue_length: f64,
    avg_wait_time_sec: f64,
    sample_count: i64,
    service_type: &str,
) -> PyResult<(bool, String)> {
    match validate_record(
        window_id,
        avg_queue_length,
        max_queue_length,
        min_queue_length,
        avg_wait_time_sec,
        sample_count,
        service_type,
    ) {
        Ok(()) => Ok((true, String::new())),
        Err(msg) => Ok((false, msg)),
    }
}

/// Python-callable: validate a batch of records.
/// Returns list of (row_index, error_message) for invalid records.
#[pyfunction]
fn validate_batch(records: Vec<(i64, f64, f64, f64, f64, i64, String)>) -> PyResult<Vec<(usize, String)>> {
    let mut errors = Vec::new();
    for (i, (window_id, avg_q, max_q, min_q, avg_w, samples, svc)) in records.iter().enumerate() {
        if let Err(msg) = validate_record(*window_id, *avg_q, *max_q, *min_q, *avg_w, *samples, svc) {
            errors.push((i, msg));
        }
    }
    Ok(errors)
}

#[pymodule]
fn mfc_validator(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(validate_queue_record, m)?)?;
    m.add_function(wrap_pyfunction!(validate_batch, m)?)?;
    Ok(())
}