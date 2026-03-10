use crate::error::WatchdogError;
use anyhow::Error;
use std::backtrace::BacktraceStatus;
use tracing::{debug, error, warn};

fn find_watchdog_error(err: &Error) -> Option<&WatchdogError> {
    err.chain()
        .find_map(|cause| cause.downcast_ref::<WatchdogError>())
}

/// Emit structured fatal diagnostics to tracing for machine-readable logs.
pub fn log_anyhow_error(context: &str, err: &Error) {
    error!("{}: {}", context, err);

    for (idx, cause) in err.chain().enumerate() {
        error!("error_chain[{}]: {}", idx, cause);
    }

    if let Some(watchdog_err) = find_watchdog_error(err) {
        error!(
            error_code = watchdog_err.code(),
            error_category = watchdog_err.category(),
            "Watchdog error classification"
        );
        for (key, value) in watchdog_err.diagnostic_fields() {
            error!(field = key, value = %value, "Watchdog error context");
        }
        if let Some(hint) = watchdog_err.operator_hint() {
            warn!("Suggested remediation: {}", hint);
        }
    } else {
        warn!("No WatchdogError found in error chain for structured diagnostics");
    }

    match err.backtrace().status() {
        BacktraceStatus::Captured => {
            debug!("Captured backtrace:\n{}", err.backtrace());
        }
        BacktraceStatus::Disabled => {
            debug!("Backtrace disabled. Set RUST_BACKTRACE=1 for stack traces.");
        }
        BacktraceStatus::Unsupported => {
            debug!("Backtrace unsupported on this platform/toolchain.");
        }
        _ => {
            debug!("Backtrace status is unknown.");
        }
    }
}

/// Print a human-oriented fatal report to stderr for on-host debugging.
pub fn print_anyhow_error_report(context: &str, err: &Error) {
    eprintln!("{context}: {err}");
    eprintln!("error chain:");
    for (idx, cause) in err.chain().enumerate() {
        eprintln!("  [{idx}] {cause}");
    }

    if let Some(watchdog_err) = find_watchdog_error(err) {
        eprintln!(
            "classification: code={} category={}",
            watchdog_err.code(),
            watchdog_err.category()
        );
        for (key, value) in watchdog_err.diagnostic_fields() {
            eprintln!("  {key}: {value}");
        }
        if let Some(hint) = watchdog_err.operator_hint() {
            eprintln!("hint: {hint}");
        }
    }

    eprintln!("debug tip: set RUST_LOG=watchdog=debug and RUST_BACKTRACE=1 for deeper diagnostics");
}
