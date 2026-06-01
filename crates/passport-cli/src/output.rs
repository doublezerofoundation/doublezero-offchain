//! Shared output-format helpers for the read verbs (`fetch`, `find-validator`).
//!
//! The `--json` / `--json-compact` flags are additive: when neither is set the
//! verbs reproduce the exact pre-RFC-20 human-readable output. These helpers
//! keep the format-resolution and JSON-emission logic in one place.

use std::io::Write;

use doublezero_cli_core::OutputFormat;
use serde::Serialize;

/// Honor the per-verb `--json` / `--json-compact` flags when set, otherwise fall
/// back to the context's output-format hint supplied by the binary.
pub fn resolve_format(json: bool, json_compact: bool, ctx_format: OutputFormat) -> OutputFormat {
    if json || json_compact {
        OutputFormat::from_flags(json, json_compact)
    } else {
        ctx_format
    }
}

/// True when the resolved format requests JSON output.
pub fn is_json(format: OutputFormat) -> bool {
    matches!(format, OutputFormat::Json | OutputFormat::JsonCompact)
}

/// Serialize `value` as JSON (compact or pretty per `format`), terminated by a
/// newline.
pub fn emit_json<W: Write, T: Serialize>(
    out: &mut W,
    value: &T,
    format: OutputFormat,
) -> eyre::Result<()> {
    let rendered = if matches!(format, OutputFormat::JsonCompact) {
        serde_json::to_string(value)?
    } else {
        serde_json::to_string_pretty(value)?
    };
    writeln!(out, "{rendered}")?;
    Ok(())
}
