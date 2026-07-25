//! Native (PyO3) bindings for the `unillm` Python SDK.
//!
//! The bindings layer over [`unillm_core`]: a `Client` PyO3 class wraps the Rust client, and an
//! `EventStream` exposes streaming as a Python async iterator. Type crossing happens at the JSON
//! boundary (the pure-Python facade serializes/deserializes), so the IR types are not mirrored here.

use pyo3::prelude::*;

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
