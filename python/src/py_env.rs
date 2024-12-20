use std::{
    sync::mpsc::Receiver,
    time::Duration
};

use log::{error, warn};
use py_record::PyRecord;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use torustiq_common::ffi::{
    shared::get_pipeline_module_configuration,
    types::module::{ModuleHandle, Record},
};

use crate::py_record;

/// This function is callled from Python code to submit a record to the next step
#[pyfunction]
fn torustiq_send(record: PyRecord, module_handle: ModuleHandle) {
    let on_data_receive_cb = match get_pipeline_module_configuration(module_handle) {
        Some(c) => c.on_data_receive_cb,
        None => {
            error!("torustiq_send: invalid step handle: {}", module_handle);
            return;
        }
    };
    on_data_receive_cb(record.into(), module_handle);
}

/// Returns a routine for sender thread (i.e. the first step in pipeline)
pub fn thread_sender(module_handle: ModuleHandle) -> impl Fn(Bound<PyModule>) {
    let f = move |module: Bound<PyModule>| {
        let run_fn = module.getattr("run").unwrap();
        if let Err(e) = run_fn.call1((module_handle,)) {
            warn!("Error on execution of 'run' Python function: {}", e);
        };

        let on_step_terminate_cb = match get_pipeline_module_configuration(module_handle) {
            Some(cfg) => cfg.on_step_terminate_cb,
            None => {
                error!("Failed to load the step configuration for step '{}' in Python sender thread", module_handle);
                return;
            }
        };
        on_step_terminate_cb(module_handle);
    };
    f
}

/// Returns a routine for receiver thread (i.e. step other that the first one in pipeline)
pub fn thread_receiver(module_handle: ModuleHandle, rx: Receiver<Record>) -> impl Fn(Bound<PyModule>) {
    let f = move |module: Bound<PyModule>| {
        let process_fn = module.getattr("process").unwrap();
        loop {
            let in_record: py_record::PyRecord = match rx.recv_timeout(Duration::from_secs(1)) {
                Ok(r) => r,
                Err(_) => continue, // timeout
            }.into();
            if let Err(e) = process_fn.call1((in_record, module_handle)) {
                warn!("Error on execution of 'process' Python function: {}", e);
            };
        }
    };
    f
}


/// Runs a routiune in Python environment
pub fn thread_python_env<F>(code: String, python_routine_fn: F) where F: Fn(Bound<PyModule>) {
    Python::with_gil(|py| {
        let module: Bound<PyModule> = PyModule::from_code_bound(
            py,
            &code,
            "torustiq_module_pipeline_process_record.py",
            "torustiq_module_pipeline_process_record",
        ).unwrap();
        // Register a PyRecord class and torustiq_send Python function
        module.add_class::<py_record::PyRecord>().unwrap();
        // Register a torustiq_send function
        module.add_function(wrap_pyfunction!(torustiq_send, &module).unwrap()).unwrap();
        python_routine_fn(module);
    });
}