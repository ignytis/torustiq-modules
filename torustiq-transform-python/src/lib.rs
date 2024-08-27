mod py_record;

use std::{
    collections::HashMap, sync::{mpsc::{channel, Receiver, Sender}, Mutex}, thread, time::Duration
};

use log::{debug, warn};
use once_cell::sync::Lazy;
use py_record::PyRecord;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use torustiq_common::{
    ffi::{
        shared::get_param,
        types::{
            module::{
                ModuleInfo, ModuleProcessRecordFnResult, ModuleStepHandle, ModuleStepInitArgs,
                ModuleStepInitFnResult, PipelineStepKind, Record
            },
            std_types::ConstCStrPtr
        }},
    logging::init_logger};

struct ModuleStepAtributes {
    init_args: ModuleStepInitArgs,
    sender: Sender<Record>,
}

static MODULE_INIT_ARGS: Lazy<Mutex<HashMap<ModuleStepHandle, ModuleStepAtributes>>> = Lazy::new(|| {
    Mutex::new(HashMap::new())
});

const MODULE_ID: ConstCStrPtr = c"transform_python".as_ptr();
const MODULE_NAME: ConstCStrPtr = c"Python transformation".as_ptr();

/// This function is callled from Python code to submit a record to the next step
#[pyfunction]
fn torustiq_send(record: PyRecord, step_handle: ModuleStepHandle) {
    let on_data_received_fn = {
        let l = MODULE_INIT_ARGS.lock(); // TODO:
        l.unwrap()
            .get(&step_handle).unwrap()
            .init_args.on_data_received_fn
    };
    on_data_received_fn(record.into(), step_handle);
}

#[no_mangle]
pub extern "C" fn torustiq_module_get_info() -> ModuleInfo {
    ModuleInfo {
        id: MODULE_ID,
        name: MODULE_NAME,
    }
}

#[no_mangle]
extern "C" fn torustiq_module_init() {
    init_logger();
    debug!("Python transform: initialized");
}

#[no_mangle]
extern "C" fn torustiq_module_step_init(args: ModuleStepInitArgs) -> ModuleStepInitFnResult {
    // Multiple instances of module are not supported currently because of Python's GIL.
    // There is one instance of Python environment created for process, therefore threads start
    // to lock each other. Due to this reason only one instance of Python module is allowed.
    {
        let args = MODULE_INIT_ARGS.lock().unwrap();
        if args.len() > 0 {
            return ModuleStepInitFnResult::ErrorMultipleStepsNotSupported(*args.keys().next().unwrap());
        }
    }

    let (tx, rx) = channel::<Record>();
    // TODO: add a parameter to read a Python file. File is preferrable place for larger code
    let code = get_param(args.step_handle, "code_contents").unwrap_or(String::from(""));
    let step_handle = args.step_handle;
    let kind = args.kind.clone();
    thread::spawn(move || {
        match kind {
            PipelineStepKind::Source => thread_python_env(code, thread_sender(step_handle)),
            _ => thread_python_env(code, thread_receiver(step_handle, rx)),
        };
    });

    MODULE_INIT_ARGS.lock().unwrap().insert(args.step_handle, ModuleStepAtributes {
        init_args: args,
        sender: tx,
    });

    ModuleStepInitFnResult::Ok
}

fn thread_sender(step_handle: ModuleStepHandle) -> impl Fn(Bound<PyModule>) {
    let f = move |module: Bound<PyModule>| {
        let run_fn = module.getattr("run").unwrap();
        match run_fn.call1((step_handle,)) {
            Ok(_) => {}, // Execution finished
            Err(e) => warn!("Error on execution of 'run' Python function: {}", e),
        };

        (MODULE_INIT_ARGS.lock().unwrap().get(&step_handle).unwrap().init_args.termination_handler)(step_handle);
    };
    f
}

fn thread_receiver(step_handle: ModuleStepHandle, rx: Receiver<Record>) -> impl Fn(Bound<PyModule>) {
    let f = move |module: Bound<PyModule>| {
        let process_fn = module.getattr("process").unwrap();
        loop {
            let in_record: py_record::PyRecord = match rx.recv_timeout(Duration::from_secs(1)) {
                Ok(r) => r,
                Err(_) => continue, // timeout
            }.into();
            match process_fn.call1((in_record, step_handle)) {
                Ok(_) => {},
                Err(e) => warn!("Error: {}", e),
            };
        }
    };
    f
}

fn thread_python_env<F>(code: String, python_routine_fn: F) where F: Fn(Bound<PyModule>) {
    Python::with_gil(|py| {
        let module: Bound<PyModule> = PyModule::from_code_bound(
            py,
            &code,
            "torustiq_module_process_record.py",
            "torustiq_module_process_record",
        ).unwrap();
        // Register a PyRecord class and torustiq_send Python function
        module.add_class::<py_record::PyRecord>().unwrap();
        // Register a torustiq_send function
        module.add_function(wrap_pyfunction!(torustiq_send, &module).unwrap()).unwrap();
        python_routine_fn(module);
    });
}

#[no_mangle]
extern "C" fn torustiq_module_process_record(in_record: Record, step_handle: ModuleStepHandle) -> ModuleProcessRecordFnResult {
    let mutex = MODULE_INIT_ARGS.lock().unwrap();
    let sender = match mutex.get(&step_handle) {
        Some(m) => &m.sender,
        None => return ModuleProcessRecordFnResult::None,
    };
    sender.send(in_record).unwrap();
    ModuleProcessRecordFnResult::None
}