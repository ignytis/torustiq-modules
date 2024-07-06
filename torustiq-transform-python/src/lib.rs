mod py_record;

use std::{
    collections::HashMap, sync::{mpsc::{channel, Sender}, Mutex}, thread, time::Duration
};

use log::debug;
use once_cell::sync::Lazy;
use pyo3::prelude::*;

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
    if args.kind != PipelineStepKind::Transformation {
        return ModuleStepInitFnResult::ErrorKindNotSupported;
    }

    let (tx, rx) = channel::<Record>();
    // TODO: add a parameter to read a Python file. File is preferrable place for larger code
    let code = get_param(args.step_handle, "code_contents").unwrap_or(String::from(""));
    // In order not to re-initialize the Python environment on each data processing,
    // the data is received in a loop inside a thread
    thread::spawn(move|| {
        Python::with_gil(|py| {
            let module: Bound<PyModule> = PyModule::from_code_bound(
                py,
                &code,
                "torustiq_module_process_record.py",
                "torustiq_module_process_record",
            ).unwrap();
            module.add_class::<py_record::PyRecord>().unwrap();

            loop {
                let in_record: py_record::PyRecord = match rx.recv_timeout(Duration::from_secs(1)) {
                    Ok(r) => r,
                    Err(_) => continue, // timeout
                }.into();

                let out_record: py_record::PyRecord = module.getattr("process").unwrap()
                    .call1((in_record,)).unwrap()
                    .extract().unwrap();
                let out_record: Record = out_record.into();

                let on_data_received_fn = match MODULE_INIT_ARGS.lock().unwrap().get(&args.step_handle) {
                    Some(m) => m.init_args.on_data_received_fn,
                    None => return ModuleProcessRecordFnResult::None,
                };
                on_data_received_fn(out_record, args.step_handle);
            }
        });
    });

    MODULE_INIT_ARGS.lock().unwrap().insert(args.step_handle, ModuleStepAtributes {
        init_args: args,
        sender: tx,
    });
    ModuleStepInitFnResult::Ok
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