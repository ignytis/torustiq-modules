use std::{collections::HashMap, sync::Mutex};

use log::debug;
use once_cell::sync::Lazy;
use pyo3::{prelude::*, types::PyBytes};

use torustiq_common::{
    ffi::types::{module::{IoKind, ModuleInfo, ModuleProcessRecordFnResult, ModuleStepHandle, ModuleStepInitArgs, Record}, std_types::ConstCStrPtr},
    logging::init_logger};

static MODULE_INIT_ARGS: Lazy<Mutex<HashMap<ModuleStepHandle, ModuleStepInitArgs>>> = Lazy::new(|| {
    Mutex::new(HashMap::new())
});

const MODULE_ID: ConstCStrPtr = c"transform_python".as_ptr();
const MODULE_NAME: ConstCStrPtr = c"Python transformation".as_ptr();

#[no_mangle]
pub extern "C" fn torustiq_module_get_info() -> ModuleInfo {
    ModuleInfo {
        id: MODULE_ID,
        name: MODULE_NAME,
        input_kind: IoKind::Stream,
        output_kind: IoKind::Stream,
    }
}

#[no_mangle]
extern "C" fn torustiq_module_init() {
    init_logger();
    debug!("Python transform: initialized");
}

#[no_mangle]
extern "C" fn torustiq_module_step_init(args: ModuleStepInitArgs) {
    MODULE_INIT_ARGS.lock().unwrap().insert(args.step_handle, args);
}

#[no_mangle]
extern "C" fn torustiq_module_process_record(in_record: Record, step_handle: ModuleStepHandle) -> ModuleProcessRecordFnResult {
    let new_record = Python::with_gil(|py| {
        let py_record = unsafe { PyBytes::bound_from_ptr(py, in_record.content.bytes, in_record.content.len) };
        let module = PyModule::from_code_bound(
            py,
            r#"
import json
def process(record):
    result = json.loads(record)
    return "Response: " + result["test"].upper()
        "#,
            "torustiq_module_process_record.py",
            "torustiq_module_process_record",
        ).unwrap();

        let py_result: String = module.getattr("process").unwrap()
            .call1((py_record,)).unwrap()
            .extract().unwrap();

        let result = Record {
            content: py_result.into(),
        };

        result
    });

    let on_data_received_fn = match MODULE_INIT_ARGS.lock().unwrap().get(&step_handle) {
        Some(m) => m.on_data_received_fn,
        None => return ModuleProcessRecordFnResult::None,
    };
    on_data_received_fn(new_record, step_handle);

    ModuleProcessRecordFnResult::None
}