use std::{collections::HashMap, sync::Mutex};

use log::debug;
use once_cell::sync::Lazy;
use pyo3::prelude::*;

use torustiq_common::{
    ffi::{
        types::{buffer::ByteBuffer, module::{IoKind, ModuleInfo, ModuleInitStepArgs, ModuleProcessRecordFnResult, ModuleStepHandle, Record}},
        utils::strings::{bytes_to_string_safe, str_to_cchar},
    },
    logging::init_logger};

static MODULE_INIT_ARGS: Lazy<Mutex<HashMap<ModuleStepHandle, ModuleInitStepArgs>>> = Lazy::new(|| {
    Mutex::new(HashMap::new())
});

#[no_mangle]
pub extern "C" fn torustiq_module_get_info() -> ModuleInfo {
    ModuleInfo {
        id: str_to_cchar("transform_python"),
        name: str_to_cchar("Python transformation"),
        input_kind: IoKind::Stream,
        output_kind: IoKind::Stream,
    }
}

#[no_mangle]
extern "C" fn torustiq_module_init_step(args: ModuleInitStepArgs) {
    init_logger();
    MODULE_INIT_ARGS.lock().unwrap().insert(args.step_handle, args);
    
    debug!("Python transform: initialized");
}

#[no_mangle]
extern "C" fn torustiq_module_process_record(in_record: Record, step_handle: ModuleStepHandle) -> ModuleProcessRecordFnResult {
    let py_record = bytes_to_string_safe(in_record.content.bytes, in_record.content.len);

    let new_record = Python::with_gil(|py| {
        let module = PyModule::from_code_bound(
            py,
            r#"
import json
def process(record):
    result = json.loads(record)
    return "Response: " + result["test"].upper()
        "#,
            "transform.py",
            "transform",
        ).unwrap();

        let py_result: String = module.getattr("process").unwrap()
            .call1((py_record,)).unwrap()
            .extract().unwrap();

        let result = Record {
            content: ByteBuffer::from_string(py_result)
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