mod py_record;

use std::{collections::HashMap, sync::Mutex};

use log::debug;
use once_cell::sync::Lazy;
use pyo3::prelude::*;

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
        let py_record: py_record::PyRecord = in_record.into();
        let module = PyModule::from_code_bound(
            py,
            r#"
import json
def process(record):
    j = json.loads(bytes(record.content))
    mtd = record.metadata
    mtd["added_val"] = "hello2"
    result = PyRecord(content=j["test"].upper().encode("utf-8"), metadata=mtd)
    return result
        "#,
            "torustiq_module_process_record.py",
            "torustiq_module_process_record",
        ).unwrap();
        module.add_class::<py_record::PyRecord>().unwrap();

        let out_record: py_record::PyRecord = module.getattr("process").unwrap()
            .call1((py_record,)).unwrap()
            .extract().unwrap();

        out_record.into()
    });

    let on_data_received_fn = match MODULE_INIT_ARGS.lock().unwrap().get(&step_handle) {
        Some(m) => m.on_data_received_fn,
        None => return ModuleProcessRecordFnResult::None,
    };
    on_data_received_fn(new_record, step_handle);

    ModuleProcessRecordFnResult::None
}