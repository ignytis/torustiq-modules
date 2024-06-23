use std::{collections::HashMap, sync::Mutex};

use log::{error, debug};
use once_cell::sync::Lazy;
use torustiq_common::{
    ffi::{
        types::{module::{
            IoKind, ModuleInfo, ModuleProcessRecordFnResult, ModuleStepHandle, ModuleStepInitArgs, Record},
            std_types::ConstCharPtr},
        utils::strings::{bytes_to_string_safe, cchar_to_string, str_to_cchar},
    },
    logging::init_logger};

static MODULE_PARAMS: Lazy<Mutex<HashMap<ModuleStepHandle, HashMap<String, String>>>> = Lazy::new(|| {
    Mutex::new(HashMap::new())
});

#[no_mangle]
pub extern "C" fn torustiq_module_get_info() -> ModuleInfo {
    ModuleInfo {
        id: str_to_cchar("destination_stdout"),
        name: str_to_cchar("STDOUT destination"),
        input_kind: IoKind::Stream,
        output_kind: IoKind::External,
    }
}

#[no_mangle]
extern "C" fn torustiq_module_init() {
    init_logger();
    debug!("Source HTTP destination: initialized");
}

#[no_mangle]
extern "C" fn torustiq_module_step_init(_args: ModuleStepInitArgs) {

}

#[no_mangle]
extern "C" fn torustiq_module_step_set_param(h: ModuleStepHandle, k: ConstCharPtr, v: ConstCharPtr) {
    let mut module_params_container = MODULE_PARAMS.lock().unwrap();
    if !module_params_container.contains_key(&h) {
        module_params_container.insert(h, HashMap::new());
    }
    let step_cfg = module_params_container.get_mut(&h).unwrap();
    step_cfg.insert(cchar_to_string(k), cchar_to_string(v));
}

#[no_mangle]
extern "C" fn torustiq_module_process_record(input: Record) -> ModuleProcessRecordFnResult {
    println!("{}", bytes_to_string_safe(input.content.bytes, input.content.len));
    ModuleProcessRecordFnResult::None
}